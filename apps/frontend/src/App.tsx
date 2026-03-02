import { TripsLayer } from "@deck.gl/geo-layers";
import { ScatterplotLayer } from "@deck.gl/layers";
import { DeckGL, PickingInfo } from "deck.gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useMemo, useRef, useState } from "react";
import MapView from "react-map-gl/maplibre";
import { useLiveVehicles } from "./hooks/useLiveVehicles";
import { RenderedVehicle, Vehicle } from "./types/vehicle";

// --- CONFIGURATION ---
const WS_URL =
  import.meta.env.VITE_WS_URL?.toString() ?? "ws://127.0.0.1:3000/ws";

const API_URL = "http://127.0.0.1:8000";

const MAP_STYLE =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

const INITIAL_VIEW_STATE = {
  longitude: 14.4378,
  latitude: 50.0755,
  zoom: 12,
  pitch: 0,
  bearing: 0,
};

// Live mode playback offset for smoother motion.
const PLAYBACK_DELAY_MS = 30000;

// Visual trail duration.
const TRAIL_LENGTH_MS = 150000;

// Drop vehicles with stale data.
const DROP_THRESHOLD_MS = 5 * 60 * 1000;

// Render-side smoothing factor.
const SMOOTHING_FACTOR = 0.1;

// --- HELPER FUNCTIONS ---

const lerp = (start: number, end: number, factor: number): number =>
  start + (end - start) * factor;

const isValidPoint = (point: unknown): point is [number, number, number] =>
  Array.isArray(point) && point.length >= 3;

interface TargetPositionResult {
  pos: [number, number];
  isStale: boolean;
}

type ViewMode = "live" | "replay";
type ReplayMeta = {
  start_ts: string;
  end_ts: string;
  vehicle_id: string | null;
  mode: string | null;
  requested_limit: number;
  returned_count: number;
  has_more: boolean;
  query_time_ms: number;
};
type ReplayPointState = "observed" | "interpolated" | "invalid_gap";
type ReplayConfidence = "high" | "medium" | "low";
type ReplayRow = {
  time: string;
  vehicle_id: string;
  lat: number;
  lon: number;
  point_state: ReplayPointState;
  confidence: ReplayConfidence;
  interpolation_method: string | null;
  gap_reason: string | null;
  route_id: string | null;
  trip_id: string | null;
  mode: string | null;
};

function getTargetPosition(
  path: Array<[number, number, number]>,
  currentTime: number,
): TargetPositionResult | null {
  if (path.length < 1) return null;

  const lastPoint = path[path.length - 1];
  const lastTime = lastPoint[2];

  // Check if data is expired
  if (currentTime - lastTime > DROP_THRESHOLD_MS) {
    return null;
  }

  // Check if we have reached the end of the known history
  // The vehicle is technically "waiting" for new data from the server.
  if (currentTime >= lastTime) {
    return {
      pos: [lastPoint[0], lastPoint[1]],
      isStale: true,
    };
  }

  // Interpolate position within the history
  // Search backwards for the relevant time segment.
  for (let i = path.length - 2; i >= 0; i--) {
    const startNode = path[i];
    const endNode = path[i + 1];

    if (currentTime >= startNode[2] && currentTime <= endNode[2]) {
      const duration = endNode[2] - startNode[2];

      if (duration === 0)
        return { pos: [startNode[0], startNode[1]], isStale: false };

      const ratio = (currentTime - startNode[2]) / duration;
      const lon = startNode[0] + (endNode[0] - startNode[0]) * ratio;
      const lat = startNode[1] + (endNode[1] - startNode[1]) * ratio;

      return {
        pos: [lon, lat],
        isStale: false,
      };
    }
  }

  // Fallback: Return the start of history
  return { pos: [path[0][0], path[0][1]], isStale: false };
}

// --- MAIN COMPONENT ---

export default function App() {
  const [viewMode, setViewMode] = useState<ViewMode>("live");
  const { vehicles, isConnected, startTime } = useLiveVehicles(
    WS_URL,
    viewMode === "live",
  );
  const [replayVehicles, setReplayVehicles] = useState<Vehicle[]>([]);
  const [replayStartTime, setReplayStartTime] = useState<number>(Date.now());
  const [replayMeta, setReplayMeta] = useState<ReplayMeta | null>(null);

  const activeVehicles = viewMode === "live" ? vehicles : replayVehicles;

  const loadReplay = async () => {
    const limit = 20000;
    const end = Date.now();
    const start = end - 60 * 1000;

    const endIso = new Date(end).toISOString();
    const startIso = new Date(start).toISOString();

    const response = await fetch(
      `${API_URL}/replay?start_ts=${encodeURIComponent(startIso)}&end_ts=${encodeURIComponent(endIso)}&limit=${limit}`,
    );
    if (!response.ok) {
      throw new Error(`Replay fetch failed: ${response.status}`);
    }

    const replayResponse = await response.json();
    const replayRows: ReplayRow[] = Array.isArray(replayResponse)
      ? replayResponse
      : Array.isArray(replayResponse?.data)
        ? replayResponse.data
        : [];
    setReplayMeta(Array.isArray(replayResponse) ? null : replayResponse?.meta ?? null);

    const grouped = new Map<string, ReplayRow[]>();

    for (const row of replayRows) {
      const key = row.vehicle_id;
      const current = grouped.get(key);
      if (current) {
        current.push(row);
      } else {
        grouped.set(key, [row]);
      }
    }

    for (const rows of grouped.values()) {
      rows.sort((a, b) => a.time.localeCompare(b.time));
    }

    const nextReplayVehicles: Vehicle[] = [];

    for (const [vehicleId, rows] of grouped.entries()) {
      if (rows.length < 1) continue;

      const path: Array<[number, number, number]> = [];

      for (const row of rows) {
        if (row.point_state === "invalid_gap") continue;
        const point: [number, number, number] = [
          row.lon,
          row.lat,
          new Date(row.time).getTime() - start,
        ];
        path.push(point);
      }
      if (path.length < 1) continue;

      const last = rows[rows.length - 1];
      const updatedAt = path[path.length - 1][2];
      const vehicle: Vehicle = {
        id: vehicleId,
        lat: last.lat,
        lon: last.lon,
        line: last.route_id ?? "unknown",
        delay: 0,
        mode: last.mode ?? "unknown",
        updatedAt,
        path,
      };

      nextReplayVehicles.push(vehicle);
    }

    setReplayVehicles(nextReplayVehicles);
    setReplayStartTime(Date.now());
    setViewMode("replay");
  };

  // Last rendered positions per vehicle for smoothing.
  const visualPositionsRef = useRef<Record<string, [number, number]>>({});

  // Incremented on each animation frame.
  const [frameTick, setFrameTick] = useState(0);

  useEffect(() => {
    let animationFrameId: number;
    const animate = () => {
      setFrameTick((prev) => prev + 1);
      animationFrameId = requestAnimationFrame(animate);
    };
    animationFrameId = requestAnimationFrame(animate);
    return () => cancelAnimationFrame(animationFrameId);
  }, []);

  const activeStartTime = viewMode === "live" ? startTime : replayStartTime;
  const activePlaybackDelay = viewMode === "live" ? PLAYBACK_DELAY_MS : 0;
  const playbackTime = Math.max(0, Date.now() - activeStartTime - activePlaybackDelay);

  const validVehicles = useMemo(() => {
    return activeVehicles
      .map((vehicle) => {
        const cleanPath = (vehicle.path || []).filter(isValidPoint);
        if (cleanPath.length < 1) return null;
        return { ...vehicle, path: cleanPath };
      })
      .filter((v): v is Vehicle => v !== null);
  }, [activeVehicles]);

  const renderedVehicles = useMemo(() => {
    const currentVisuals = visualPositionsRef.current;

    return validVehicles
      .map((v) => {
        const targetData = getTargetPosition(v.path, playbackTime);

        if (!targetData) {
          delete currentVisuals[v.id];
          return null;
        }

        const targetPos = targetData.pos;
        let currentPos = currentVisuals[v.id];

        if (!currentPos) {
          currentPos = targetPos;
        } else {
          const distSq =
            Math.pow(targetPos[0] - currentPos[0], 2) +
            Math.pow(targetPos[1] - currentPos[1], 2);

          if (distSq > 0.00005) {
            currentPos = targetPos;
          } else {
            currentPos = [
              lerp(currentPos[0], targetPos[0], SMOOTHING_FACTOR),
              lerp(currentPos[1], targetPos[1], SMOOTHING_FACTOR),
            ];
          }
        }

        currentVisuals[v.id] = currentPos;

        return {
          ...v,
          renderPos: currentPos,
          isStale: targetData.isStale,
        } as RenderedVehicle;
      })
      .filter((v): v is RenderedVehicle => v !== null);
  }, [validVehicles, playbackTime, frameTick]);

  const layers = [
    new TripsLayer({
      id: "trips",
      data: validVehicles,
      getPath: (d: Vehicle): Array<[number, number]> =>
        d.path.map(([lon, lat]) => [lon, lat]),
      getTimestamps: (d: Vehicle) => d.path.map((p) => p[2]),

      getColor: (d) => {
        const lastTime = d.path[d.path.length - 1][2];
        const isStale = playbackTime > lastTime;

        if (isStale) return [80, 80, 80]; // Grey
        if (d.delay > 180) return [231, 76, 60]; // Red
        if (d.delay > 60) return [243, 156, 18]; // Orange
        return [46, 204, 113]; // Green
      },

      opacity: 0.6,
      widthMinPixels: 4,
      rounded: true,
      trailLength: TRAIL_LENGTH_MS,
      currentTime: playbackTime,
      shadowEnabled: false,
    }),

    new ScatterplotLayer({
      id: "vehicles-head",
      data: renderedVehicles,
      getPosition: (d: RenderedVehicle) => d.renderPos,

      getRadius: 15,
      radiusUnits: "meters",
      radiusMinPixels: 4,
      radiusMaxPixels: 25,

      stroked: true,
      getLineWidth: 2,
      getFillColor: [255, 255, 255, 255],
      pickable: true,

      getLineColor: (d: RenderedVehicle) => {
        if (d.delay < 60) return [46, 204, 113];
        if (d.delay < 180) return [243, 156, 18];
        return [231, 76, 60];
      },

      parameters: { depthTest: false },

      updateTriggers: {
        getFillColor: [playbackTime],
        getLineColor: [playbackTime],
      },
    }),
  ];

  return (
    <div
      style={{
        width: "100vw",
        height: "100vh",
        position: "relative",
        background: "#111",
      }}
    >
      <div
        style={{
          position: "absolute",
          zIndex: 10,
          top: 20,
          left: 20,
          background: "rgba(20,20,20,0.9)",
          padding: "15px",
          borderRadius: "8px",
          border: "1px solid #333",
          color: "#fff",
          fontFamily: "monospace",
          fontSize: "12px",
          boxShadow: "0 4px 6px rgba(0,0,0,0.3)",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "10px",
            marginBottom: "8px",
          }}
        >
          <div
            style={{
              width: 10,
              height: 10,
              borderRadius: "50%",
              background:
                viewMode === "live"
                  ? isConnected
                    ? "#2ecc71"
                    : "#e74c3c"
                  : "#3498db",
              boxShadow:
                viewMode === "live" && isConnected ? "0 0 8px #2ecc71" : "none",
            }}
          />
          <span style={{ fontWeight: "bold", letterSpacing: "1px" }}>
            {viewMode === "live"
              ? isConnected
                ? "PRAGUE LIVE"
                : "DISCONNECTED"
              : "PRAGUE REPLAY"}
          </span>
        </div>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: "10px",
            color: "#aaa",
          }}
        >
          <div>
            Active:{" "}
            <span style={{ color: "#fff" }}>
              {renderedVehicles.filter((v) => !v.isStale).length}
            </span>
          </div>
          <div>
            Waiting:{" "}
            <span style={{ color: "#fff" }}>
              {renderedVehicles.filter((v) => v.isStale).length}
            </span>
          </div>

          <div style={{ display: "flex", gap: 8 }}>
            <button
              onClick={() => setViewMode("live")}
              style={{ opacity: viewMode === "live" ? 1 : 0.6 }}
            >
              Live
            </button>
            <button
              onClick={() => loadReplay()}
              style={{ opacity: viewMode === "replay" ? 1 : 0.6 }}
            >
              Replay
            </button>
          </div>
          Mode: {viewMode.charAt(0).toUpperCase()}
          {viewMode.substring(1, viewMode.length)}
        </div>

        <div
          style={{
            marginTop: "10px",
            borderTop: "1px solid #444",
            paddingTop: "5px",
            fontSize: "10px",
          }}
        >
          Buffer: {PLAYBACK_DELAY_MS / 1000}s | Trail: {TRAIL_LENGTH_MS / 1000}s
        </div>
        {viewMode === "replay" && replayMeta && (
          <div
            style={{
              marginTop: "8px",
              borderTop: "1px solid #444",
              paddingTop: "6px",
              fontSize: "10px",
              color: "#cfd8dc",
            }}
          >
            Replay: {replayMeta.returned_count}/{replayMeta.requested_limit} |
            has_more: {String(replayMeta.has_more)} | db: {replayMeta.query_time_ms}
            ms
          </div>
        )}
      </div>

      <DeckGL
        initialViewState={INITIAL_VIEW_STATE}
        controller={true}
        layers={layers}
        getTooltip={({ object }: PickingInfo) => {
          const vehicle = object as RenderedVehicle | null | undefined;
          if (!vehicle) return null;

          const realNow =
            viewMode === "live" ? Date.now() - startTime : playbackTime;
          const lastTimestamp = vehicle.path[vehicle.path.length - 1][2];
          const secondsAgo = (realNow - lastTimestamp) / 1000;

          return {
            html: `
              <div style="font-family: sans-serif; font-size: 12px; padding: 4px; color: #fff; background: #000;">
                <strong style="font-size: 14px">Line ${vehicle.line}</strong><br/>
                <span style="color: ${vehicle.delay > 180 ? "#e74c3c" : "#2ecc71"}">
                  ${vehicle.delay > 0 ? `+${Math.round(vehicle.delay / 60)} min` : "On time"}
                </span><br/>
                <span style="color: #888; font-size: 10px">ID: ${vehicle.id}</span><br/>
                ${vehicle.isStale ? "Stationary (Waiting)" : "Moving"}<br/>
                Last update: ${Math.round(Math.max(0, secondsAgo))}s ago
              </div>
            `,
          };
        }}
      >
        <MapView mapStyle={MAP_STYLE} />
      </DeckGL>
    </div>
  );
}
