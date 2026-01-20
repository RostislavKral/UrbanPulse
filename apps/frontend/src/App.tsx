import { TripsLayer } from "@deck.gl/geo-layers";
import { ScatterplotLayer } from "@deck.gl/layers";
import { DeckGL, PickingInfo } from "deck.gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useMemo, useRef, useState } from "react";
import Map from "react-map-gl/maplibre";
import { useLiveVehicles } from "./hooks/useLiveVehicles";
import { RenderedVehicle, Vehicle } from "./types/vehicle";

// --- CONFIGURATION ---
// TODO: Move to .env
const WS_URL = "ws://127.0.0.1:3000/ws";
const MAP_STYLE =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";

const INITIAL_VIEW_STATE = {
  longitude: 14.4378,
  latitude: 50.0755,
  zoom: 12,
  pitch: 0,
  bearing: 0,
};

/**
 * Playback delay in milliseconds (30 seconds).
 * Creates a buffer allowing the system to interpolate movement between past points.
 * A higher value results in smoother movement but higher latency.
 */
const PLAYBACK_DELAY_MS = 30000;

/**
 * Length of the visual trail behind the vehicle in milliseconds (2.5 minutes).
 */
const TRAIL_LENGTH_MS = 150000;

/**
 * Threshold to consider a vehicle "dead" (5 minutes).
 * If no data is received for this duration, the vehicle is removed from the map.
 */
const DROP_THRESHOLD_MS = 5 * 60 * 1000;

/**
 * Linear interpolation factor for visual smoothing (0.0 - 1.0).
 * Lower values = smoother, heavier feel (slower to react to jumps).
 * Higher values = snappier, more jittery.
 */
const SMOOTHING_FACTOR = 0.1;

// --- HELPER FUNCTIONS ---

/**
 * Linear Interpolation function.
 */
const lerp = (start: number, end: number, factor: number): number =>
  start + (end - start) * factor;

/**
 * Type guard to ensure a point has valid [lon, lat, timestamp] coordinates.
 */
const isValidPoint = (point: unknown): point is [number, number, number] =>
  Array.isArray(point) && point.length >= 3;

interface TargetPositionResult {
  pos: [number, number];
  isStale: boolean;
}

/**
 * Calculates the target position of a vehicle at a specific playback time.
 * Handles three states:
 * 1. Dead/Zombie (expired data) -> returns null.
 * 2. Waiting (caught up to latest data) -> returns last known position (stale).
 * 3. Moving (between two known points) -> returns interpolated position.
 */
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
  const { vehicles, isConnected, startTime } = useLiveVehicles(WS_URL);

  // Stores the last rendered visual position for every vehicle ID.
  // This allows us to apply LERP smoothing between the current visual state
  // and the new logical target state.
  const visualPositionsRef = useRef<Record<string, [number, number]>>({});

  // State used to force a re-render on every animation frame.
  const [frameTick, setFrameTick] = useState(0);

  // Animation Loop
  useEffect(() => {
    let animationFrameId: number;
    const animate = () => {
      setFrameTick((prev) => prev + 1);
      animationFrameId = requestAnimationFrame(animate);
    };
    animationFrameId = requestAnimationFrame(animate);
    return () => cancelAnimationFrame(animationFrameId);
  }, []);

  // Calculate current playback time relative to application start
  const playbackTime = Math.max(0, Date.now() - startTime - PLAYBACK_DELAY_MS);

  // Data Preparation & Validation
  const validVehicles = useMemo(() => {
    return vehicles
      .map((vehicle) => {
        const cleanPath = (vehicle.path || []).filter(isValidPoint);
        if (cleanPath.length < 1) return null;
        return { ...vehicle, path: cleanPath };
      })
      .filter((v): v is Vehicle => v !== null);
  }, [vehicles]);

  // Position Calculation (Logic + Smoothing)
  // This runs on every frame tick to ensure smooth movement.
  const renderedVehicles = useMemo(() => {
    const currentVisuals = visualPositionsRef.current;

    return validVehicles
      .map((v) => {
        // Determine logical position based on history
        const targetData = getTargetPosition(v.path, playbackTime);

        if (!targetData) {
          // Cleanup visual state if vehicle expired
          delete currentVisuals[v.id];
          return null;
        }

        const targetPos = targetData.pos;
        let currentPos = currentVisuals[v.id];

        if (!currentPos) {
          // New vehicle: snap immediately to target
          currentPos = targetPos;
        } else {
          // Existing vehicle: Apply LERP smoothing

          // Calculate squared distance to detect massive jumps (teleportation)
          const distSq =
            Math.pow(targetPos[0] - currentPos[0], 2) +
            Math.pow(targetPos[1] - currentPos[1], 2);

          // Threshold approx 300-500m. If the jump is too large, teleport instead of smoothing
          // to avoid vehicles flying across the map.
          if (distSq > 0.00005) {
            currentPos = targetPos;
          } else {
            currentPos = [
              lerp(currentPos[0], targetPos[0], SMOOTHING_FACTOR),
              lerp(currentPos[1], targetPos[1], SMOOTHING_FACTOR),
            ];
          }
        }

        // Update visual state reference
        currentVisuals[v.id] = currentPos;

        return {
          ...v,
          renderPos: currentPos,
          isStale: targetData.isStale,
        } as RenderedVehicle;
      })
      .filter((v): v is RenderedVehicle => v !== null);
  }, [validVehicles, playbackTime, frameTick]);

  // --- LAYERS ---
  const layers = [
    // Layer 1: "Historical" Trails
    new TripsLayer({
      id: "trips",
      data: validVehicles,
      getPath: (d) => d.path.map((p) => [p[0], p[1]]),
      getTimestamps: (d) => d.path.map((p) => p[2]),

      // Color logic: Grey if stale, otherwise Red/Orange/Green based on delay
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

    // Layer 2: Vehicle Heads (Dots)
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

      // Outline color: Same logic as trails
      getLineColor: (d: RenderedVehicle) => {
        if (d.delay < 60) return [46, 204, 113];
        if (d.delay < 180) return [243, 156, 18];
        return [231, 76, 60];
      },

      parameters: { depthTest: false },

      // Ensure colors update immediately when delay or state changes
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
      {/* UI OVERLAY */}
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
              background: isConnected ? "#2ecc71" : "#e74c3c",
              boxShadow: isConnected ? "0 0 8px #2ecc71" : "none",
            }}
          />
          <span style={{ fontWeight: "bold", letterSpacing: "1px" }}>
            {isConnected ? "PRAGUE LIVE" : "DISCONNECTED"}
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
      </div>

      <DeckGL
        initialViewState={INITIAL_VIEW_STATE}
        controller={true}
        layers={layers}
        getTooltip={({ object }: PickingInfo<RenderedVehicle>) => {
          if (!object) return null;

          // Calculate time since the last real update of vehicle
          const realNow = Date.now() - startTime;
          const lastTimestamp = object.path[object.path.length - 1][2];
          const secondsAgo = (realNow - lastTimestamp) / 1000;

          return {
            html: `
              <div style="font-family: sans-serif; font-size: 12px; padding: 4px; color: #fff; background: #000;">
                <strong style="font-size: 14px">Line ${object.line}</strong><br/>
                <span style="color: ${object.delay > 180 ? "#e74c3c" : "#2ecc71"}">
                  ${object.delay > 0 ? `+${Math.round(object.delay / 60)} min` : "On time"}
                </span><br/>
                <span style="color: #888; font-size: 10px">ID: ${object.id}</span><br/>
                ${object.isStale ? "Stationary (Waiting)" : "Moving"}<br/>
                Last update: ${Math.round(Math.max(0, secondsAgo))}s ago
              </div>
            `,
          };
        }}
      >
        <Map mapStyle={MAP_STYLE} />
      </DeckGL>
    </div>
  );
}
