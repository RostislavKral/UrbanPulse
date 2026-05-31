import { TripsLayer } from "@deck.gl/geo-layers";
import { ScatterplotLayer } from "@deck.gl/layers";
import { DeckGL, PickingInfo } from "deck.gl";
import "maplibre-gl/dist/maplibre-gl.css";
import {
  startTransition,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import MapView from "react-map-gl/maplibre";
import { useLiveVehicles } from "./hooks/useLiveVehicles";
import { RenderedVehicle, Vehicle, VehicleMode } from "./types/vehicle";
import { isWithinPragueBounds } from "./utils/geo";

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

const PLAYBACK_DELAY_MS = 30000;
const TRAIL_LENGTH_MS = 150000;
const DROP_THRESHOLD_MS = 5 * 60 * 1000;
const SMOOTHING_FACTOR = 0.1;
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
  total_count: number | null;
  loaded_count: number;
  has_more: boolean;
  query_time_ms: number;
  next_cursor_time: string | null;
  next_cursor_vehicle_id: string | null;
};
type ReplayBoundsMeta = {
  min_time: string | null;
  max_time: string | null;
  query_time_ms: number;
};
type ReplayBoundsResponse = {
  meta: ReplayBoundsMeta;
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
type ReplayFilters = {
  mode: VehicleMode | "all";
  lineQuery: string;
  includeInterpolated: boolean;
  hideVehiclesWithLatestInvalidGap: boolean;
};
type ReplayStats = {
  observedPoints: number;
  interpolatedPoints: number;
  invalidGapPoints: number;
  shownVehicles: number;
};
type DelayAlertMeta = {
  artifact_path: string;
  artifact_mtime: string;
  total_count: number;
  returned_count: number;
  alerts_only: boolean;
};
type DelayAlertRecord = {
  vehicle_id: string;
  time: string;
  delay: number | null;
  speed: number | null;
  line: string | null;
  route_id: string | null;
  state_position: string | null;
  target_delay_delta: number | null;
  delay_increase_risk: number;
  delay_increase_alert: boolean;
  actual_increase_60s?: number | null;
};
type DelayAlertsResponse = {
  meta: DelayAlertMeta;
  data: DelayAlertRecord[];
};
type RiskListFilter = "all" | "on_map";

const REPLAY_MODES: Array<VehicleMode | "all"> = [
  "all",
  "tram",
  "metro",
  "rail",
  "bus",
  "ferry",
  "trolleybus",
  "unknown",
];
const DEFAULT_REPLAY_FILTERS: ReplayFilters = {
  mode: "all",
  lineQuery: "",
  includeInterpolated: true,
  hideVehiclesWithLatestInvalidGap: false,
};
const REPLAY_WINDOW_MINUTES = [1, 5, 15, 30];
const REPLAY_PAGE_SIZE = 5000;
const REPLAY_REFRESH_DEBOUNCE_MS = 150;
const DELAY_ALERT_LIMIT = 12;
const DELAY_ALERT_REFRESH_MS = 60_000;

function formatPercent(value: number): string {
  return `${Math.round(value * 100)}%`;
}

function formatSeconds(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "--";
  const sign = value > 0 ? "+" : "";
  if (Math.abs(value) < 60) return `${sign}${Math.round(value)}s`;
  return `${sign}${(value / 60).toFixed(1)}m`;
}

function formatAlertTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(date);
}

function appendReplayRows(
  grouped: Map<string, ReplayRow[]>,
  rows: ReplayRow[],
): void {
  for (const row of rows) {
    const current = grouped.get(row.vehicle_id);
    if (current) {
      current.push(row);
    } else {
      grouped.set(row.vehicle_id, [row]);
    }
  }
}

// Collapse raw replay rows into renderable vehicle paths after applying UI filters.
function buildReplayVehicles(
  grouped: Map<string, ReplayRow[]>,
  replayStartMs: number,
  filters: ReplayFilters,
): { vehicles: Vehicle[]; stats: ReplayStats } {
  const nextReplayVehicles: Vehicle[] = [];
  const stats: ReplayStats = {
    observedPoints: 0,
    interpolatedPoints: 0,
    invalidGapPoints: 0,
    shownVehicles: 0,
  };

  for (const [vehicleId, vehicleRows] of grouped.entries()) {
    if (vehicleRows.length < 1) continue;

    const boundedRows = vehicleRows.filter((row) =>
      isWithinPragueBounds(row.lon, row.lat),
    );
    if (boundedRows.length < 1) continue;

    const last = boundedRows[boundedRows.length - 1];
    const mode = (last.mode ?? "unknown") as VehicleMode;
    const line = last.route_id ?? "unknown";
    const latestPointIsInvalidGap = last.point_state === "invalid_gap";

    if (filters.mode !== "all" && mode !== filters.mode) continue;
    if (
      filters.lineQuery &&
      !line.toLowerCase().includes(filters.lineQuery.toLowerCase())
    ) {
      continue;
    }
    if (filters.hideVehiclesWithLatestInvalidGap && latestPointIsInvalidGap) {
      continue;
    }

    const path: Array<[number, number, number]> = [];

    for (const row of boundedRows) {
      if (row.point_state === "invalid_gap") {
        stats.invalidGapPoints += 1;
        continue;
      }
      if (row.point_state === "interpolated") {
        stats.interpolatedPoints += 1;
        if (!filters.includeInterpolated) continue;
      }
      if (row.point_state === "observed") {
        stats.observedPoints += 1;
      }
      path.push([row.lon, row.lat, new Date(row.time).getTime() - replayStartMs]);
    }

    if (path.length < 1) continue;

    nextReplayVehicles.push({
      id: vehicleId,
      lat: last.lat,
      lon: last.lon,
      line,
      delay: 0,
      mode,
      updatedAt: path[path.length - 1][2],
      path,
    });
    stats.shownVehicles += 1;
  }

  return { vehicles: nextReplayVehicles, stats };
}

// Resolve the vehicle position for the current playback time within a sampled path.
function getTargetPosition(
  path: Array<[number, number, number]>,
  currentTime: number,
): TargetPositionResult | null {
  if (path.length < 1) return null;

  const lastPoint = path[path.length - 1];
  const lastTime = lastPoint[2];

  if (currentTime - lastTime > DROP_THRESHOLD_MS) {
    return null;
  }

  if (currentTime >= lastTime) {
    return {
      pos: [lastPoint[0], lastPoint[1]],
      isStale: true,
    };
  }

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

  return { pos: [path[0][0], path[0][1]], isStale: false };
}

export default function App() {
  const [viewMode, setViewMode] = useState<ViewMode>("live");
  const { vehicles, isConnected, startTime } = useLiveVehicles(
    WS_URL,
    viewMode === "live",
  );
  const [replayVehicles, setReplayVehicles] = useState<Vehicle[]>([]);
  const [replayMeta, setReplayMeta] = useState<ReplayMeta | null>(null);
  const [isReplayPlaying, setIsReplayPlaying] = useState(true);
  const [replaySpeed, setReplaySpeed] = useState(1);
  const [replayElapsedMs, setReplayElapsedMs] = useState(0);
  const [replayWindowDraftMinutes, setReplayWindowDraftMinutes] = useState(1);
  const [activeReplayWindowMinutes, setActiveReplayWindowMinutes] = useState(1);
  const [isReplayLoading, setIsReplayLoading] = useState(false);
  const [replayError, setReplayError] = useState<string | null>(null);
  const [replayFilters, setReplayFilters] =
    useState<ReplayFilters>(DEFAULT_REPLAY_FILTERS);
  const [replayStats, setReplayStats] = useState<ReplayStats>({
    observedPoints: 0,
    interpolatedPoints: 0,
    invalidGapPoints: 0,
    shownVehicles: 0,
  });
  const [delayAlerts, setDelayAlerts] = useState<DelayAlertRecord[]>([]);
  const [delayAlertMeta, setDelayAlertMeta] = useState<DelayAlertMeta | null>(
    null,
  );
  const [isDelayAlertsLoading, setIsDelayAlertsLoading] = useState(false);
  const [delayAlertsError, setDelayAlertsError] = useState<string | null>(null);
  const [isRiskPanelOpen, setIsRiskPanelOpen] = useState(true);
  const [riskListFilter, setRiskListFilter] = useState<RiskListFilter>("all");
  const replayLoadIdRef = useRef(0);
  const replayFiltersRef = useRef<ReplayFilters>(DEFAULT_REPLAY_FILTERS);
  const replayRowsRef = useRef<Map<string, ReplayRow[]>>(new Map());
  const replayStartMsRef = useRef(0);
  const lastAnimationTimeRef = useRef<number | null>(null);
  const replayRefreshTimerRef = useRef<number | null>(null);

  const activeVehicles = viewMode === "live" ? vehicles : replayVehicles;
  const activeVehicleIds = useMemo(() => {
    return new Set(activeVehicles.map((vehicle) => vehicle.id));
  }, [activeVehicles]);
  const delayAlertByVehicle = useMemo(() => {
    return new Map(delayAlerts.map((alert) => [alert.vehicle_id, alert]));
  }, [delayAlerts]);
  const onMapDelayAlertCount = useMemo(() => {
    return delayAlerts.filter((alert) => activeVehicleIds.has(alert.vehicle_id))
      .length;
  }, [activeVehicleIds, delayAlerts]);
  const visibleDelayAlerts = useMemo(() => {
    if (riskListFilter === "on_map") {
      return delayAlerts.filter((alert) => activeVehicleIds.has(alert.vehicle_id));
    }
    return delayAlerts;
  }, [activeVehicleIds, delayAlerts, riskListFilter]);
  const topDelayAlert = delayAlerts[0] ?? null;
  const loadDelayAlerts = useCallback(async () => {
    setIsDelayAlertsLoading(true);
    setDelayAlertsError(null);

    try {
      const params = new URLSearchParams({
        limit: String(DELAY_ALERT_LIMIT),
      });
      const response = await fetch(
        `${API_URL}/delay-increase-alerts?${params.toString()}`,
      );
      if (!response.ok) {
        throw new Error(`Delay alert fetch failed: ${response.status}`);
      }

      const payload = (await response.json()) as DelayAlertsResponse;
      setDelayAlerts(payload.data);
      setDelayAlertMeta(payload.meta);
    } catch (error) {
      setDelayAlertsError(
        error instanceof Error ? error.message : "Delay alert loading failed",
      );
    } finally {
      setIsDelayAlertsLoading(false);
    }
  }, []);
  const rebuildReplayVehicles = (
    filters: ReplayFilters,
    resetVisuals: boolean = false,
  ) => {
    const nextReplayState = buildReplayVehicles(
      replayRowsRef.current,
      replayStartMsRef.current,
      filters,
    );

    startTransition(() => {
      setReplayVehicles(nextReplayState.vehicles);
      setReplayStats(nextReplayState.stats);
    });

    if (resetVisuals) {
      visualPositionsRef.current = {};
    }
  };
  const scheduleReplayRefresh = () => {
    if (replayRefreshTimerRef.current !== null) return;

    replayRefreshTimerRef.current = window.setTimeout(() => {
      replayRefreshTimerRef.current = null;
      rebuildReplayVehicles(replayFiltersRef.current);
    }, REPLAY_REFRESH_DEBOUNCE_MS);
  };

  const loadReplay = async () => {
    const limit = REPLAY_PAGE_SIZE;
    const loadId = replayLoadIdRef.current + 1;
    replayLoadIdRef.current = loadId;
    let loadedCount = 0;
    let cursorTime: string | null = null;
    let cursorVehicleId: string | null = null;
    let totalCount = 0;

    if (replayRefreshTimerRef.current !== null) {
      window.clearTimeout(replayRefreshTimerRef.current);
      replayRefreshTimerRef.current = null;
    }
    replayRowsRef.current = new Map();
    setReplayVehicles([]);
    setReplayMeta(null);
    setIsReplayLoading(true);
    setReplayError(null);
    setReplayStats({
      observedPoints: 0,
      interpolatedPoints: 0,
      invalidGapPoints: 0,
      shownVehicles: 0,
    });
    setReplayElapsedMs(0);
    setIsReplayPlaying(true);
    setReplaySpeed(1);
    setActiveReplayWindowMinutes(replayWindowDraftMinutes);
    replayFiltersRef.current = DEFAULT_REPLAY_FILTERS;
    setReplayFilters(DEFAULT_REPLAY_FILTERS);
    lastAnimationTimeRef.current = null;
    visualPositionsRef.current = {};

    try {
      const boundsResponse = await fetch(`${API_URL}/replay/bounds`);
      if (!boundsResponse.ok) {
        throw new Error(`Replay bounds fetch failed: ${boundsResponse.status}`);
      }

      const bounds = (await boundsResponse.json()) as ReplayBoundsResponse;
      if (!bounds.meta.max_time) {
        throw new Error("Replay data is empty");
      }

      const end = new Date(bounds.meta.max_time).getTime();
      if (!Number.isFinite(end)) {
        throw new Error("Replay bounds returned an invalid max_time");
      }

      const start = end - replayWindowDraftMinutes * 60 * 1000;
      const endIso = new Date(end).toISOString();
      const startIso = new Date(start).toISOString();
      replayStartMsRef.current = start;

      while (true) {
        const params = new URLSearchParams({
          start_ts: startIso,
          end_ts: endIso,
          limit: String(limit),
        });

        if (cursorTime && cursorVehicleId) {
          params.set("cursor_time", cursorTime);
          params.set("cursor_vehicle_id", cursorVehicleId);
        }

        const response = await fetch(`${API_URL}/replay?${params.toString()}`);
        if (!response.ok) {
          throw new Error(`Replay fetch failed: ${response.status}`);
        }

        const replayResponse = (await response.json()) as {
          data: ReplayRow[];
          meta: ReplayMeta;
        };

        if (replayLoadIdRef.current !== loadId) {
          return;
        }

        const pageRows = replayResponse.data;
        const meta = replayResponse.meta;
        loadedCount += pageRows.length;
        totalCount = meta.total_count ?? totalCount;

        appendReplayRows(replayRowsRef.current, pageRows);
        setReplayMeta({
          ...meta,
          loaded_count: loadedCount,
          total_count: totalCount,
        });

        if (loadedCount === pageRows.length) {
          rebuildReplayVehicles(replayFiltersRef.current, true);
          setViewMode("replay");
        } else {
          scheduleReplayRefresh();
        }

        if (!meta.has_more) {
          rebuildReplayVehicles(replayFiltersRef.current);
          setIsReplayLoading(false);
          return;
        }

        if (!meta.next_cursor_time || !meta.next_cursor_vehicle_id) {
          throw new Error("Replay pagination is missing cursor metadata");
        }

        cursorTime = meta.next_cursor_time;
        cursorVehicleId = meta.next_cursor_vehicle_id;
      }
    } catch (error) {
      if (replayLoadIdRef.current !== loadId) {
        return;
      }
      setIsReplayLoading(false);
      setReplayError(
        error instanceof Error ? error.message : "Replay loading failed",
      );
    }
  };

  useEffect(() => {
    replayFiltersRef.current = replayFilters;
  }, [replayFilters]);

  useEffect(() => {
    if (viewMode !== "replay") return;

    rebuildReplayVehicles(replayFilters, true);
  }, [replayFilters, viewMode]);

  useEffect(() => {
    loadDelayAlerts();
    const intervalId = window.setInterval(
      loadDelayAlerts,
      DELAY_ALERT_REFRESH_MS,
    );

    return () => {
      window.clearInterval(intervalId);
    };
  }, [loadDelayAlerts]);

  useEffect(() => {
    return () => {
      if (replayRefreshTimerRef.current !== null) {
        window.clearTimeout(replayRefreshTimerRef.current);
      }
    };
  }, []);

  const visualPositionsRef = useRef<Record<string, [number, number]>>({});
  const [frameTick, setFrameTick] = useState(0);

  useEffect(() => {
    let animationFrameId: number;
    const animate = (now: number) => {
      const previous = lastAnimationTimeRef.current;
      lastAnimationTimeRef.current = now;

      if (
        previous !== null &&
        viewMode === "replay" &&
        isReplayPlaying
      ) {
        const deltaMs = now - previous;
        setReplayElapsedMs((prev) => prev + deltaMs * replaySpeed);
      }

      setFrameTick((prev) => prev + 1);
      animationFrameId = requestAnimationFrame(animate);
    };
    animationFrameId = requestAnimationFrame(animate);
    return () => {
      cancelAnimationFrame(animationFrameId);
      lastAnimationTimeRef.current = null;
    };
  }, [viewMode, isReplayPlaying, replaySpeed]);

  const activePlaybackDelay = viewMode === "live" ? PLAYBACK_DELAY_MS : 0;
  const playbackTime =
    viewMode === "live"
      ? Math.max(0, Date.now() - startTime - activePlaybackDelay)
      : Math.max(0, replayElapsedMs);
  const replayDurationMs = replayMeta
    ? Math.max(
        0,
        new Date(replayMeta.end_ts).getTime() -
          new Date(replayMeta.start_ts).getTime(),
      )
    : 0;
  const replayProgressPct =
    replayMeta && replayMeta.total_count && replayMeta.total_count > 0
      ? Math.min(100, (replayMeta.loaded_count / replayMeta.total_count) * 100)
      : 0;
  const replayWindowDirty = replayWindowDraftMinutes !== activeReplayWindowMinutes;

  useEffect(() => {
    if (viewMode !== "replay" || replayDurationMs <= 0) return;
    if (replayElapsedMs < replayDurationMs) return;

    if (isReplayPlaying) {
      setIsReplayPlaying(false);
    }
    if (replayElapsedMs !== replayDurationMs) {
      setReplayElapsedMs(replayDurationMs);
    }
  }, [isReplayPlaying, replayDurationMs, replayElapsedMs, viewMode]);

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
    // frameTick intentionally refreshes smoothed positions between data updates.
    // eslint-disable-next-line react-hooks/exhaustive-deps
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

        if (isStale) return [80, 80, 80];
        if (delayAlertByVehicle.has(d.id)) return [255, 82, 82];
        if (d.delay > 180) return [231, 76, 60];
        if (d.delay > 60) return [243, 156, 18];
        return [46, 204, 113];
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
      getFillColor: (d: RenderedVehicle) =>
        delayAlertByVehicle.has(d.id)
          ? [255, 82, 82, 255]
          : [255, 255, 255, 255],
      pickable: true,

      getLineColor: (d: RenderedVehicle) => {
        if (delayAlertByVehicle.has(d.id)) return [255, 213, 79];
        if (d.delay < 60) return [46, 204, 113];
        if (d.delay < 180) return [243, 156, 18];
        return [231, 76, 60];
      },

      parameters: { depthTest: false },

      updateTriggers: {
        getFillColor: [delayAlerts],
        getLineColor: [playbackTime, delayAlerts],
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
              onClick={() => {
                replayLoadIdRef.current += 1;
                setIsReplayLoading(false);
                setIsReplayPlaying(false);
                setReplayElapsedMs(0);
                lastAnimationTimeRef.current = null;
                setViewMode("live");
              }}
              style={{ opacity: viewMode === "live" ? 1 : 0.6 }}
            >
              Live
            </button>
            <button
              onClick={() => loadReplay()}
              disabled={isReplayLoading}
              style={{ opacity: viewMode === "replay" ? 1 : 0.6 }}
            >
              {isReplayLoading ? "Loading..." : "Replay"}
            </button>
          </div>
          <label style={{ display: "flex", gap: "6px", alignItems: "center" }}>
            Window
            <select
              value={replayWindowDraftMinutes}
              onChange={(event) =>
                setReplayWindowDraftMinutes(Number(event.target.value))
              }
              disabled={isReplayLoading}
            >
              {REPLAY_WINDOW_MINUTES.map((minutes) => (
                <option key={minutes} value={minutes}>
                  {minutes}m
                </option>
              ))}
            </select>
          </label>
          <button
            onClick={() => loadReplay()}
            disabled={isReplayLoading || !replayWindowDirty}
            style={{ opacity: replayWindowDirty ? 1 : 0.6 }}
          >
            Apply
          </button>
          Mode: {viewMode.charAt(0).toUpperCase()}
          {viewMode.substring(1, viewMode.length)}
        </div>

        {viewMode === "replay" && (
          <div
            style={{
              marginTop: "10px",
              display: "flex",
              flexWrap: "wrap",
              gap: "8px",
              alignItems: "center",
            }}
          >
            <button onClick={() => setIsReplayPlaying((prev) => !prev)}>
              {isReplayPlaying ? "Pause" : "Play"}
            </button>
            <button
              onClick={() => {
                setReplayElapsedMs(0);
                visualPositionsRef.current = {};
              }}
            >
              Reset
            </button>
            <label style={{ display: "flex", gap: "6px", alignItems: "center" }}>
              Speed
              <select
                value={replaySpeed}
                onChange={(event) =>
                  setReplaySpeed(Number(event.target.value))
                }
              >
                <option value={0.5}>0.5x</option>
                <option value={1}>1x</option>
                <option value={2}>2x</option>
                <option value={4}>4x</option>
              </select>
            </label>
          </div>
        )}

        {viewMode === "replay" && (
          <div
            style={{
              marginTop: "8px",
              display: "grid",
              gridTemplateColumns: "1fr 1fr",
              gap: "8px",
              color: "#cfd8dc",
            }}
          >
            <label style={{ display: "flex", gap: "6px", alignItems: "center" }}>
              Mode
              <select
                value={replayFilters.mode}
                onChange={(event) =>
                  setReplayFilters((prev) => ({
                    ...prev,
                    mode: event.target.value as ReplayFilters["mode"],
                  }))
                }
              >
                {REPLAY_MODES.map((mode) => (
                  <option key={mode} value={mode}>
                    {mode}
                  </option>
                ))}
              </select>
            </label>
            <label style={{ display: "flex", gap: "6px", alignItems: "center" }}>
              Line
              <input
                value={replayFilters.lineQuery}
                onChange={(event) =>
                  setReplayFilters((prev) => ({
                    ...prev,
                    lineQuery: event.target.value,
                  }))
                }
                placeholder="all"
                style={{ width: "100%" }}
              />
            </label>
            <label style={{ display: "flex", gap: "6px", alignItems: "center" }}>
              <input
                type="checkbox"
                checked={replayFilters.includeInterpolated}
                onChange={(event) =>
                  setReplayFilters((prev) => ({
                    ...prev,
                    includeInterpolated: event.target.checked,
                  }))
                }
              />
              Include interpolated
            </label>
            <label style={{ display: "flex", gap: "6px", alignItems: "center" }}>
              <input
                type="checkbox"
                checked={replayFilters.hideVehiclesWithLatestInvalidGap}
                onChange={(event) =>
                  setReplayFilters((prev) => ({
                    ...prev,
                    hideVehiclesWithLatestInvalidGap: event.target.checked,
                  }))
                }
              />
              Hide latest invalid-gap
            </label>
          </div>
        )}

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
            Load: {isReplayLoading ? "fetching" : "ready"} | progress:{" "}
            {replayMeta.total_count ? replayProgressPct.toFixed(1) : "--"}% |
            window: {activeReplayWindowMinutes}m | Replay: {replayMeta.loaded_count}/
            {replayMeta.total_count ?? "?"} | has_more: {String(replayMeta.has_more)} |
            db: {replayMeta.query_time_ms}ms | t:{" "}
            {(playbackTime / 1000).toFixed(1)}s /{" "}
            {(replayDurationMs / 1000).toFixed(1)}s | speed: {replaySpeed}x |
            shown: {replayStats.shownVehicles} | obs: {replayStats.observedPoints}{" "}
            int: {replayStats.interpolatedPoints} | invalid:{" "}
            {replayStats.invalidGapPoints}
          </div>
        )}
        {viewMode === "replay" && replayError && (
          <div
            style={{
              marginTop: "8px",
              borderTop: "1px solid #5c2b2b",
              paddingTop: "6px",
              fontSize: "10px",
              color: "#ffb3b3",
            }}
          >
            Replay error: {replayError}
          </div>
        )}
        {viewMode === "replay" && replayMeta && (
          <div
            style={{
              marginTop: "6px",
              height: "4px",
              background: "#222",
              borderRadius: "999px",
              overflow: "hidden",
            }}
          >
            <div
              style={{
                width: `${replayProgressPct}%`,
                height: "100%",
                background: isReplayLoading ? "#3498db" : "#2ecc71",
                transition: "width 120ms linear",
              }}
            />
          </div>
        )}
      </div>

      <div
        className={`risk-panel ${isRiskPanelOpen ? "" : "risk-panel--collapsed"}`}
      >
        <div className="risk-panel__header">
          <div className="risk-panel__title">
            <div
              className={`risk-panel__dot ${
                delayAlertsError ? "risk-panel__dot--error" : ""
              }`}
            />
            <div>
              <div className="risk-panel__eyebrow">HIGH CONFIDENCE</div>
              <div className="risk-panel__heading">Delay Increase Risk</div>
            </div>
          </div>
          <div className="risk-panel__actions">
            <button
              className="risk-panel__button"
              onClick={() => loadDelayAlerts()}
              disabled={isDelayAlertsLoading}
              title="Refresh alerts"
            >
              {isDelayAlertsLoading ? "..." : "Refresh"}
            </button>
            <button
              className="risk-panel__button"
              onClick={() => setIsRiskPanelOpen((prev) => !prev)}
              title={isRiskPanelOpen ? "Collapse alerts" : "Expand alerts"}
            >
              {isRiskPanelOpen ? "-" : "+"}
            </button>
          </div>
        </div>

        {isRiskPanelOpen && (
          <div className="risk-panel__body">
            <div className="risk-panel__summary">
              <div className="risk-panel__metric">
                <div className="risk-panel__metric-label">Alerts</div>
                <div className="risk-panel__metric-value">
                  {delayAlertMeta?.total_count ?? delayAlerts.length}
                </div>
              </div>
              <div className="risk-panel__metric">
                <div className="risk-panel__metric-label">On Map</div>
                <div className="risk-panel__metric-value">
                  {onMapDelayAlertCount}
                </div>
              </div>
              <div className="risk-panel__metric">
                <div className="risk-panel__metric-label">Top Risk</div>
                <div className="risk-panel__metric-value">
                  {topDelayAlert
                    ? formatPercent(topDelayAlert.delay_increase_risk)
                    : "--"}
                </div>
              </div>
            </div>

            <div className="risk-panel__filters">
              <button
                className={`risk-panel__filter ${
                  riskListFilter === "all" ? "risk-panel__filter--active" : ""
                }`}
                onClick={() => setRiskListFilter("all")}
              >
                All Alerts
              </button>
              <button
                className={`risk-panel__filter ${
                  riskListFilter === "on_map"
                    ? "risk-panel__filter--active"
                    : ""
                }`}
                onClick={() => setRiskListFilter("on_map")}
              >
                On Map ({onMapDelayAlertCount})
              </button>
            </div>

            {delayAlertsError && (
              <div className="risk-panel__status risk-panel__status--error">
                {delayAlertsError}
              </div>
            )}

            {!delayAlertsError && visibleDelayAlerts.length === 0 && (
              <div className="risk-panel__status">
                No high-confidence alerts in this view.
              </div>
            )}

            {!delayAlertsError && visibleDelayAlerts.length > 0 && (
              <div className="risk-panel__list">
                {visibleDelayAlerts.map((alert) => (
                  <div
                    className="risk-alert"
                    key={`${alert.vehicle_id}-${alert.time}`}
                  >
                    <div className="risk-alert__score">
                      {formatPercent(alert.delay_increase_risk)}
                    </div>
                    <div className="risk-alert__main">
                      <div className="risk-alert__topline">
                        <div className="risk-alert__name">
                          {alert.line ?? alert.route_id ?? "unknown"} /{" "}
                          {alert.vehicle_id}
                        </div>
                        <div className="risk-alert__time">
                          {formatAlertTime(alert.time)}
                        </div>
                      </div>
                      <div className="risk-alert__meta">
                        {alert.state_position ?? "unknown"} | route{" "}
                        {alert.route_id ?? "--"}
                      </div>
                      <div className="risk-alert__chips">
                        <span className="risk-alert__chip risk-alert__chip--hot">
                          delta {formatSeconds(alert.target_delay_delta)}
                        </span>
                        <span className="risk-alert__chip">
                          delay {formatSeconds(alert.delay)}
                        </span>
                        <span className="risk-alert__chip">
                          speed{" "}
                          {typeof alert.speed === "number" &&
                          Number.isFinite(alert.speed)
                            ? Math.round(alert.speed)
                            : "--"}
                        </span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
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
          const delayAlert = delayAlertByVehicle.get(vehicle.id);
          const delayAlertHtml = delayAlert
            ? `
                <span style="color: #ffb3b3">
                  Risk: ${formatPercent(delayAlert.delay_increase_risk)}
                  (${formatSeconds(delayAlert.target_delay_delta)})
                </span><br/>
              `
            : "";

          return {
            html: `
              <div style="font-family: sans-serif; font-size: 12px; padding: 4px; color: #fff; background: #000;">
                <strong style="font-size: 14px">Line ${vehicle.line}</strong><br/>
                ${delayAlertHtml}
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
