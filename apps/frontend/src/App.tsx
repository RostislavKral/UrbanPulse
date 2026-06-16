import { TripsLayer } from "@deck.gl/geo-layers";
import { ScatterplotLayer, IconLayer } from "@deck.gl/layers";
import { DeckGL } from "deck.gl";
import type { PickingInfo } from "deck.gl";
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
import { RiskPanel } from "./components/RiskPanel";
import { StatusPanel } from "./components/StatusPanel";
import { useLiveVehicles } from "./hooks/useLiveVehicles";
import {
  VEHICLE_ICON_ATLAS,
  VEHICLE_ICON_MAPPING,
  vehicleIconName,
  vehicleModeColor,
} from "./map/vehicleIcons";
import {
  type DelayAlertMeta,
  type DelayAlertRecord,
  type DelayAlertsResponse,
  type RiskListFilter,
} from "./types/alerts";
import {
  type ReplayBoundsResponse,
  type ReplayFilters,
  type ReplayMeta,
  type ReplayRow,
  type ReplayStats,
  type ViewMode,
} from "./types/replay";
import type { DataQualityResponse } from "./types/dataQuality";
import type { RenderedVehicle, Vehicle, VehicleMode } from "./types/vehicle";
import { isWithinPragueBounds } from "./utils/geo";

const WS_URL =
  import.meta.env.VITE_WS_URL?.toString() ?? "ws://127.0.0.1:3000/ws";

const API_URL = (
  import.meta.env.VITE_API_URL?.toString() ?? "http://127.0.0.1:8000"
).replace(/\/$/, "");

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

type MapViewState = typeof INITIAL_VIEW_STATE;

const isValidPoint = (point: unknown): point is [number, number, number] =>
  Array.isArray(point) && point.length >= 3;

interface TargetPositionResult {
  pos: [number, number];
  isStale: boolean;
}

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
const DELAY_ALERT_LIMIT = 1000;
const RISK_LIST_LIMIT = 30;
const DELAY_ALERT_REFRESH_MS = 60_000;
const DATA_QUALITY_REFRESH_MS = 30_000;

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

function displayLineFromReplayRow(row: ReplayRow): string {
  const routeId = row.route_id?.trim();
  if (routeId?.startsWith("L") && routeId.length > 1) {
    return routeId.slice(1);
  }
  return routeId || row.trip_id || "unknown";
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
    const line = displayLineFromReplayRow(last);
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
  const [mapViewState, setMapViewState] =
    useState<MapViewState>(INITIAL_VIEW_STATE);
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
  const [dataQuality, setDataQuality] = useState<DataQualityResponse | null>(null);
  const [isDataQualityLoading, setIsDataQualityLoading] = useState(false);
  const [dataQualityError, setDataQualityError] = useState<string | null>(null);
  const [isRiskPanelOpen, setIsRiskPanelOpen] = useState(true);
  const [riskListFilter, setRiskListFilter] = useState<RiskListFilter>("all");
  const [selectedVehicleId, setSelectedVehicleId] = useState<string | null>(null);
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
  const delayRiskByVehicle = useMemo(() => {
    return new Map(delayAlerts.map((alert) => [alert.vehicle_id, alert]));
  }, [delayAlerts]);
  const thresholdDelayAlertByVehicle = useMemo(() => {
    return new Map(
      delayAlerts
        .filter((alert) => alert.delay_increase_alert)
        .map((alert) => [alert.vehicle_id, alert]),
    );
  }, [delayAlerts]);
  const onMapDelayAlertCount = useMemo(() => {
    return delayAlerts.filter(
      (alert) =>
        alert.delay_increase_alert && activeVehicleIds.has(alert.vehicle_id),
    ).length;
  }, [activeVehicleIds, delayAlerts]);
  const visibleDelayAlerts = useMemo(() => {
    const filtered =
      riskListFilter === "on_map"
        ? delayAlerts.filter(
            (alert) =>
              alert.delay_increase_alert && activeVehicleIds.has(alert.vehicle_id),
          )
        : delayAlerts;
    return filtered.slice(0, RISK_LIST_LIMIT);
  }, [activeVehicleIds, delayAlerts, riskListFilter]);
  const topDelayAlert = delayAlerts[0] ?? null;
  const selectedDelayAlert = selectedVehicleId
    ? delayRiskByVehicle.get(selectedVehicleId) ?? null
    : null;
  const loadDataQuality = useCallback(async () => {
    setIsDataQualityLoading(true);
    setDataQualityError(null);

    try {
      const response = await fetch(`${API_URL}/data-quality`);
      if (!response.ok) {
        throw new Error(`Data quality fetch failed: ${response.status}`);
      }

      const payload = (await response.json()) as DataQualityResponse;
      setDataQuality(payload);
    } catch (error) {
      setDataQualityError(
        error instanceof Error ? error.message : "Data quality loading failed",
      );
    } finally {
      setIsDataQualityLoading(false);
    }
  }, []);

  const loadDelayAlerts = useCallback(async () => {
    setIsDelayAlertsLoading(true);
    setDelayAlertsError(null);

    try {
      const params = new URLSearchParams({
        limit: String(DELAY_ALERT_LIMIT),
        alerts_only: "false",
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
    setSelectedVehicleId(null);
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
    loadDataQuality();
    const intervalId = window.setInterval(
      loadDataQuality,
      DATA_QUALITY_REFRESH_MS,
    );

    return () => {
      window.clearInterval(intervalId);
    };
  }, [loadDataQuality]);

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

  const focusVehicle = useCallback(
    (vehicleId: string) => {
      const vehicle = renderedVehicles.find(
        (candidate) => candidate.id === vehicleId,
      );
      if (!vehicle) return;

      setSelectedVehicleId(vehicle.id);
      setMapViewState((current) => ({
        ...current,
        longitude: vehicle.renderPos[0],
        latitude: vehicle.renderPos[1],
        zoom: Math.max(current.zoom, 14),
      }));
    },
    [renderedVehicles],
  );

  useEffect(() => {
    if (!selectedVehicleId) return;
    if (renderedVehicles.some((vehicle) => vehicle.id === selectedVehicleId)) {
      return;
    }
    setSelectedVehicleId(null);
  }, [renderedVehicles, selectedVehicleId]);

  const activeRenderedCount = useMemo(
    () => renderedVehicles.filter((vehicle) => !vehicle.isStale).length,
    [renderedVehicles],
  );
  const waitingRenderedCount = renderedVehicles.length - activeRenderedCount;
  const mapNotice = useMemo(() => {
    if (viewMode === "live") {
      if (!isConnected) return "Live stream is disconnected.";
      if (renderedVehicles.length === 0) return "Live stream is waiting for vehicles.";
      return null;
    }

    if (isReplayLoading) return "Replay is loading vehicle history.";
    if (replayError) return `Replay error: ${replayError}`;
    if (replayMeta && replayStats.shownVehicles === 0) {
      return "Replay has no visible vehicles for the current window.";
    }
    return null;
  }, [
    isConnected,
    isReplayLoading,
    renderedVehicles.length,
    replayError,
    replayMeta,
    replayStats.shownVehicles,
    viewMode,
  ]);
  const mapNoticeTone = replayError || (viewMode === "live" && !isConnected)
    ? "error"
    : "muted";

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
        if (thresholdDelayAlertByVehicle.has(d.id)) return [255, 82, 82];
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
      id: "vehicles-background",
      data: renderedVehicles,
      getPosition: (d: RenderedVehicle) => d.renderPos,

      getRadius: (d: RenderedVehicle) => (d.id === selectedVehicleId ? 22 : 16),
      radiusUnits: "meters",
      radiusMinPixels: 12,
      radiusMaxPixels: 30,

      stroked: true,
      getLineWidth: 2,
      getFillColor: (d: RenderedVehicle) => {
        if (thresholdDelayAlertByVehicle.has(d.id)) return [255, 82, 82, 255];

        const [r, g, b] = vehicleModeColor(d.mode);
        return [r, g, b, 235];
      },
      pickable: true,
      onClick: ({ object }: PickingInfo) => {
        const vehicle = object as RenderedVehicle | null | undefined;
        if (!vehicle) return false;
        setSelectedVehicleId(vehicle.id);
        return true;
      },

      getLineColor: (d: RenderedVehicle) => {
        if (thresholdDelayAlertByVehicle.has(d.id)) return [255, 213, 79];
        if (d.delay < 60) return [46, 204, 113];
        if (d.delay < 180) return [243, 156, 18];
        return [231, 76, 60];
      },

      parameters: { depthTest: false },
      updateTriggers: {
        getRadius: [selectedVehicleId],
        getFillColor: [thresholdDelayAlertByVehicle],
        getLineColor: [playbackTime, thresholdDelayAlertByVehicle, selectedVehicleId],
      },
    }),

    new IconLayer({
      id: "vehicles-type-icon",
      data: renderedVehicles,
      getPosition: (d: RenderedVehicle) => d.renderPos,
      iconAtlas: VEHICLE_ICON_ATLAS,
      iconMapping: VEHICLE_ICON_MAPPING,
      getIcon: (d: RenderedVehicle) => vehicleIconName(d.mode),
      getColor: () => [255, 255, 255],
      getSize: (d: RenderedVehicle) => (d.id === selectedVehicleId ? 32 : 24),
      sizeUnits: "pixels",
      pickable: false,
      parameters: { depthTest: false },
      updateTriggers: {
        getSize: [selectedVehicleId],
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
      <StatusPanel
        viewMode={viewMode}
        isConnected={isConnected}
        activeCount={activeRenderedCount}
        waitingCount={waitingRenderedCount}
        isReplayLoading={isReplayLoading}
        isReplayPlaying={isReplayPlaying}
        replaySpeed={replaySpeed}
        replayWindowDraftMinutes={replayWindowDraftMinutes}
        replayWindowDirty={replayWindowDirty}
        replayWindowOptions={REPLAY_WINDOW_MINUTES}
        replayFilters={replayFilters}
        replayModes={REPLAY_MODES}
        replayMeta={replayMeta}
        replayProgressPct={replayProgressPct}
        activeReplayWindowMinutes={activeReplayWindowMinutes}
        playbackTime={playbackTime}
        replayDurationMs={replayDurationMs}
        replayStats={replayStats}
        replayError={replayError}
        dataQuality={dataQuality}
        dataQualityError={dataQualityError}
        isDataQualityLoading={isDataQualityLoading}
        playbackDelaySeconds={PLAYBACK_DELAY_MS / 1000}
        trailLengthSeconds={TRAIL_LENGTH_MS / 1000}
        onLiveClick={() => {
          replayLoadIdRef.current += 1;
          setIsReplayLoading(false);
          setIsReplayPlaying(false);
          setReplayElapsedMs(0);
          setSelectedVehicleId(null);
          lastAnimationTimeRef.current = null;
          setViewMode("live");
        }}
        onReplayClick={() => loadReplay()}
        onApplyReplayWindow={() => loadReplay()}
        onReplayWindowChange={setReplayWindowDraftMinutes}
        onToggleReplayPlaying={() => setIsReplayPlaying((prev) => !prev)}
        onReplayReset={() => {
          setReplayElapsedMs(0);
          setSelectedVehicleId(null);
          visualPositionsRef.current = {};
        }}
        onReplaySpeedChange={setReplaySpeed}
        onReplayFiltersChange={setReplayFilters}
      />

      <RiskPanel
        isOpen={isRiskPanelOpen}
        alerts={delayAlerts}
        visibleAlerts={visibleDelayAlerts}
        meta={delayAlertMeta}
        topAlert={topDelayAlert}
        onMapCount={onMapDelayAlertCount}
        activeVehicleIds={activeVehicleIds}
        selectedVehicleId={selectedVehicleId}
        selectedAlert={selectedDelayAlert}
        filter={riskListFilter}
        isLoading={isDelayAlertsLoading}
        error={delayAlertsError}
        formatPercent={formatPercent}
        formatSeconds={formatSeconds}
        formatAlertTime={formatAlertTime}
        onRefresh={loadDelayAlerts}
        onToggleOpen={() => setIsRiskPanelOpen((prev) => !prev)}
        onFilterChange={setRiskListFilter}
        onSelectAlert={(alert) => focusVehicle(alert.vehicle_id)}
        onClearSelection={() => setSelectedVehicleId(null)}
      />

      {mapNotice && (
        <div className={`map-notice map-notice--${mapNoticeTone}`}>
          {mapNotice}
        </div>
      )}

      <DeckGL
        viewState={mapViewState}
        onViewStateChange={({ viewState }) => {
          setMapViewState((current) => ({
            ...current,
            longitude:
              typeof viewState.longitude === "number"
                ? viewState.longitude
                : current.longitude,
            latitude:
              typeof viewState.latitude === "number"
                ? viewState.latitude
                : current.latitude,
            zoom:
              typeof viewState.zoom === "number" ? viewState.zoom : current.zoom,
            pitch:
              typeof viewState.pitch === "number"
                ? viewState.pitch
                : current.pitch,
            bearing:
              typeof viewState.bearing === "number"
                ? viewState.bearing
                : current.bearing,
          }));
        }}
        controller={true}
        layers={layers}
        getTooltip={({ object }: PickingInfo) => {
          const vehicle = object as RenderedVehicle | null | undefined;
          if (!vehicle) return null;

          const realNow =
            viewMode === "live" ? Date.now() - startTime : playbackTime;
          const lastTimestamp = vehicle.path[vehicle.path.length - 1][2];
          const secondsAgo = (realNow - lastTimestamp) / 1000;
          const delayAlert = delayRiskByVehicle.get(vehicle.id);
          const delayAlertHtml = delayAlert
            ? `
                <span style="color: #ffb3b3">
                  Risk: ${formatPercent(delayAlert.delay_increase_risk)}
                </span><br/>
              `
            : "";

          return {
            html: `
              <div style="font-family: sans-serif; font-size: 12px; padding: 4px; color: #fff; background: #000;">
                <strong style="font-size: 14px">Line ${vehicle.line}</strong><br/>
                ${delayAlertHtml}
                <span style="color: ${vehicle.delay > 180 ? "#e74c3c" : "#2ecc71"}">
                  Delay vs schedule: ${formatSeconds(vehicle.delay)}
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
