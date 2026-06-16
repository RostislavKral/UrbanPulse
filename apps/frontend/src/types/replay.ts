import { VehicleMode } from "./vehicle";

export type ViewMode = "live" | "replay";

export type ReplayMeta = {
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

export type ReplayBoundsMeta = {
  min_time: string | null;
  max_time: string | null;
  query_time_ms: number;
};

export type ReplayBoundsResponse = {
  meta: ReplayBoundsMeta;
};

export type ReplayPointState = "observed" | "interpolated" | "invalid_gap";

export type ReplayConfidence = "high" | "medium" | "low";

export type ReplayRow = {
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

export type ReplayFilters = {
  mode: VehicleMode | "all";
  lineQuery: string;
  includeInterpolated: boolean;
  hideVehiclesWithLatestInvalidGap: boolean;
};

export type ReplayStats = {
  observedPoints: number;
  interpolatedPoints: number;
  invalidGapPoints: number;
  shownVehicles: number;
};
