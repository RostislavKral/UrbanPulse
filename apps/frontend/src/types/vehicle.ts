/**
 * Represents the state of a single public transport vehicle in the visualization.
 */
// TODO(mode): Add/adjust variants as you decide to support.
export type VehicleMode =
  | "tram"
  | "metro"
  | "rail"
  | "bus"
  | "ferry"
  | "trolleybus"
  | "unknown";

export interface Vehicle {
  /** Unique identifier provided by the source API (Golemio). */
  id: string;

  /** Current latitude (WGS84). */
  lat: number;

  /** Current longitude (WGS84). */
  lon: number;

  /** Route designation (e.g., "22", "A", "S1"). */
  line: string;

  /** Current delay in seconds relative to the schedule. Positive values indicate late arrival. */
  delay: number;

  /** TODO(mode): Set from backend payload once route_type mapping is implemented in data-service. */
  route_type?: number;

  /** TODO(mode): Normalized transport mode derived from GTFS route_type. */
  mode?: VehicleMode;

  /** * Historical trajectory of the vehicle used for rendering trails.
   * Format: [longitude, latitude, relativeTimestamp]
   */
  path: Array<[number, number, number]>;

  /** * Timestamp of the last received data update, relative to the application start time.
   * Used to identify and prune stale vehicles.
   */
  updatedAt?: number;
}

/**
 * Represents a vehicle processed for rendering on the map.
 * Extends the raw Vehicle with calculated UI properties (smooth position, state).
 */
export interface RenderedVehicle extends Vehicle {
  /** The calculated smooth position for the current animation frame [lon, lat] */
  renderPos: [number, number];

  /** Indicates if the vehicle is waiting for new data (stuck at the end of path) */
  isStale: boolean;
}
