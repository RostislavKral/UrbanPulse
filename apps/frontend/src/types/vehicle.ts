export type VehicleMode =
  | "tram"
  | "metro"
  | "rail"
  | "bus"
  | "ferry"
  | "trolleybus"
  | "unknown";

export interface Vehicle {
  id: string;

  lat: number;

  lon: number;

  line: string;

  delay: number;

  route_type?: number;

  mode?: VehicleMode;

  path: Array<[number, number, number]>;

  updatedAt?: number;
}

export interface RenderedVehicle extends Vehicle {
  renderPos: [number, number];

  isStale: boolean;
}
