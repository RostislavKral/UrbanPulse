import { useEffect, useRef, useState } from "react";
import { Vehicle } from "../types/vehicle";

/**
 * Maximum number of points to keep in the vehicle's history trail.
 */
const MAX_PATH_POINTS = 30;

/**
 * Maximum allowed distance jump between two updates in meters.
 * If a vehicle moves more than this, it's considered a GPS error or teleportation.
 */
const MAX_JUMP_METERS = 1500;

/**
 * Maximum realistic speed in meters per second (approx. 144 km/h).
 * Used to filter out GPS glitches.
 */
const MAX_SPEED_MPS = 40;

/**
 * TTL for stale vehicles in milliseconds (2 minutes).
 * Vehicles inactive for longer than this will be removed.
 */
const STALE_VEHICLE_TTL_MS = 2 * 60 * 1000;

/**
 * Geographic bounding box for Prague.
 * Updates outside this area are discarded to prevent invalid GPS data
 * (e.g., coordinates 0,0) from distorting the map.
 */
const PRAGUE_BOUNDS = {
  minLon: 14.2,
  maxLon: 14.75,
  minLat: 49.9,
  maxLat: 50.2,
};

/**
 * Converts degrees to radians.
 */
const toRadians = (value: number): number => (value * Math.PI) / 180;

/**
 * Calculates the great-circle distance between two points using the Haversine formula.
 * @param a - Tuple of [longitude, latitude] for the first point.
 * @param b - Tuple of [longitude, latitude] for the second point.
 * @returns Distance in meters.
 */
const distanceMeters = (a: [number, number], b: [number, number]): number => {
  const R = 6371000; // Earth radius in meters
  const lat1 = toRadians(a[1]);
  const lat2 = toRadians(b[1]);
  const deltaLat = toRadians(b[1] - a[1]);
  const deltaLon = toRadians(b[0] - a[0]);

  const sinLat = Math.sin(deltaLat / 2);
  const sinLon = Math.sin(deltaLon / 2);

  const h = sinLat * sinLat + Math.cos(lat1) * Math.cos(lat2) * sinLon * sinLon;

  return 2 * R * Math.asin(Math.min(1, Math.sqrt(h)));
};

interface UseLiveVehiclesResult {
  vehicles: Vehicle[];
  isConnected: boolean;
  startTime: number;
}

/**
 * Hook to manage real-time vehicle data via WebSocket.
 * Handles data validation, buffering, relative timing, and cleanup of stale data.
 * * @param wsUrl - The WebSocket server URL (realtime-gateway).
 * @returns Object containing the list of vehicles, connection status, and start time.
 */
export function useLiveVehicles(wsUrl: string): UseLiveVehiclesResult {
  const [vehicles, setVehicles] = useState<Record<string, Vehicle>>({});
  const [isConnected, setIsConnected] = useState<boolean>(false);
  const wsRef = useRef<WebSocket | null>(null);

  // Store the application start time to calculate relative timestamps.
  const startTimeRef = useRef<number>(Date.now());

  useEffect(() => {
    if (wsRef.current) return;

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    // Periodically remove vehicles that haven't updated recently.
    const pruneTimer = window.setInterval(() => {
      const nowRelative = Date.now() - startTimeRef.current;

      setVehicles((prev) => {
        const next: Record<string, Vehicle> = {};
        let changed = false;

        for (const [key, vehicle] of Object.entries(prev)) {
          const lastPathPoint = vehicle.path?.[vehicle.path.length - 1];
          const updatedAt = vehicle.updatedAt ?? lastPathPoint?.[2];

          // Keep the vehicle if it has updated within the allowed TTL window.
          if (
            typeof updatedAt === "number" &&
            nowRelative - updatedAt <= STALE_VEHICLE_TTL_MS
          ) {
            next[key] = vehicle;
          } else {
            changed = true;
          }
        }

        // Optimization: Return the previous state reference if no changes occurred
        // to prevent unnecessary re-renders.
        return changed ? next : prev;
      });
    }, 5000);

    ws.onopen = () => setIsConnected(true);
    ws.onclose = () => setIsConnected(false);

    ws.onmessage = (event: MessageEvent) => {
      try {
        const data = JSON.parse(event.data);
        const id = String(data.id ?? "");
        const lat = Number(data.lat);
        const lon = Number(data.lon);
        // TODO(mode): Validate/normalize mode fields coming from backend:
        // - route_type should be numeric when present
        // - mode should be a known VehicleMode value
        // Keep unknown values as "unknown" so rendering stays robust.

        // Validation
        if (!id || !Number.isFinite(lat) || !Number.isFinite(lon)) {
          return;
        }

        // Filter out coordinates outside of Prague bounds
        if (
          lat < PRAGUE_BOUNDS.minLat ||
          lat > PRAGUE_BOUNDS.maxLat ||
          lon < PRAGUE_BOUNDS.minLon ||
          lon > PRAGUE_BOUNDS.maxLon
        ) {
          return;
        }

        let relativeTime = Date.now() - startTimeRef.current;

        setVehicles((prev) => {
          const existing = prev[id];
          const oldPath = Array.isArray(existing?.path) ? existing.path : [];
          const lastPoint = oldPath[oldPath.length - 1];

          // Ensure that the data won't arrive out of order or too fast,
          // increment the timestamp slightly to prevent array sorting issues.
          if (lastPoint && relativeTime <= lastPoint[2]) {
            relativeTime = lastPoint[2] + 1;
          }

          let nextBasePath = oldPath;

          if (lastPoint) {
            const dist = distanceMeters(
              [lastPoint[0], lastPoint[1]],
              [lon, lat],
            );
            const deltaTimeMs = Math.max(relativeTime - lastPoint[2], 1);
            const speed = dist / (deltaTimeMs / 1000); // meters per second

            // If the vehicle jumped too far or moved impossibly fast, reset the path
            // to start a fresh trail, preventing "teleportation" lines across the map.
            if (dist > MAX_JUMP_METERS || speed > MAX_SPEED_MPS) {
              nextBasePath = [];
            } else if (dist < 1) {
              // If the vehicle moved less than 1 meter (GPS noise while stationary),
              // ignore the spatial update but update the timestamp to keep it alive.
              return {
                ...prev,
                [id]: { ...existing, ...data, updatedAt: relativeTime },
              };
            }
          }

          // Append new point and trim history to defined length
          const newPath = [...nextBasePath, [lon, lat, relativeTime]].slice(
            -MAX_PATH_POINTS,
          );

          return {
            ...prev,
            [id]: {
              ...data,
              id,
              lat,
              lon,
              updatedAt: relativeTime,
              path: newPath,
            },
          };
        });
      } catch (err) {
        console.error("Parse error:", err);
      }
    };

    return () => {
      window.clearInterval(pruneTimer);
      ws.close();
      wsRef.current = null;
    };
  }, [wsUrl]);

  return {
    vehicles: Object.values(vehicles),
    isConnected,
    startTime: startTimeRef.current,
  };
}
