import { useEffect, useRef, useState } from "react";
import { Vehicle } from "../types/vehicle";

const MAX_PATH_POINTS = 30;

const MAX_JUMP_METERS = 1500;

const MAX_SPEED_MPS = 40;

const STALE_VEHICLE_TTL_MS = 2 * 60 * 1000;

const PRAGUE_BOUNDS = {
  minLon: 14.2,
  maxLon: 14.75,
  minLat: 49.9,
  maxLat: 50.2,
};

const toRadians = (value: number): number => (value * Math.PI) / 180;

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

export function useLiveVehicles(
  wsUrl: string,
  enabled: boolean = true,
): UseLiveVehiclesResult {
  const [vehicles, setVehicles] = useState<Record<string, Vehicle>>({});
  const [isConnected, setIsConnected] = useState<boolean>(false);
  const wsRef = useRef<WebSocket | null>(null);

  const startTimeRef = useRef<number>(Date.now());

  useEffect(() => {
    if (!enabled) {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      setIsConnected(false);
      return;
    }

    if (wsRef.current) return;

    const ws = new WebSocket(wsUrl);
    wsRef.current = ws;

    const pruneTimer = window.setInterval(() => {
      const nowRelative = Date.now() - startTimeRef.current;

      setVehicles((prev) => {
        const next: Record<string, Vehicle> = {};
        let changed = false;

        for (const [key, vehicle] of Object.entries(prev)) {
          const lastPathPoint = vehicle.path?.[vehicle.path.length - 1];
          const updatedAt = vehicle.updatedAt ?? lastPathPoint?.[2];

          if (
            typeof updatedAt === "number" &&
            nowRelative - updatedAt <= STALE_VEHICLE_TTL_MS
          ) {
            next[key] = vehicle;
          } else {
            changed = true;
          }
        }

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
        if (!id || !Number.isFinite(lat) || !Number.isFinite(lon)) {
          return;
        }

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
            const speed = dist / (deltaTimeMs / 1000);

            if (dist > MAX_JUMP_METERS || speed > MAX_SPEED_MPS) {
              nextBasePath = [];
            } else if (dist < 1) {
              return {
                ...prev,
                [id]: { ...existing, ...data, updatedAt: relativeTime },
              };
            }
          }

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
  }, [wsUrl, enabled]);

  return {
    vehicles: Object.values(vehicles),
    isConnected,
    startTime: startTimeRef.current,
  };
}
