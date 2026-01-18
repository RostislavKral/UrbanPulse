import { useEffect, useMemo, useState } from "react";
import { DeckGL, ScatterplotLayer } from "deck.gl";
import Map from "react-map-gl/maplibre";

type Vehicle = {
  id: string;
  lat: number;
  lon: number;
  line: string;
  delay: number;
};

const INITIAL_VIEW_STATE = {
  longitude: 14.4378,
  latitude: 50.0755,
  zoom: 12,
  pitch: 0,
  bearing: 0
};

const MAP_STYLE =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";
const WS_URL = "ws://127.0.0.1:3000/ws";

const isVehicleMessage = (value: unknown): value is Vehicle => {
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    typeof record.id === "string" &&
    typeof record.lat === "number" &&
    typeof record.lon === "number" &&
    typeof record.line === "string" &&
    typeof record.delay === "number"
  );
};

export default function App() {
  const [vehicles, setVehicles] = useState<Record<string, Vehicle>>({});

  useEffect(() => {
    const socket = new WebSocket(WS_URL);

    socket.addEventListener("message", (event) => {
      let payload: unknown;
      try {
        payload = JSON.parse(event.data as string);
      } catch {
        return;
      }

      if (!isVehicleMessage(payload)) {
        return;
      }

      setVehicles((current) => ({ ...current, [payload.id]: payload }));
    });

    return () => {
      socket.close();
    };
  }, []);

  const layers = useMemo(() => {
    const data = Object.values(vehicles);
    return [
      new ScatterplotLayer<Vehicle>({
        id: "vehicles",
        data,
        getPosition: (vehicle) => [vehicle.lon, vehicle.lat],
        getRadius: 7,
        radiusUnits: "meters",
        getFillColor: (vehicle) =>
          vehicle.delay > 180 ? [255, 0, 0] : [0, 255, 0],
        pickable: true
      })
    ];
  }, [vehicles]);

  return (
    <DeckGL
      initialViewState={INITIAL_VIEW_STATE}
      controller={true}
      layers={layers}
      getTooltip={({ object }) =>
        object ? `Line: ${object.line} | Delay: ${object.delay} sec` : null
      }
      style={{ width: "100%", height: "100%" }}
    >
      <Map mapStyle={MAP_STYLE} />
    </DeckGL>
  );
}
