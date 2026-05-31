export const PRAGUE_BOUNDS = {
  minLon: 14.2,
  maxLon: 14.75,
  minLat: 49.9,
  maxLat: 50.2,
};

const toRadians = (value: number): number => (value * Math.PI) / 180;

export const distanceMeters = (
  a: [number, number],
  b: [number, number],
): number => {
  const earthRadiusMeters = 6371000;
  const lat1 = toRadians(a[1]);
  const lat2 = toRadians(b[1]);
  const deltaLat = toRadians(b[1] - a[1]);
  const deltaLon = toRadians(b[0] - a[0]);

  const sinLat = Math.sin(deltaLat / 2);
  const sinLon = Math.sin(deltaLon / 2);

  const haversine =
    sinLat * sinLat + Math.cos(lat1) * Math.cos(lat2) * sinLon * sinLon;

  return 2 * earthRadiusMeters * Math.asin(Math.min(1, Math.sqrt(haversine)));
};

export const isWithinPragueBounds = (lon: number, lat: number): boolean =>
  lat >= PRAGUE_BOUNDS.minLat &&
  lat <= PRAGUE_BOUNDS.maxLat &&
  lon >= PRAGUE_BOUNDS.minLon &&
  lon <= PRAGUE_BOUNDS.maxLon;
