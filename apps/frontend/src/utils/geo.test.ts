import { describe, expect, it } from "vitest";
import { distanceMeters, isWithinPragueBounds } from "./geo";

describe("geo utilities", () => {
  it("calculates distance between two Prague landmarks", () => {
    const oldTownSquare: [number, number] = [14.4214, 50.0875];
    const charlesBridge: [number, number] = [14.4114, 50.0865];

    expect(distanceMeters(oldTownSquare, charlesBridge)).toBeCloseTo(722, -1);
  });

  it("accepts central Prague coordinates", () => {
    expect(isWithinPragueBounds(14.4378, 50.0755)).toBe(true);
  });

  it("rejects coordinates outside the configured Prague envelope", () => {
    expect(isWithinPragueBounds(16.6068, 49.1951)).toBe(false);
  });
});
