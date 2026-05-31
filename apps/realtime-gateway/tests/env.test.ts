import { describe, expect, it } from "vitest";
import { parseEnvContents } from "../src/env";

describe("parseEnvContents", () => {
  it("parses common .env syntax", () => {
    expect(
      parseEnvContents(`
        # ignored
        REDIS_HOST=redis
        export PORT=3000
        QUOTED="hello world"
        SINGLE_QUOTED='one two'
      `),
    ).toEqual({
      REDIS_HOST: "redis",
      PORT: "3000",
      QUOTED: "hello world",
      SINGLE_QUOTED: "one two",
    });
  });

  it("keeps values containing equals signs intact", () => {
    expect(parseEnvContents("DATABASE_URL=postgresql://u:p@host:5432/db?a=b")).toEqual(
      {
        DATABASE_URL: "postgresql://u:p@host:5432/db?a=b",
      },
    );
  });
});
