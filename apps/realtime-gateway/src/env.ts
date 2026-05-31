import fs from "node:fs";
import path from "node:path";

export function parseEnvContents(contents: string): Record<string, string> {
  const values: Record<string, string> = {};

  for (const rawLine of contents.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;

    const normalized = line.startsWith("export ")
      ? line.slice("export ".length).trim()
      : line;
    const eqIndex = normalized.indexOf("=");
    if (eqIndex === -1) continue;

    const key = normalized.slice(0, eqIndex).trim();
    const value = normalized
      .slice(eqIndex + 1)
      .trim()
      .replace(/^['"]|['"]$/g, "");

    if (key) values[key] = value;
  }

  return values;
}

export function loadRootEnv(
  startDirs: string[] = [process.cwd(), __dirname],
  maxDepth = 8,
): void {
  for (const startDir of startDirs) {
    let current = startDir;
    for (let i = 0; i < maxDepth; i++) {
      const candidate = path.join(current, ".env");
      if (fs.existsSync(candidate)) {
        const values = parseEnvContents(fs.readFileSync(candidate, "utf8"));
        for (const [key, value] of Object.entries(values)) {
          if (process.env[key] === undefined) process.env[key] = value;
        }
        return;
      }

      const parent = path.dirname(current);
      if (parent === current) break;
      current = parent;
    }
  }
}
