import Fastify from "fastify";
import cors from "@fastify/cors";
import Redis from "ioredis";
import fs from "node:fs";
import path from "node:path";
import { WebSocketServer, WebSocket } from "ws";

function loadRootEnv(): void {
  const startDirs = [process.cwd(), __dirname];

  for (const startDir of startDirs) {
    let current = startDir;
    for (let i = 0; i < 8; i++) {
      const candidate = path.join(current, ".env");
      if (fs.existsSync(candidate)) {
        const lines = fs.readFileSync(candidate, "utf8").split(/\r?\n/);
        for (const rawLine of lines) {
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
          if (key && process.env[key] === undefined) process.env[key] = value;
        }
        return;
      }

      const parent = path.dirname(current);
      if (parent === current) break;
      current = parent;
    }
  }
}

loadRootEnv();

const redisHost = process.env.REDIS_HOST ?? "127.0.0.1";
const redisPort = Number.parseInt(process.env.REDIS_PORT ?? "6379", 10);
const serverPort = Number.parseInt(process.env.PORT ?? "3000", 10);

const startServer = async () => {
  const app = Fastify({ logger: true });

  // Allow CORS
  await app.register(cors, { origin: "*" });

  app.get("/", async () => {
    return { status: "Urban Pulse Gateway is Running" };
  });

  // Redis
  const redis = new Redis({ host: redisHost, port: redisPort });
  const clients = new Set<WebSocket>();

  redis.on("error", (err) => {
    app.log.error({ err }, "Redis error");
  });

  redis.subscribe("urban_pulse:updates", (err) => {
    if (err) app.log.error("Redis sub failed");
    else app.log.info("Redis subscribed to urban_pulse:updates");
  });

  redis.on("message", (channel, message) => {
    // Send all clients
    for (const client of clients) {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    }
  });

  // Start HTTP server
  try {
    await app.listen({ port: serverPort, host: "0.0.0.0" });
    console.log(`HTTP Server running at http://0.0.0.0:${serverPort}`);
  } catch (err) {
    app.log.error(err);
    process.exit(1);
  }

  // Websocket server
  const wss = new WebSocketServer({ server: app.server, path: "/ws" });

  wss.on("connection", (socket) => {
    app.log.info("Client connected via WebSocket");
    clients.add(socket);

    socket.on("message", (msg) => {
      // Ignore, at least for now...
    });

    socket.on("close", () => {
      app.log.info("Client disconnected");
      clients.delete(socket);
    });

    socket.on("error", (e) => {
      app.log.error(e);
      clients.delete(socket);
    });
  });
};

startServer();
