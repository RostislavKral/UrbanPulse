import Fastify from "fastify";
import cors from "@fastify/cors";
import Redis from "ioredis";
import { WebSocketServer, WebSocket } from "ws";

const redisHost = process.env.REDIS_HOST ?? "localhost";
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