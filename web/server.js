import Fastify from "fastify";
import fastifyStatic from "@fastify/static";
import { fileURLToPath } from "url";
import { join, dirname } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 3000);
const HOST = process.env.HOST ?? "0.0.0.0";

const app = Fastify({ logger: true });

// Serve the Vite build output
app.register(fastifyStatic, {
    root: join(__dirname, "dist"),
    prefix: "/",
});

// SPA fallback — all unmatched routes return index.html
app.setNotFoundHandler((_req, reply) => {
    reply.sendFile("index.html");
});

app.listen({ port: PORT, host: HOST }, (err) => {
    if (err) {
        app.log.error(err);
        process.exit(1);
    }
});
