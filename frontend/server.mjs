import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("./dist", import.meta.url));
const port = Number(process.env.PORT || 8080);
const apiBase = (
  process.env.CRYPTO_API_BASE_URL ||
  "https://gorila-crypto-cleanroom-binance-capture.onrender.com"
).replace(/\/$/, "");

const mime = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".webmanifest": "application/manifest+json; charset=utf-8",
  ".ico": "image/x-icon",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
};

function sendText(res, status, body, type = "text/plain; charset=utf-8") {
  res.writeHead(status, {
    "Content-Type": type,
    "Cache-Control": "no-store",
  });
  res.end(body);
}

async function proxyApi(req, res) {
  const target = apiBase + req.url;
  try {
    const upstream = await fetch(target, {
      method: req.method,
      headers: {
        accept: req.headers.accept || "application/json",
      },
    });
    const body = await upstream.arrayBuffer();
    res.writeHead(upstream.status, {
      "Content-Type": upstream.headers.get("content-type") || "application/json",
      "Cache-Control": "no-store",
    });
    res.end(Buffer.from(body));
  } catch (error) {
    sendText(
      res,
      502,
      JSON.stringify({
        detail: "crypto_backend_unavailable",
        error: String(error?.message || error),
      }),
      "application/json",
    );
  }
}

async function serveStatic(req, res) {
  let pathname = new URL(req.url || "/", "http://localhost").pathname;
  if (pathname === "/") pathname = "/index.html";

  const safe = normalize(pathname).replace(/^([.][.][\\/])+/, "");
  const filePath = join(root, safe);
  try {
    const info = await stat(filePath);
    if (!info.isFile()) throw new Error("not a file");
    res.writeHead(200, {
      "Content-Type": mime[extname(filePath)] || "application/octet-stream",
      "Cache-Control": pathname.startsWith("/assets/") ? "public, max-age=31536000, immutable" : "no-cache",
    });
    createReadStream(filePath).pipe(res);
  } catch {
    const indexPath = join(root, "index.html");
    try {
      const html = await readFile(indexPath);
      res.writeHead(200, {
        "Content-Type": mime[".html"],
        "Cache-Control": "no-cache",
      });
      res.end(html);
    } catch {
      sendText(res, 500, "Cryptonita frontend build is unavailable");
    }
  }
}

const server = createServer(async (req, res) => {
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET,HEAD,OPTIONS",
      "Access-Control-Allow-Headers": "*",
    });
    return res.end();
  }

  if ((req.url || "").startsWith("/api/crypto/")) {
    return proxyApi(req, res);
  }

  if (req.url === "/healthz") {
    return sendText(res, 200, JSON.stringify({
      service: "cryptonita-terminal",
      status: "live",
      api_base: apiBase,
    }), "application/json");
  }

  return serveStatic(req, res);
});

server.listen(port, "0.0.0.0", () => {
  console.log(`CRYPTONITA_GATEWAY_LISTENING port=${port} api=${apiBase}`);
});
