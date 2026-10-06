const CACHE = "cryptonita-shell-v3";
const isApi = request => new URL(request.url).pathname.startsWith("/api/crypto/");

self.addEventListener("install", event => {
  // Do not precache "/" here. A stale HTML shell can otherwise survive a
  // deploy/restart and make the terminal appear frozen even when the API is live.
  event.waitUntil(caches.open(CACHE).then(cache =>
    cache.addAll(["/manifest.webmanifest"])
  ));
  self.skipWaiting();
});

self.addEventListener("activate", event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE)
          .map(key => caches.delete(key))
      )
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", event => {
  const request = event.request;
  if (request.method !== "GET") return;

  // API/read-model responses are always network-authoritative.
  if (isApi(request)) {
    event.respondWith(fetch(request, { cache: "no-store" }));
    return;
  }

  // HTML must prefer the deployed shell. Fall back to cache only when offline.
  const acceptsHtml = request.mode === "navigate" ||
    request.headers.get("accept")?.includes("text/html");
  if (acceptsHtml) {
    event.respondWith(
      fetch(request, { cache: "no-store" }).catch(() =>
        caches.match(request).then(r => r || caches.match("/manifest.webmanifest"))
      )
    );
    return;
  }

  event.respondWith(
    fetch(request).catch(() => caches.match(request))
  );
});

self.addEventListener("message", event => {
  if (event.data === "SKIP_WAITING") self.skipWaiting();
});
