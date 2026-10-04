const CACHE = "cryptonita-shell-v2";
const isApi = request => new URL(request.url).pathname.startsWith("/api/crypto/");

self.addEventListener("install", event => {
  event.waitUntil(
    caches.open(CACHE).then(cache =>
      cache.addAll(["/", "/manifest.webmanifest"])
    )
  );
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

  // Never turn an API failure into cached application HTML. API responses are
  // always network-authoritative and are already marked no-store by the UI.
  if (isApi(request)) {
    event.respondWith(fetch(request));
    return;
  }

  event.respondWith(
    fetch(request).catch(() =>
      caches.match(request).then(r => r || caches.match("/"))
    )
  );
});
