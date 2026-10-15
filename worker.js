// Serves the static site, adding HTTP Range support for the videos under /media/.
// Workers static assets answer a Range request with the whole file (200), and Safari will not play an MP4 without
// 206 Partial Content, so ranged video requests are answered here by streaming the requested slice of the asset.
// Every other request goes straight to the assets.
// The asset fetch carries no Content-Length, so file sizes come from media-sizes.json, which the media build
// (horizon-of-steel/.tmp/marketing/site_media.py) rewrites with the files.

import SIZES from "./media-sizes.json";

const RANGE = /^bytes=(\d*)-(\d*)$/;

function log(op, data) {
  console.log(JSON.stringify({ component: "site-worker", op, ...data }));
}

/** Streams bytes [start, end] of a body, reading past the skipped prefix and cancelling after the end. */
function slice(body, start, end) {
  const reader = body.getReader();
  let offset = 0;
  return new ReadableStream({
    async pull(controller) {
      while (true) {
        const { done, value } = await reader.read();
        if (done) { controller.close(); return; }
        const chunkStart = offset;
        offset += value.byteLength;
        if (offset <= start) continue;
        const from = Math.max(0, start - chunkStart);
        const to = Math.min(value.byteLength, end + 1 - chunkStart);
        if (to > from) controller.enqueue(value.subarray(from, to));
        if (offset > end) { controller.close(); await reader.cancel(); }
        return;
      }
    },
    cancel(reason) { return reader.cancel(reason); },
  });
}

export default {
  async fetch(request, env) {
    const range = request.headers.get("Range");
    if (!range) return env.ASSETS.fetch(request);

    const headers = new Headers(request.headers);
    headers.delete("Range");
    const asset = await env.ASSETS.fetch(new Request(request.url, { method: request.method, headers }));
    if (asset.status !== 200 || !asset.body) return asset;

    const path = new URL(request.url).pathname;
    const size = Number(asset.headers.get("Content-Length")) || SIZES[path] || 0;
    const match = RANGE.exec(range.trim());
    if (!Number.isFinite(size) || size <= 0 || !match || (match[1] === "" && match[2] === "")) {
      log("range-ignored", { path, range, size });
      return asset;
    }

    let start;
    let end;
    if (match[1] === "") {
      // Suffix form: the last N bytes.
      start = Math.max(0, size - Number(match[2]));
      end = size - 1;
    } else {
      start = Number(match[1]);
      end = match[2] === "" ? size - 1 : Math.min(Number(match[2]), size - 1);
    }
    if (start >= size || start > end) {
      await asset.body.cancel();
      log("range-unsatisfiable", { path, range, size });
      return new Response(null, { status: 416, headers: { "Content-Range": `bytes */${size}` } });
    }

    const out = new Headers(asset.headers);
    out.set("Content-Range", `bytes ${start}-${end}/${size}`);
    out.set("Content-Length", String(end - start + 1));
    out.set("Accept-Ranges", "bytes");
    // FixedLengthStream keeps Content-Length on the streamed response, which the runtime would otherwise drop.
    const body = request.method === "HEAD" ? null : slice(asset.body, start, end).pipeThrough(new FixedLengthStream(end - start + 1));
    if (body === null) await asset.body.cancel();
    return new Response(body, { status: 206, headers: out });
  },
};
