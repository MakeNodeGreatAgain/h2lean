// Buffer.from(view) against Buffer.allocUnsafe(len) + view.copy(): the two ways to get
// a standalone copy of a slice. Buffer.from(view) always allocates its backing store
// standalone; allocUnsafe under Buffer.poolSize / 2 takes it from the shared pool, so
// for the small sizes that frame payloads have the copy through allocUnsafe wins.
// Measured on node v26.12.1 (5e6 iterations, medians of 3 runs): 9 B 92.9 -> 53.8 ns,
// 36 B 84.6 -> 58.4 ns, 180 B 87.4 -> 64.4 ns. Used at the cached block copy
// (lib/headers.js), the PING payload and the GOAWAY debug data (lib/session.js,
// lib/client-session.js) and the direct WINDOW_UPDATE write (lib/client-session.js).
const N = 5e6;
for (const len of [9, 36, 180]) {
  const src = Buffer.allocUnsafe(1024);
  // warmup
  for (let i = 0; i < 1e5; i++) {
    const b = Buffer.from(src.subarray(0, len)); b[0] = 1;
  }
  for (let i = 0; i < 1e5; i++) {
    const b = Buffer.allocUnsafe(len); src.copy(b, 0, 0, len); b[0] = 1;
  }
  let t = process.hrtime.bigint();
  for (let i = 0; i < N; i++) {
    const b = Buffer.from(src.subarray(0, len)); b[0] = 1;
  }
  const fromNs = Number(process.hrtime.bigint() - t) / N;
  t = process.hrtime.bigint();
  for (let i = 0; i < N; i++) {
    const b = Buffer.allocUnsafe(len); src.copy(b, 0, 0, len); b[0] = 1;
  }
  const copyNs = Number(process.hrtime.bigint() - t) / N;
  console.log(`len=${String(len).padStart(4)}  Buffer.from(subarray): ${fromNs.toFixed(1)} ns   allocUnsafe+copy: ${copyNs.toFixed(1)} ns`);
}
