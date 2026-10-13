// Simple throughput load test against POST /api/events.
// Usage: TARGET_RPS=200 DURATION_SEC=30 node scripts/load-test.js
const http = require("http");
const config = require("../src/config/env");

const TARGET_RPS = parseInt(process.env.TARGET_RPS || "100", 10);
const DURATION_SEC = parseInt(process.env.DURATION_SEC || "30", 10);

let sent = 0;
let ok = 0;
let failed = 0;

function sampleEvent(i) {
  return {
    eventType: "load_test.ping",
    payload: { i },
    sourceId: `load-test-${i % 50}`,
  };
}

function sendOne(i) {
  const body = JSON.stringify(sampleEvent(i));
  const req = http.request(
    {
      hostname: "localhost",
      port: config.api.port,
      path: "/api/events",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(body),
        Authorization: `Bearer ${config.api.authToken}`,
      },
    },
    (res) => {
      res.on("data", () => {});
      res.on("end", () => {
        if (res.statusCode === 202) ok += 1;
        else failed += 1;
      });
    }
  );
  req.on("error", () => {
    failed += 1;
  });
  req.write(body);
  req.end();
}

function run() {
  const intervalMs = 1000 / TARGET_RPS;
  let i = 0;
  const ticker = setInterval(() => {
    sendOne(i);
    sent += 1;
    i += 1;
  }, intervalMs);

  setTimeout(() => {
    clearInterval(ticker);
    setTimeout(() => {
      console.log(`Sent: ${sent}, OK: ${ok}, Failed: ${failed}, Target RPS: ${TARGET_RPS}`);
      process.exit(0);
    }, 1000);
  }, DURATION_SEC * 1000);
}

run();
