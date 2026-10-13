const express = require("express");
const { validateEvent } = require("../validators/event");
const producer = require("../../kafka/producer");
const metrics = require("../../metrics/prometheus");

const router = express.Router();

router.post("/", async (req, res) => {
  const body = req.body;
  const isBatch = Array.isArray(body);
  const rawEvents = isBatch ? body : [body];

  if (rawEvents.length === 0) {
    return res.status(400).json({ error: "no events provided" });
  }

  const results = rawEvents.map((raw) => validateEvent(raw));
  const valid = [];
  const statuses = results.map((result) => {
    if (result.success) {
      valid.push(result.data);
      return { status: "accepted" };
    }
    metrics.eventsInvalidPayloadTotal.inc();
    return { status: "rejected", error: result.error };
  });

  if (valid.length > 0) {
    const sendResult = await producer.sendEvents(valid);
    if (!sendResult.success) {
      return res.status(502).json({ error: "failed to enqueue events", details: sendResult.error });
    }
  }

  const acceptedCount = valid.length;
  const rejectedCount = statuses.length - acceptedCount;

  return res.status(acceptedCount > 0 ? 202 : 400).json({
    accepted: acceptedCount,
    rejected: rejectedCount,
    results: isBatch ? statuses : statuses[0],
  });
});

module.exports = router;
