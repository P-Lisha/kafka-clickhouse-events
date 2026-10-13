const { validateEvent } = require("../src/api/validators/event");

describe("validateEvent", () => {
  test("accepts a valid event and defaults timestamp", () => {
    const result = validateEvent({
      eventType: "order.created",
      payload: { amount: 10 },
      sourceId: "svc-1",
    });
    expect(result.success).toBe(true);
    expect(result.data.timestamp).toBeDefined();
  });

  test("accepts a supplied ISO timestamp", () => {
    const result = validateEvent({
      eventType: "order.created",
      timestamp: "2026-01-01T00:00:00.000Z",
      payload: {},
      sourceId: "svc-1",
    });
    expect(result.success).toBe(true);
    expect(result.data.timestamp).toBe("2026-01-01T00:00:00.000Z");
  });

  test("rejects missing eventType", () => {
    const result = validateEvent({ payload: {}, sourceId: "svc-1" });
    expect(result.success).toBe(false);
  });

  test("rejects missing sourceId", () => {
    const result = validateEvent({ eventType: "x", payload: {} });
    expect(result.success).toBe(false);
  });

  test("rejects non-object payload", () => {
    const result = validateEvent({ eventType: "x", sourceId: "svc-1", payload: "not-an-object" });
    expect(result.success).toBe(false);
  });
});
