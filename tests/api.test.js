jest.mock("../src/kafka/producer", () => ({
  connect: jest.fn(),
  disconnect: jest.fn(),
  sendEvents: jest.fn().mockResolvedValue({ success: true }),
}));

process.env.API_AUTH_TOKEN = "test-token";

const request = require("supertest");
const { createApp } = require("../src/api/server");
const producer = require("../src/kafka/producer");

const app = createApp();
const AUTH = { Authorization: "Bearer test-token" };

describe("POST /api/events", () => {
  beforeEach(() => producer.sendEvents.mockClear());

  test("rejects without auth token", async () => {
    const res = await request(app).post("/api/events").send({});
    expect(res.status).toBe(401);
  });

  test("accepts a single valid event", async () => {
    const res = await request(app)
      .post("/api/events")
      .set(AUTH)
      .send({ eventType: "x", payload: {}, sourceId: "s1" });

    expect(res.status).toBe(202);
    expect(res.body.accepted).toBe(1);
    expect(producer.sendEvents).toHaveBeenCalledTimes(1);
  });

  test("accepts a batch with partial validity", async () => {
    const res = await request(app)
      .post("/api/events")
      .set(AUTH)
      .send([
        { eventType: "x", payload: {}, sourceId: "s1" },
        { payload: {}, sourceId: "s2" }, // missing eventType -> rejected
      ]);

    expect(res.status).toBe(202);
    expect(res.body.accepted).toBe(1);
    expect(res.body.rejected).toBe(1);
  });

  test("rejects when all events in batch are invalid", async () => {
    const res = await request(app)
      .post("/api/events")
      .set(AUTH)
      .send([{ payload: {} }]);

    expect(res.status).toBe(400);
    expect(producer.sendEvents).not.toHaveBeenCalled();
  });
});
