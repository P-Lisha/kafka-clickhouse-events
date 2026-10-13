const { z } = require("zod");

const eventSchema = z.object({
  eventType: z.string().min(1),
  timestamp: z
    .string()
    .datetime({ offset: true })
    .optional(),
  payload: z.record(z.any()),
  sourceId: z.string().min(1),
});

// Validates one raw event and fills in server-side timestamp fallback.
// Returns { success: true, data } or { success: false, error }.
function validateEvent(raw) {
  const result = eventSchema.safeParse(raw);
  if (!result.success) {
    return { success: false, error: result.error.issues.map((i) => i.message).join("; ") };
  }
  const data = { ...result.data };
  if (!data.timestamp) {
    data.timestamp = new Date().toISOString();
  }
  return { success: true, data };
}

module.exports = { validateEvent };
