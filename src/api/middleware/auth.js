const config = require("../../config/env");

function requireAuth(req, res, next) {
  const header = req.headers.authorization || "";
  const [scheme, token] = header.split(" ");

  if (scheme !== "Bearer" || token !== config.api.authToken) {
    return res.status(401).json({ error: "unauthorized" });
  }
  next();
}

module.exports = requireAuth;
