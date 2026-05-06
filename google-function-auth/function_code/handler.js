// --- Your app logic goes here ---
// This is the only file you change per project.
// It receives the authenticated request and sends the response.

function handle(req, res) {
  const name = req.body?.name || "World";
  res.json({ message: `Hello, ${name}!` });
}

module.exports = { handle };
