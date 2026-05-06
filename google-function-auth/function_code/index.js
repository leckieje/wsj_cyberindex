const functions = require("@google-cloud/functions-framework");
const { handle } = require("./handler");

const ACCESS_KEY = process.env.ACCESS_KEY || "WSJpro-data2026";

functions.http("helloHttp", (req, res) => {
  if (req.method === "OPTIONS") {
    res.status(204).send("");
    return;
  }

  if (req.method === "GET") {
    res.set("Content-Type", "text/html");
    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>WSJ Pro Data</title>
  <style>
    body {
      font-family: system-ui, sans-serif;
      max-width: 600px;
      margin: 60px auto;
      padding: 0 20px;
      color: #222;
    }
    h1 { margin-bottom: 0.25em; }
    p { color: #555; }
    #login-form {
      margin-top: 20px;
    }
    #password-input {
      padding: 10px;
      font-size: 1rem;
      width: 250px;
      border: 1px solid #ccc;
      border-radius: 4px;
    }
    #submit-btn {
      padding: 10px 20px;
      font-size: 1rem;
      cursor: pointer;
      margin-left: 8px;
    }
    #status {
      margin-top: 12px;
      color: #c00;
    }
    #app {
      margin-top: 20px;
    }
  </style>
</head>
<body>
  <h1>WSJ Pro Data</h1>
  <p>Enter the access password to continue.</p>

  <div id="login-form">
    <input type="password" id="password-input" placeholder="Enter Password" />
    <button id="submit-btn" onclick="submitPassword()">Submit</button>
  </div>
  <p id="status"></p>
  <div id="app"></div>

  <script>
    document.getElementById("password-input").addEventListener("keydown", function(e) {
      if (e.key === "Enter") submitPassword();
    });

    async function submitPassword() {
      const password = document.getElementById("password-input").value;
      const status = document.getElementById("status");
      status.textContent = "Loading...";
      status.style.color = "#555";

      try {
        const resp = await fetch(window.location.href, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ _key: password })
        });
        const text = await resp.text();
        if (resp.ok) {
          document.getElementById("login-form").style.display = "none";
          document.querySelector("p").style.display = "none";
          status.style.display = "none";
          document.getElementById("app").innerHTML = text;
        } else {
          status.style.color = "#c00";
          status.textContent = resp.status === 403 ? "Incorrect password." : "Error: " + text;
        }
      } catch (err) {
        status.style.color = "#c00";
        status.textContent = "Request failed: " + err.message;
      }
    }
  </script>
</body>
</html>`);
    return;
  }

  const body = req.body || {};
  if (body._key !== ACCESS_KEY) {
    res.status(403).send("Access denied.");
    return;
  }

  handle(req, res);
});
