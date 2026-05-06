import os

import functions_framework
from flask import redirect, request, session, url_for

from app import app

ACCESS_KEY = os.environ.get("ACCESS_KEY")
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(32))

LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>WSJ Pro CyberIndex</title>
  <style>
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif;
      max-width: 480px;
      margin: 80px auto;
      padding: 0 20px;
      color: #111827;
      background: #F8F9FA;
      text-align: center;
    }
    .app-logo {
      font-size: 22px; font-weight: 700; letter-spacing: -0.3px;
      margin-bottom: 4px;
    }
    .app-logo span { color: #2563EB; }
    .subtitle { color: #6B7280; margin-bottom: 24px; font-size: 13px; }
    .form-card {
      background: #fff;
      border: 1px solid #DEE2E6;
      border-radius: 8px;
      padding: 24px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.08);
      text-align: left;
    }
    .credit { font-size: 10px; color: #9ca3af; text-align: right; margin-top: 6px; }
    input[type="password"] {
      display: block;
      box-sizing: border-box;
      width: 100%;
      padding: 10px 12px;
      font-size: 0.875rem;
      border: 1px solid #DEE2E6;
      border-radius: 6px;
      margin-bottom: 16px;
    }
    input[type="password"]:focus { outline: none; border-color: #2563EB; box-shadow: 0 0 0 3px rgba(37,99,235,0.1); }
    button {
      width: 100%;
      padding: 10px;
      font-size: 0.875rem;
      font-weight: 500;
      color: #fff;
      background: #2563EB;
      border: none;
      border-radius: 6px;
      cursor: pointer;
    }
    button:hover { background: #1d4ed8; }
    .error { color: #DC2626; font-size: 0.875rem; margin-top: 12px; }
  </style>
</head>
<body>
  <div class="app-logo">WSJ Pro <span>CyberIndex</span></div>
  <p class="subtitle">Top 20 cybersecurity companies by market cap.</p>
  <div style="max-width:320px;margin:0 auto;">
    <div class="form-card">
      <form method="POST" action="/login">
        <input type="password" id="access-key" name="access_key" placeholder="Enter access key" autofocus />
        <button type="submit">Continue</button>
      </form>
      {error}
    </div>
    <div class="credit">WSJ Pro Data</div>
  </div>
</body>
</html>"""


@app.before_request
def require_auth():
    if request.path.startswith("/static/"):
        return
    if request.endpoint == "login":
        return
    if not session.get("authenticated"):
        return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return LOGIN_PAGE.replace("{error}", ""), 200, {"Content-Type": "text/html"}

    key = request.form.get("access_key", "")
    if ACCESS_KEY and key == ACCESS_KEY:
        session["authenticated"] = True
        return redirect("/")

    error_html = '<p class="error">Incorrect access key.</p>'
    return LOGIN_PAGE.replace("{error}", error_html), 403, {"Content-Type": "text/html"}


@functions_framework.http
def cyberindex_entry_point(req):
    with app.request_context(req.environ):
        try:
            rv = app.full_dispatch_request()
        except Exception as e:
            rv = app.handle_exception(e)
        return rv


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
