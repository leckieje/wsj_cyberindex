# WSJ Pro Data — Cloud Function Module

This folder is a reusable template for deploying authenticated Google Cloud Functions. Each copy of this folder becomes one app with its own stable URL. The Cloud Function serves both the landing page (GET) and the app logic (POST) from a single endpoint — no external hosting required.

Access is controlled by a shared password (soft gate). No OAuth or GCP Credentials page needed.

---

## File Inventory

```
project-folder/
├── app.config                  ← EDIT THIS: set APP_NAME
├── deploy.sh                   ← DO NOT EDIT: deploys everything automatically
├── set-up-workflows.md         ← This file (instructions)
└── function_code/
    ├── index.js                ← DO NOT EDIT: Node.js landing page + auth gate
    ├── handler.js              ← EDIT THIS (Node.js): your app logic
    ├── package.json            ← DO NOT EDIT: Node.js dependencies
    ├── main.py                 ← DO NOT EDIT: Python landing page + auth gate
    ├── handler.py              ← EDIT THIS (Python): your app logic
    └── requirements.txt        ← DO NOT EDIT: Python dependencies
```

---

## Setup Steps for a New Project

1. Copy this entire folder to a new location.

2. Choose a language. Delete the files for the language you are NOT using:
   - For Node.js: delete `main.py`, `handler.py`, `requirements.txt`
   - For Python: delete `index.js`, `handler.js`, `package.json`

3. Edit `app.config`:
   ```bash
   APP_NAME="my-tool-name"
   ```
   The function will be named `wsj-pro-data-<APP_NAME>`.

4. Write your app logic in the handler file (see "Handler Contract" below).

5. Deploy: run `./deploy.sh` from the project root.

6. The script outputs the function URL. That URL is the stable link you share with teammates.

---

## How It Works (User Experience)

1. Teammate visits the function URL
2. They see a "WSJ Pro Data" landing page with a password field
3. They enter the access password and press Submit (or Enter)
4. The password is verified server-side — if correct, the handler response renders immediately
5. If incorrect, they see "Incorrect password."

---

## Access Control

Access is controlled by a shared password defined in `deploy.sh`:

```bash
ACCESS_KEY="WSJpro-data2026"
```

This is passed as an environment variable to the function at deploy time. The password is checked server-side — it is NOT visible in the page source. To change the password, edit the `ACCESS_KEY` value in `deploy.sh` and re-deploy.

---

## Runtime Auto-Detection

`deploy.sh` detects the language automatically:

- `function_code/package.json` exists → deploys as **nodejs22**, entry point `helloHttp`
- `function_code/requirements.txt` exists → deploys as **python312**, entry point `hello_http`
- Both exist → ERROR (ambiguous, remove one)
- Neither exists → ERROR

---

## Handler Contract

The handler file is the ONLY file you write app logic in. Everything else (landing page, password gate, deployment) is handled by the shell.

The handler receives the POST body. Note: the `_key` field is used for authentication and should be ignored in your handler logic.

### Node.js — `function_code/handler.js`

```javascript
function handle(req, res) {
  // req.body contains the POST JSON payload (ignore _key)
  // Use res.send() to return HTML, or res.json() for JSON
  res.send("<h2>Welcome!</h2><p>Your app content here.</p>");
}

module.exports = { handle };
```

Requirements:
- Export a `handle` function that accepts `(req, res)`
- `req` is an Express-style request object
- `res` is an Express-style response object
- Call `res.send()` with HTML to render a full app UI, or `res.json()` for data

### Python — `function_code/handler.py`

```python
from flask import jsonify

def handle(request):
    # request.get_json() contains the POST payload (ignore _key)
    # Return HTML string or use jsonify() for JSON
    return ("<h2>Welcome!</h2><p>Your app content here.</p>", 200, {"Content-Type": "text/html"})
```

Requirements:
- Export a `handle` function that accepts a Flask `request` object
- Return HTML as a string/tuple, or use `jsonify()` for JSON
- Import any additional packages and add them to `requirements.txt`

---

## Deployment Details

`deploy.sh` does the following automatically:

1. Reads `APP_NAME` from `app.config`
2. Detects runtime from `function_code/`
3. Deploys the function as `wsj-pro-data-<APP_NAME>` to project `dj-newsrm-stag-aiml`
4. Flags: `--gen2`, `--region=us-central1`, `--no-allow-unauthenticated`, `--no-invoker-iam-check`
5. Sets `ACCESS_KEY` as an environment variable on the function
6. Prints the function URL

Service account: `wsj-pro-data@dj-newsrm-stag-aiml.iam.gserviceaccount.com`

---

## Updating an App

Edit your handler file and/or any other logic, then run `./deploy.sh` again. Same URL, updated app.

---

## What NOT To Do

- DO NOT use Firebase (no firebase-functions, firebase-admin, Firebase Hosting, or Firebase CLI)
- DO NOT edit `index.js` or `main.py` — they are the shared landing page + auth gate
- DO NOT edit `deploy.sh` unless changing the shared password
- DO NOT put the access password in handler files or client-side code
- DO NOT include both `package.json` and `requirements.txt` — pick one language per project
- DO NOT change the entry point names (`helloHttp` for Node, `hello_http` for Python)
