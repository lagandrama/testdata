from dotenv import load_dotenv, find_dotenv
from pathlib import Path
import os, time, json
import requests
from flask import Flask, request, redirect
from urllib.parse import urlencode

# 1) .env loader — traži .env od root-a projekta prema gore i prepiši env varijable ako postoje
load_dotenv(find_dotenv(), override=True)

# (dodatno: ako želiš 100% deterministički, možeš ovako)
# ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
# load_dotenv(ENV_PATH, override=True)

CLIENT_ID = os.getenv("OURA_CLIENT_ID")
CLIENT_SECRET = os.getenv("OURA_CLIENT_SECRET")
REDIRECT_URI = os.getenv("OURA_REDIRECT_URI", "http://localhost:8000/callback")
SCOPES = "daily personal email"

if not CLIENT_ID or not CLIENT_SECRET:
    raise SystemExit("OURA_CLIENT_ID / OURA_CLIENT_SECRET nisu postavljeni (provjeri .env).")


TOKENS_PATH = Path(__file__).parent / "oura_tokens.json"

app = Flask(__name__)

@app.route("/")
def index():
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": "x",  # po želji randomiziraj i validiraj
    }
    url = "https://cloud.ouraring.com/oauth/authorize?" + urlencode(params)
    return redirect(url)

@app.route("/callback")
def callback():
    if request.args.get("error"):
        return f"Error: {request.args['error']}"
    code = request.args.get("code")

    resp = requests.post(
        "https://api.ouraring.com/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=30,
    )
    resp.raise_for_status()
    tokens = resp.json()
    tokens["created_at"] = int(time.time())
    tokens["expires_at"] = tokens["created_at"] + int(tokens.get("expires_in", 3600))

    TOKENS_PATH.write_text(json.dumps(tokens, indent=2), encoding="utf-8")
    return (
        "<h3>Oura access token spremljen!</h3>"
        f"<p>Put: {TOKENS_PATH}</p>"
        f"<pre>{json.dumps(tokens, indent=2)}</pre>"
    )

def refresh_tokens(refresh_token: str) -> dict:
    resp = requests.post(
        "https://api.ouraring.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
        timeout=30,
    )
    resp.raise_for_status()
    tokens = resp.json()
    tokens.setdefault("created_at", int(time.time()))
    tokens.setdefault("expires_at", tokens["created_at"] + int(tokens.get("expires_in", 3600)))
    TOKENS_PATH.write_text(json.dumps(tokens, indent=2), encoding="utf-8")
    return tokens

if __name__ == "__main__":
    app.run(port=8000)
