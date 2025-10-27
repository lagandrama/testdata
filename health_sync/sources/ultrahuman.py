# sources/ultrahuman.py
import os
import time
import urllib.parse
from typing import Any, Dict, Optional

import requests


class UltrahumanClient:
    """
    Minimalni Ultrahuman Partner API klijent (OAuth2 + metrics).
    - Čita konfiguraciju iz okoline (.env) u __init__
    - Podržava Authorization Code i Refresh Token tokene
    - Automatski refresh na 401, jednom ponovi zahtjev
    - get_metrics(date, email=...) vraća JSON payload za zadani dan

    Dokumentacija (sažetak):
      - Base: https://partner.ultrahuman.com
      - OAuth:   /oauth/authorize, /oauth/token
      - API v1:  /api/v1/metrics?email=...&date=YYYY-MM-DD
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        scopes: Optional[str] = None,
        user_email: Optional[str] = None,
        timeout_seconds: int = 30,
    ):
        self.base = (base_url or os.getenv("ULTRAHUMAN_BASE_URL", "https://partner.ultrahuman.com")).rstrip("/")
        self.client_id = client_id or os.getenv("ULTRAHUMAN_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("ULTRAHUMAN_CLIENT_SECRET")
        self.redirect_uri = redirect_uri or os.getenv("ULTRAHUMAN_REDIRECT_URI")
        self.scopes = scopes or os.getenv("ULTRAHUMAN_SCOPES", "profile ring_data")
        self.user_email = user_email or os.getenv("ULTRAHUMAN_USER_EMAIL")

        self._access_token = access_token or os.getenv("ULTRAHUMAN_ACCESS_TOKEN")
        self._refresh_token = refresh_token or os.getenv("ULTRAHUMAN_REFRESH_TOKEN")
        self._expires_at: float = 0.0  # ako ne znamo expiraciju, ćemo refreshati kad dobijemo 401
        self.timeout = timeout_seconds

    # ---------- OAuth helpers ----------

    def build_authorize_url(self, state: Optional[str] = None) -> str:
        """
        Vrati authorize URL koji možeš otvoriti u browseru da dobiješ authorization code.
        """
        q = {
            "response_type": "code",
            "client_id": self.client_id or "",
            "redirect_uri": self.redirect_uri or "",
            "scope": self.scopes,
        }
        if state:
            q["state"] = state
        return f"{self.base}/oauth/authorize?{urllib.parse.urlencode(q)}"

    def exchange_code_for_token(self, code: str) -> Dict[str, Any]:
        """
        Razmijeni authorization code za access+refresh token.
        Sačuvaj refresh token (npr. u .env / secret store).
        """
        if not self.client_id or not self.client_secret or not self.redirect_uri:
            raise RuntimeError("ULTRAHUMAN_CLIENT_ID/SECRET/REDIRECT_URI nisu postavljeni")
        url = f"{self.base}/oauth/token"
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        r = requests.post(url, data=data, timeout=self.timeout)
        r.raise_for_status()
        tok = r.json()
        self._apply_token_response(tok)
        return tok

    def refresh_access_token(self) -> Dict[str, Any]:
        """
        Osvježi access token koristeći refresh token.
        """
        if not self._refresh_token:
            raise RuntimeError("Nema ULTRAHUMAN_REFRESH_TOKEN; uradi OAuth exchange prvo.")
        if not self.client_id or not self.client_secret:
            raise RuntimeError("ULTRAHUMAN_CLIENT_ID/SECRET nisu postavljeni")
        url = f"{self.base}/oauth/token"
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        r = requests.post(url, data=data, timeout=self.timeout)
        r.raise_for_status()
        tok = r.json()
        self._apply_token_response(tok, keep_refresh_if_missing=True)
        return tok

    def _apply_token_response(self, tok: Dict[str, Any], keep_refresh_if_missing: bool = False) -> None:
        self._access_token = tok.get("access_token") or self._access_token
        new_refresh = tok.get("refresh_token")
        if new_refresh or not keep_refresh_if_missing:
            self._refresh_token = new_refresh
        expires_in = tok.get("expires_in")
        if isinstance(expires_in, (int, float)):
            # ostavi 60s buffer
            self._expires_at = time.time() + float(expires_in) - 60

    def _headers(self) -> Dict[str, str]:
        if not self._access_token or time.time() >= self._expires_at:
            # Ako znamo refresh token, probaj odmah refresh
            if self._refresh_token:
                try:
                    self.refresh_access_token()
                except Exception:
                    # fallback: koristit ćemo postojeći access token; ako je 401, opet ćemo probati refresh
                    pass
        if not self._access_token:
            raise RuntimeError("ULTRAHUMAN_ACCESS_TOKEN nije dostupan. Uradi OAuth exchange.")
        return {"Authorization": f"Bearer {self._access_token}"}

    # ---------- HTTP wrappers ----------

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"  # path tipa '/api/v1/metrics'
        r = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout)
        # 401 -> pokušaj refresh jednom
        if r.status_code == 401 and self._refresh_token:
            self.refresh_access_token()
            r = requests.get(url, headers=self._headers(), params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    # ---------- API v1 ----------

    def get_metrics(self, date: str, email: Optional[str] = None) -> Dict[str, Any]:
        """
        Vrati metrike za jedan dan (YYYY-MM-DD) i korisnika (email).
        """
        email = email or self.user_email
        if not email:
            raise RuntimeError("ULTRAHUMAN_USER_EMAIL nije postavljen")
        return self._get("/api/v1/metrics", params={"email": email, "date": date})

    def get_user_info(self) -> Dict[str, Any]:
        """
        (Opcionalno) Ako je izložen user_info endpoint i scope 'profile' – možeš ga pozvati.
        """
        return self._get("/api/v1/user_info", params={"email": self.user_email} if self.user_email else None)


if __name__ == "__main__":
    # Mali CLI za pomoć oko OAuth-a:
    #   python -m health_sync.sources.ultrahuman auth-url
    #   python -m health_sync.sources.ultrahuman exchange-code <CODE>
    import sys

    uc = UltrahumanClient()
    if len(sys.argv) >= 2 and sys.argv[1] == "auth-url":
        print(uc.build_authorize_url())
    elif len(sys.argv) >= 3 and sys.argv[1] == "exchange-code":
        code = sys.argv[2]
        print(uc.exchange_code_for_token(code))
    else:
        print("Usage:\n  auth-url\n  exchange-code <CODE>")
