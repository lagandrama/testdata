# testdata/health_sync/sources/rollaone.py
import os
import time
from typing import Any, Dict, List, Optional, Tuple
import requests
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone

class RollaOneClient:
    """
    Minimalni Rolla One API klijent (session-based auth).

    - .env se čita u __init__
    - login na /api/login (email + pass) -> token
    - token se postavlja i kao Cookie i kao Authorization header
    - _request() automatski relogin na 401
    - endpointi koriste uglavnom GET sa query parametrima
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        email: Optional[str] = None,
        password: Optional[str] = None,
        debug: bool | None = None,
    ):
        self.base = (base_url or os.getenv("ROLLAONE_BASE_URL", "https://api.rolla.app")).rstrip("/")
        self.email = email or os.getenv("ROLLAONE_EMAIL")
        self.password = password or os.getenv("ROLLAONE_PASSWORD")
        # Omogući override login puta (ako se API promijenio)
        self.login_path = os.getenv("ROLLAONE_LOGIN_PATH", "/api/login")
        self.debug = (str(debug) if debug is not None else os.getenv("ROLLAONE_DEBUG", "0")) == "1"

        self._s = requests.Session()
        self._token: Optional[str] = None
        self._expires_at: float = 0.0  # ne znamo TTL; relogin radimo po 401

    # ---------- helpers ----------
    def _cookie_domain(self) -> Optional[str]:
        try:
            return urlparse(self.base).hostname
        except Exception:
            return None

    def _login(self) -> None:
        if not self.email or not self.password:
            raise RuntimeError("ROLLAONE_EMAIL ili ROLLAONE_PASSWORD nisu postavljeni (provjeri .env)")

        url = urljoin(self.base, self.login_path)

        # Pokušaj više varijanti payloada jer se API ponekad mijenja
        attempts = [
            (True,  {"email": self.email, "password": self.password}),  # JSON, 'password'
            (False, {"email": self.email, "password": self.password}),  # form, 'password'
            (False, {"email": self.email, "pass": self.password}),      # form, 'pass'
        ]

        last_payload: dict | None = None
        last_data: dict | None = None
        for as_json, payload in attempts:
            try:
                last_payload = payload
                if as_json:
                    r = requests.post(url, json=payload, timeout=30)
                else:
                    r = requests.post(url, data=payload, timeout=30)
                r.raise_for_status()
                data = r.json()
                last_data = data if isinstance(data, dict) else None
            except Exception:
                continue

            token = None
            if isinstance(last_data, dict):
                token = (
                    last_data.get("token")
                    or last_data.get("access_token")
                    or (last_data.get("data", {}) if isinstance(last_data.get("data"), dict) else {}).get("token")
                )

            if token:
                self._token = token
                break

        if not self._token:
            if self.debug:
                print("[rollaone] Login failed. Response:", last_data, "Payload:", last_payload)
            raise RuntimeError(f"ROLLAONE login failed: {last_data or 'Unknown response'}")

        self._token = data["token"]
        # Authorization header + cookie (pokrij obje varijante servera)
        self._s.headers.update({"Authorization": f"Bearer {self._token}"})
        try:
            self._s.cookies.set("token", self._token, domain=self._cookie_domain())
        except Exception:
            self._s.cookies.set("token", self._token)

        # ne znamo stvarni expiry; postavi "soft" 12h
        self._expires_at = time.time() + 12 * 3600

        if self.debug:
            print("[rollaone] Logged in.")

    def _request(self, method: str, path: str, params: Dict[str, Any] | None = None, data: Dict[str, Any] | None = None) -> Dict[str, Any]:
        if not self._token or time.time() >= self._expires_at:
            self._login()

        url = urljoin(self.base, path)
        r = self._s.request(method, url, params=params, data=data, timeout=30)

        if r.status_code == 401:
            # probaj jednom relogin
            if self.debug:
                print("[rollaone] 401 -> re-login")
            self._login()
            r = self._s.request(method, url, params=params, data=data, timeout=30)

        r.raise_for_status()
        payload = r.json()
        # neki endpointi vraćaju success false sa reason
        if payload.get("success") is False and payload.get("reason"):
            raise RuntimeError(payload.get("reason"))
        return payload

    # ---------- generic extractors ----------
    @staticmethod
    def _to_date_iso(v: Any) -> Optional[str]:
        """
        Pokušaj izvući YYYY-MM-DD iz stringa ili unix timestamp-a.
        """
        if v is None:
            return None
        # ako je već string sa datumom
        if isinstance(v, str):
            # uzmi samo date dio ako je timestamp s vremenom
            try:
                # ISO sa T ili space
                dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
                return dt.date().isoformat()
            except Exception:
                # možda je "YYYY-MM-DD"
                if len(v) >= 10 and v[4] == "-" and v[7] == "-":
                    return v[:10]
                return None
        # integer/float unix timestamp
        try:
            if isinstance(v, (int, float)):
                return datetime.fromtimestamp(v, tz=timezone.utc).date().isoformat()
        except Exception:
            pass
        return None

    @staticmethod
    def _first_present(d: Dict[str, Any], *names: str) -> Any:
        for n in names:
            if n in d and d[n] is not None:
                return d[n]
        return None

    # ---------- endpoints we use ----------
    def steps(self, from_date: str, to_date: str, granularity: str = "daily") -> List[Dict[str, Any]]:
        """
        GET /health/steps/get
        Očekujemo listu; tolerantno čitamo 'steps', 'steps_data', 'data', 'items'.
        """
        out = self._request("GET", "/health/steps/get", params={"from": from_date, "to": to_date, "type": granularity})
        for k in ("steps", "steps_data", "data", "items"):
            v = out.get(k)
            if isinstance(v, list):
                return v
        # ponekad vrati mapu {date: value}
        v = out.get("steps") or out.get("steps_data")
        if isinstance(v, dict):
            return [{"period_start": k, "steps": v[k]} for k in sorted(v.keys())]
        return []

    def calories(self, from_date: str, to_date: str, granularity: str = "daily") -> List[Dict[str, Any]]:
        """
        GET /health/calories2/get
        Ključevi u responsu: 'active_calories' (lista) + agregati.
        """
        out = self._request("GET", "/health/calories2/get", params={"from": from_date, "to": to_date, "type": granularity})
        v = out.get("active_calories")
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            # fallback ako dođe kao mapa
            return [{"period_start": k, "calories": v[k]} for k in sorted(v.keys())]
        # fallback na 'data/items'
        for k in ("data", "items"):
            v = out.get(k)
            if isinstance(v, list):
                return v
        return []

    def heartrate(self, from_date: str, to_date: str, granularity: str = "daily") -> List[Dict[str, Any]]:
        out = self._request("GET", "/health/heartrate/get", params={"from": from_date, "to": to_date, "type": granularity})
        for k in ("heart_rate_data", "data", "items"):
            v = out.get(k)
            if isinstance(v, list):
                return v
        return []

    def hrv(self, from_date: str, to_date: str, granularity: str = "daily") -> List[Dict[str, Any]]:
        out = self._request("GET", "/health/hrv/get", params={"from": from_date, "to": to_date, "type": granularity})
        v = out.get("hrv_data")
        if isinstance(v, dict):
            if isinstance(v.get("items"), list):
                return v["items"]
        if isinstance(out.get("items"), list):
            return out["items"]
        if isinstance(out.get("data"), list):
            return out["data"]
        return []

    def sleep(self, from_date: str, to_date: str, sleep_type: str = "all") -> List[Dict[str, Any]]:
        """
        type: 'daily' ili 'all' (segmenti).
        """
        out = self._request("GET", "/health/sleep/get", params={"from": from_date, "to": to_date, "type": sleep_type})
        v = out.get("sleep")
        if isinstance(v, list):
            return v
        if isinstance(v, dict):
            # fallback ako dođe kao mapa
            return [{"period_start": k, **(v[k] if isinstance(v[k], dict) else {"value": v[k]})} for k in sorted(v.keys())]
        return []

    # score endpoints postoje samo specifični:
    def score_steps(self, date: str) -> Optional[float]:
        out = self._request("GET", "/health/score/steps", params={"date": date})
        return out.get("score")

    def score_active_calories(self, date: str) -> Optional[float]:
        out = self._request("GET", "/health/score/active_calories", params={"date": date})
        return out.get("score")
