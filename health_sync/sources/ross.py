# sources/ross.py
import os
import time
from typing import Dict, Any, List, Optional

import requests


class RossClient:
    """
    Minimalan ROSS API klijent.

    - .env se čita u __init__ (nema više zavisnosti od redoslijeda importa)
    - _post() automatski relogin na 401
    - score() podržava više oblika odgovora (lista ili mapa)
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        partner_id: Optional[str] = None,
        email: Optional[str] = None,
        password: Optional[str] = None,
    ):
        # čitaj iz parametara ili iz okoline
        self.base = base_url or os.getenv("ROSS_BASE_URL", "https://ross.rolla.cloud")
        self.partner_id = partner_id or os.getenv("ROSS_PARTNER_ID")
        self.email = email or os.getenv("ROSS_EMAIL")
        self.password = password or os.getenv("ROSS_PASSWORD")

        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.expires_at: float = 0.0

    # --- auth ---
    def _login(self) -> None:
        if not self.partner_id:
            raise RuntimeError("ROSS_PARTNER_ID nije postavljen (provjeri .env)")
        if not self.email or not self.password:
            raise RuntimeError("ROSS_EMAIL ili ROSS_PASSWORD nisu postavljeni (provjeri .env)")

        r = requests.post(
            f"{self.base}/api/login",
            headers={"Partner-ID": self.partner_id},
            data={"email": self.email, "password": self.password},
            timeout=30,
        )
        data = r.json()
        if not r.ok or not data.get("success", True):
            raise RuntimeError(f"ROSS login failed: {data}")

        tokens = data.get("tokens", {})
        self.access_token = tokens.get("access_token")
        self.refresh_token = tokens.get("refresh_token")
        # expires_in tipično ~1800s; ostavi buffer
        self.expires_at = time.time() + int(tokens.get("expires_in", 1500)) - 60

    def _headers(self) -> Dict[str, str]:
        if not self.access_token or time.time() >= self.expires_at:
            self._login()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Partner-ID": self.partner_id or "",
        }

    def _post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        r = requests.post(url, headers=self._headers(), data=data, timeout=30)

        # 401 -> probaj jednom relogin
        if r.status_code == 401:
            self._login()
            r = requests.post(url, headers=self._headers(), data=data, timeout=30)

        r.raise_for_status()
        payload = r.json()
        if payload.get("success") is False:
            raise RuntimeError(payload.get("reason", "ROSS error"))
        return payload

    # --- health endpoints ---
    def steps(self, from_date: str, to_date: str, granularity: str = "daily") -> List[Dict[str, Any]]:
        out = self._post("/health/steps/get", {"from": from_date, "to": to_date, "type": granularity})
        return out.get("steps", [])

    def calories(self, from_date: str, to_date: str, granularity: str = "daily") -> List[Dict[str, Any]]:
        out = self._post("/health/calories/get", {"from": from_date, "to": to_date, "type": granularity})
        return out.get("calories", [])

    def heartrate(self, from_date: str, to_date: str, granularity: str = "daily") -> List[Dict[str, Any]]:
        out = self._post("/health/heartrate/get", {"from": from_date, "to": to_date, "type": granularity})
        return out.get("heart_rate_data", [])

    def hrv(self, from_date: str, to_date: str, granularity: str = "daily") -> List[Dict[str, Any]]:
        out = self._post("/health/hrv/get", {"from": from_date, "to": to_date, "type": granularity})
        return out.get("hrv_data", [])

    def rhr(self, from_date: str, to_date: str, granularity: str = "daily") -> List[Dict[str, Any]]:
        out = self._post("/health/rhr/get", {"from": from_date, "to": to_date, "type": granularity})
        return out.get("rhr", [])

    def sleep(self, from_date: str, to_date: str, sleep_type: str = "all") -> List[Dict[str, Any]]:
        out = self._post("/health/sleep/get", {"from": from_date, "to": to_date, "type": sleep_type})
        return out.get("sleep", [])

    def sleep_daily(self, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        """Dnevni agregati spavanja (score, trajanje, itd.) ako su dostupni."""
        out = self._post("/health/sleep/get", {"from": from_date, "to": to_date, "type": "daily"})
        return out.get("sleep", [])

    def score(self, score_name: str, from_date: str, to_date: str, granularity: str = "daily") -> List[Dict[str, Any]]:
        """
        Vrati listu zapisa oblika:
          [{"period_start": "YYYY-MM-DD", "score": N}, ...]
        Podržava više mogućih oblika odgovora backend-a (lista ili mapa).
        """
        out = self._post(
            "/health/score/get",
            {"score": score_name, "from": from_date, "to": to_date, "type": granularity},
        )

        # 1) Najčešće: lista pod "scores"
        v = out.get("scores")
        if isinstance(v, list):
            return v

        # 2) Nekad: lista pod "score"
        v = out.get("score")
        if isinstance(v, list):
            return v

        # 3) Nekad: mapa { "YYYY-MM-DD": N }
        v = out.get("scores")
        if isinstance(v, dict):
            return [{"period_start": k, "score": v[k]} for k in sorted(v.keys())]

        # 4) Fallback na tipične ključeve koji nose listu
        for k in ("data", "result", "results", "items"):
            v = out.get(k)
            if isinstance(v, list):
                return v

        # neočekivan oblik – digni jasnu grešku da vidimo payload jednom
        raise RuntimeError(f"Unexpected response from /health/score/get: {out}")

    # --- activities (opcionalno) ---
    def activities_list(
        self,
        limit: int = 50,
        cursor: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        import urllib.parse

        q = []
        if from_date:
            q.append(("from", from_date))
        if to_date:
            q.append(("to", to_date))
        if limit:
            q.append(("limit", str(limit)))
        if cursor:
            q.append(("cursor", cursor))

        url = f"{self.base}/activities/list"
        if q:
            url += "?" + urllib.parse.urlencode(q)

        r = requests.get(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()
