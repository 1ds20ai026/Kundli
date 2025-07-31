import datetime as dt
import os
import re
from typing import Optional, Tuple, Dict, Any

import requests
from dotenv import load_dotenv

from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder

load_dotenv()

PROKERALA_CLIENT_ID = os.getenv("PROKERALA_CLIENT_ID")
PROKERALA_CLIENT_SECRET = os.getenv("PROKERALA_CLIENT_SECRET")

TOKEN_URL = "https://api.prokerala.com/token"
CHART_URL = "https://api.prokerala.com/v2/astrology/chart"
DIVISIONAL_URL = "https://api.prokerala.com/v2/astrology/divisional-planet-position"
KUNDLI_URL = "https://api.prokerala.com/v2/astrology/kundli"
DASHA_URL = "https://api.prokerala.com/v2/astrology/dasha-periods"  # NEW

_TOKEN_CACHE = {"access_token": None, "expires_at": None}


# ---------- OAuth client ----------
class ProkeralaClient:
    def __init__(self, client_id: Optional[str] = None, client_secret: Optional[str] = None):
        self.client_id = client_id or PROKERALA_CLIENT_ID
        self.client_secret = client_secret or PROKERALA_CLIENT_SECRET
        if not self.client_id or not self.client_secret:
            raise ValueError("Missing PROKERALA_CLIENT_ID or PROKERALA_CLIENT_SECRET in environment.")

    def _get_token(self) -> str:
        now = dt.datetime.utcnow()
        if _TOKEN_CACHE["access_token"] and _TOKEN_CACHE["expires_at"] and now < _TOKEN_CACHE["expires_at"]:
            return _TOKEN_CACHE["access_token"]

        resp = requests.post(
            TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(self.client_id, self.client_secret),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        access_token = data["access_token"]
        expires_in = int(data.get("expires_in", 3600))
        _TOKEN_CACHE["access_token"] = access_token
        _TOKEN_CACHE["expires_at"] = now + dt.timedelta(seconds=expires_in - 60)
        return access_token

    @staticmethod
    def _force_jan1(iso_local: str) -> str:
        """Turn 'YYYY-MM-DDTHH:MM:SS+HH:MM' into 'YYYY-01-01THH:MM:SS+HH:MM'."""
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}:\d{2}:\d{2})([+-]\d{2}:\d{2})$", iso_local)
        if not m:
            return iso_local
        year, _, _, hms, off = m.groups()
        return f"{year}-01-01T{hms}{off}"

    # -------- Chart (SVG) with sandbox auto-retry --------
    def get_chart_svg(
        self,
        *,
        lat: float,
        lon: float,
        birth_dt_local_with_offset_iso: str,
        chart_type: str,
        chart_style: str,
        language: str = "en",
        upagraha_position: str = "middle",
        ayanamsa: int = 1,  # Lahiri (hidden from UI)
    ) -> Tuple[str, bool]:
        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}", "Accept": "image/svg+xml"}

        def call(datetime_value: str) -> requests.Response:
            params = {
                "ayanamsa": ayanamsa,
                "coordinates": f"{lat},{lon}",
                "datetime": datetime_value,
                "chart_type": chart_type,
                "chart_style": chart_style,
                "format": "svg",
                "la": language,
                "upagraha_position": upagraha_position,
            }
            return requests.get(CHART_URL, headers=headers, params=params, timeout=30)

        resp = call(birth_dt_local_with_offset_iso)
        if resp.status_code < 400:
            return resp.text, False

        used_fallback = False
        try:
            msg = resp.json()
            if (
                resp.status_code == 400
                and isinstance(msg, dict)
                and any(e.get("code") == "1004" for e in msg.get("errors", []))
            ):
                used_fallback = True
                jan1_iso = self._force_jan1(birth_dt_local_with_offset_iso)
                resp2 = call(jan1_iso)
                resp2.raise_for_status()
                return resp2.text, used_fallback
        except Exception:
            pass

        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise requests.HTTPError(f"{resp.status_code} {resp.reason}: {detail}")

    # -------- Divisional Planet Position (JSON) with sandbox auto-retry --------
    def get_divisional_planet_position(
        self,
        *,
        lat: float,
        lon: float,
        birth_dt_local_with_offset_iso: str,
        chart_type: str,
        language: str = "en",
        ayanamsa: int = 1,  # Lahiri
    ) -> Tuple[Dict[str, Any], bool]:
        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        def call(datetime_value: str) -> requests.Response:
            params = {
                "ayanamsa": ayanamsa,
                "coordinates": f"{lat},{lon}",
                "datetime": datetime_value,
                "chart_type": chart_type,
                "la": language,
            }
            return requests.get(DIVISIONAL_URL, headers=headers, params=params, timeout=30)

        resp = call(birth_dt_local_with_offset_iso)
        if resp.status_code < 400:
            return resp.json(), False

        used_fallback = False
        try:
            msg = resp.json()
            if (
                resp.status_code == 400
                and isinstance(msg, dict)
                and any(e.get("code") == "1004" for e in msg.get("errors", []))
            ):
                used_fallback = True
                jan1_iso = self._force_jan1(birth_dt_local_with_offset_iso)
                resp2 = call(jan1_iso)
                resp2.raise_for_status()
                return resp2.json(), used_fallback
        except Exception:
            pass

        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise requests.HTTPError(f"{resp.status_code} {resp.reason}: {detail}")

    # -------- Kundli (JSON) with sandbox auto-retry --------
    def get_kundli(
        self,
        *,
        lat: float,
        lon: float,
        birth_dt_local_with_offset_iso: str,
        language: str = "en",
        ayanamsa: int = 1,
    ) -> Tuple[Dict[str, Any], bool]:
        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        def call(datetime_value: str) -> requests.Response:
            params = {
                "ayanamsa": ayanamsa,
                "coordinates": f"{lat},{lon}",
                "datetime": datetime_value,
                "la": language,
            }
            return requests.get(KUNDLI_URL, headers=headers, params=params, timeout=30)

        resp = call(birth_dt_local_with_offset_iso)
        if resp.status_code < 400:
            return resp.json(), False

        used_fallback = False
        try:
            msg = resp.json()
            if (
                resp.status_code == 400
                and isinstance(msg, dict)
                and any(e.get("code") == "1004" for e in msg.get("errors", []))
            ):
                used_fallback = True
                jan1_iso = self._force_jan1(birth_dt_local_with_offset_iso)
                resp2 = call(jan1_iso)
                resp2.raise_for_status()
                return resp2.json(), used_fallback
        except Exception:
            pass

        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise requests.HTTPError(f"{resp.status_code} {resp.reason}: {detail}")

    # -------- Dasha Periods (JSON) with sandbox auto-retry) --------
    def get_dasha_periods(
        self,
        *,
        lat: float,
        lon: float,
        birth_dt_local_with_offset_iso: str,
        language: str = "en",
        year_length: int = 1,   # 1 = 365.25 days, 0 = 360 days
        ayanamsa: int = 1,      # Lahiri
    ) -> Tuple[Dict[str, Any], bool]:
        """
        Returns (json_data, used_sandbox_fallback).
        """
        token = self._get_token()
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

        def call(datetime_value: str) -> requests.Response:
            params = {
                "ayanamsa": ayanamsa,
                "coordinates": f"{lat},{lon}",
                "datetime": datetime_value,
                "la": language,
                "year_length": year_length,
            }
            return requests.get(DASHA_URL, headers=headers, params=params, timeout=30)

        resp = call(birth_dt_local_with_offset_iso)
        if resp.status_code < 400:
            return resp.json(), False

        used_fallback = False
        try:
            msg = resp.json()
            if (
                resp.status_code == 400
                and isinstance(msg, dict)
                and any(e.get("code") == "1004" for e in msg.get("errors", []))
            ):
                used_fallback = True
                jan1_iso = self._force_jan1(birth_dt_local_with_offset_iso)
                resp2 = call(jan1_iso)
                resp2.raise_for_status()
                return resp2.json(), used_fallback
        except Exception:
            pass

        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise requests.HTTPError(f"{resp.status_code} {resp.reason}: {detail}")


# ---------- Utilities ----------
_geocoder = Nominatim(user_agent="kundli_streamlit_app")
_tzf = TimezoneFinder()


def geocode_place(place: str):
    loc = _geocoder.geocode(place)
    if not loc:
        raise ValueError("Unable to geocode the given place. Try a more specific query.")
    return float(loc.latitude), float(loc.longitude), loc.address


def infer_timezone(lat: float, lon: float) -> str:
    tz = _tzf.timezone_at(lat=lat, lng=lon)
    if not tz:
        raise ValueError("Unable to infer time zone from coordinates.")
    return tz


def local_birth_to_iso_with_offset(birth_date: dt.date, birth_time: dt.time, tz_name: str) -> str:
    """
    Return local datetime with numeric offset, e.g. 'YYYY-MM-DDTHH:MM:SS+05:30'
    """
    try:
        from zoneinfo import ZoneInfo
        local = dt.datetime.combine(birth_date, birth_time, tzinfo=ZoneInfo(tz_name))
        s = local.strftime("%Y-%m-%dT%H:%M:%S%z")
        return s[:-2] + ":" + s[-2:]  # +0530 -> +05:30
    except Exception:
        import pytz
        tz = pytz.timezone(tz_name)
        local = tz.localize(dt.datetime.combine(birth_date, birth_time))
        offset = local.strftime("%z")
        offset = offset[:-2] + ":" + offset[-2:]
        return local.strftime("%Y-%m-%dT%H:%M:%S") + offset
