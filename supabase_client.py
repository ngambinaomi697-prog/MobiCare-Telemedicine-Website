"""Minimal server-side Supabase Auth and PostgREST client for the Flask app."""

import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class SupabaseError(Exception):
    def __init__(self, status, message, code=None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def _configuration():
    base_url = (os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL", "")).rstrip("/")
    api_key = (
        os.environ.get("SUPABASE_PUBLISHABLE_KEY")
        or os.environ.get("SUPABASE_ANON_KEY")
        or os.environ.get("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY")
        or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )
    if not base_url or not api_key:
        raise SupabaseError(503, "Supabase is not configured")
    return base_url, api_key


def _request(method, path, payload=None, access_token=None):
    base_url, api_key = _configuration()
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = Request(
        f"{base_url}{path}",
        data=body,
        headers={
            "apikey": api_key,
            "Authorization": f"Bearer {access_token or api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        method=method,
    )
    try:
        with urlopen(request, timeout=12) as response:
            raw = response.read()
    except HTTPError as error:
        raw = error.read()
        try:
            details = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            details = {}
        message = details.get("msg") or details.get("message") or details.get("error_description") or details.get("error") or "Supabase request failed"
        raise SupabaseError(error.code, str(message), details.get("code")) from None
    except (URLError, TimeoutError, OSError) as error:
        raise SupabaseError(503, "Supabase is temporarily unavailable") from error

    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SupabaseError(502, "Supabase returned an invalid response") from error


def sign_up(email, password, metadata, redirect_to):
    path = "/auth/v1/signup"
    if redirect_to:
        path += "?" + urlencode({"redirect_to": redirect_to})
    return _request("POST", path, {"email": email, "password": password, "data": metadata})


def sign_in(email, password):
    return _request("POST", "/auth/v1/token?grant_type=password", {"email": email, "password": password})


def refresh_session(refresh_token):
    return _request("POST", "/auth/v1/token?grant_type=refresh_token", {"refresh_token": refresh_token})


def get_user(access_token):
    return _request("GET", "/auth/v1/user", access_token=access_token)


def get_profile(access_token, user_id):
    query = urlencode({"id": f"eq.{user_id}", "select": "id,full_name,phone,role"})
    rows = _request("GET", f"/rest/v1/profiles?{query}", access_token=access_token)
    return rows[0] if isinstance(rows, list) and rows else None


def sign_out(access_token):
    return _request("POST", "/auth/v1/logout", access_token=access_token)
