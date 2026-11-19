"""
==========================================================
 compare - Telemedicine Platform
 Pure Python backend that connects to your existing HTML files
 Run: python MobiCare.py
==========================================================
"""

import asyncio
import hmac
import math
import os
import secrets
import sqlite3
import threading
import time
import uuid
from datetime import datetime
from functools import wraps

import asyncpg

from flask import (
    Flask, abort, flash, g, redirect, render_template, request,
    session, url_for
)
from werkzeug.security import generate_password_hash

import supabase_client
from supabase_client import SupabaseError

# ==========================================================
# APP CONFIGURATION
# ==========================================================
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="None",
    SESSION_COOKIE_SECURE=True,
)


@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Permissions-Policy"] = "camera=(self), microphone=(self), geolocation=()"
    if os.environ.get("VERCEL_ENV") == "production":
        response.headers["Strict-Transport-Security"] = "max-age=63072000"
    return response


DATABASE = os.environ.get("MOBICARE_DATABASE", "mobicare.db")
_neon_loop = None
_neon_loop_thread = None
_neon_loop_lock = threading.Lock()
_neon_pool = None
_neon_pool_lock = None


def _get_neon_loop():
    global _neon_loop, _neon_loop_thread
    with _neon_loop_lock:
        if _neon_loop is None:
            _neon_loop = asyncio.new_event_loop()
            _neon_loop_thread = threading.Thread(
                target=_neon_loop.run_forever,
                name="mobicare-neon-asyncio",
                daemon=True,
            )
            _neon_loop_thread.start()
    return _neon_loop


async def _get_neon_pool():
    global _neon_pool, _neon_pool_lock
    if _neon_pool_lock is None:
        _neon_pool_lock = asyncio.Lock()
    async with _neon_pool_lock:
        if _neon_pool is None:
            database_url = os.environ.get("DATABASE_URL")
            if not database_url:
                raise RuntimeError("DATABASE_URL is not configured")
            _neon_pool = await asyncpg.create_pool(
                dsn=database_url,
                min_size=1,
                max_size=5,
                command_timeout=10,
            )
    return _neon_pool


def _run_neon(coroutine):
    loop = _get_neon_loop()
    future = asyncio.run_coroutine_threadsafe(coroutine, loop)
    return future.result(timeout=15)


async def _list_patient_vitals(patient_id, limit=20):
    pool = await _get_neon_pool()
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT id, patient_id, systolic_mm_hg, diastolic_mm_hg,
                   heart_rate_bpm, temperature_c, oxygen_saturation,
                   alert_level, recorded_at, reviewed_at
            FROM public.patient_vitals
            WHERE patient_id = $1
            ORDER BY recorded_at DESC
            LIMIT $2
            """,
            int(patient_id),
            int(limit),
        )
    return [dict(row) for row in rows]


async def _list_vitals_for_patients(patient_ids, limit=100):
    if not patient_ids:
        return []
    pool = await _get_neon_pool()
    async with pool.acquire() as connection:
        rows = await connection.fetch(
            """
            SELECT id, patient_id, systolic_mm_hg, diastolic_mm_hg,
                   heart_rate_bpm, temperature_c, oxygen_saturation,
                   alert_level, recorded_at, reviewed_at
            FROM public.patient_vitals
            WHERE patient_id = ANY($1::BIGINT[])
            ORDER BY CASE alert_level
                       WHEN 'urgent' THEN 0
                       WHEN 'review' THEN 1
                       ELSE 2
                     END,
                     recorded_at DESC
            LIMIT $2
            """,
            [int(patient_id) for patient_id in patient_ids],
            int(limit),
        )
    return [dict(row) for row in rows]


async def _insert_patient_vital(patient_id, values, alert_level):
    pool = await _get_neon_pool()
    async with pool.acquire() as connection:
        return await connection.fetchrow(
            """
            INSERT INTO public.patient_vitals
                (patient_id, systolic_mm_hg, diastolic_mm_hg,
                 heart_rate_bpm, temperature_c, oxygen_saturation, alert_level)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """,
            int(patient_id),
            values["systolic_mm_hg"],
            values["diastolic_mm_hg"],
            values["heart_rate_bpm"],
            values["temperature_c"],
            values["oxygen_saturation"],
            alert_level,
        )


async def _get_vital_patient_id(vital_id):
    pool = await _get_neon_pool()
    async with pool.acquire() as connection:
        return await connection.fetchval(
            "SELECT patient_id FROM public.patient_vitals WHERE id = $1",
            int(vital_id),
        )


async def _mark_vital_reviewed(vital_id, doctor_id):
    pool = await _get_neon_pool()
    async with pool.acquire() as connection:
        return await connection.fetchrow(
            """
            UPDATE public.patient_vitals
            SET reviewed_by = $2, reviewed_at = NOW()
            WHERE id = $1 AND alert_level <> 'routine' AND reviewed_at IS NULL
            RETURNING id
            """,
            int(vital_id),
            int(doctor_id),
        )


def _classify_vitals(values):
    systolic = values["systolic_mm_hg"]
    diastolic = values["diastolic_mm_hg"]
    heart_rate = values["heart_rate_bpm"]
    temperature = values["temperature_c"]
    oxygen = values["oxygen_saturation"]

    if (
        systolic >= 180
        or diastolic >= 120
        or heart_rate < 40
        or heart_rate >= 130
        or temperature >= 40
        or oxygen < 90
    ):
        return "urgent"
    if (
        systolic < 90
        or systolic >= 140
        or diastolic < 60
        or diastolic >= 90
        or heart_rate < 50
        or heart_rate > 100
        or temperature < 35
        or temperature >= 38
        or oxygen < 94
    ):
        return "review"
    return "routine"


def _parse_vitals_form():
    try:
        values = {
            "systolic_mm_hg": int(request.form.get("systolic_mm_hg", "")),
            "diastolic_mm_hg": int(request.form.get("diastolic_mm_hg", "")),
            "heart_rate_bpm": int(request.form.get("heart_rate_bpm", "")),
            "temperature_c": round(float(request.form.get("temperature_c", "")), 1),
            "oxygen_saturation": int(request.form.get("oxygen_saturation", "")),
        }
    except (TypeError, ValueError):
        return None

    if not math.isfinite(values["temperature_c"]):
        return None
    if not 50 <= values["systolic_mm_hg"] <= 300:
        return None
    if not 30 <= values["diastolic_mm_hg"] <= 200:
        return None
    if values["diastolic_mm_hg"] >= values["systolic_mm_hg"]:
        return None
    if not 20 <= values["heart_rate_bpm"] <= 250:
        return None
    if not 25 <= values["temperature_c"] <= 45:
        return None
    if not 50 <= values["oxygen_saturation"] <= 100:
        return None
    return values


def _validate_csrf_token():
    expected = session.get("csrf_token")
    submitted = request.form.get("csrf_token", "")
    if not expected or not submitted or not hmac.compare_digest(expected, submitted):
        abort(400)


# ==========================================================
# DATABASE HELPERS
# ==========================================================
def get_db():
    """Open a database connection."""
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Create tables + seed demo users on first run."""
    conn = get_db()
    cur = conn.cursor()
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            full_name TEXT NOT NULL,
            role TEXT NOT NULL,
            phone TEXT,
            specialty TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        

        CREATE TABLE IF NOT EXISTS appointments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id INTEGER NOT NULL,
            doctor_id INTEGER NOT NULL,
            appointment_date TEXT NOT NULL,
            reason TEXT,
            status TEXT DEFAULT 'Scheduled',
            room_id TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (patient_id) REFERENCES users(id),
            FOREIGN KEY (doctor_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS health_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id INTEGER NOT NULL,
            doctor_id INTEGER NOT NULL,
            record_type TEXT NOT NULL,
            diagnosis TEXT NOT NULL,
            prescription TEXT,
            notes TEXT,
            visit_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (patient_id) REFERENCES users(id),
            FOREIGN KEY (doctor_id) REFERENCES users(id)
        );
    """)

    user_columns = {row["name"] for row in cur.execute("PRAGMA table_info(users)")}
    if "phone" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN phone TEXT")
    if "specialty" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN specialty TEXT")
    if "auth_user_id" not in user_columns:
        cur.execute("ALTER TABLE users ADD COLUMN auth_user_id TEXT")
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS users_auth_user_id_unique "
        "ON users(auth_user_id) WHERE auth_user_id IS NOT NULL"
    )
    cur.execute("""
        CREATE TABLE IF NOT EXISTS auth_sessions (
            session_id TEXT PRIMARY KEY,
            auth_user_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            access_token TEXT NOT NULL,
            refresh_token TEXT NOT NULL,
            expires_at REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()
    conn.close()


# ==========================================================
# AUTH DECORATORS & HELPERS
# ==========================================================
VALID_PROFILE_ROLES = {"patient", "doctor", "admin", "administrator"}


def _user_for_app(row):
    return {key: row[key] for key in ("id", "username", "full_name", "role", "phone", "specialty")}


def _delete_auth_session(session_id):
    conn = get_db()
    conn.execute("DELETE FROM auth_sessions WHERE session_id = ?", (session_id,))
    conn.commit()
    conn.close()


def _resolve_local_user(auth_user, profile):
    auth_user_id = str(auth_user.get("id", ""))
    email = (auth_user.get("email") or "").strip().lower()
    role = profile.get("role")
    if not auth_user_id or not email or role not in VALID_PROFILE_ROLES:
        raise PermissionError("This account is not provisioned for MobiCare.")

    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE auth_user_id = ?", (auth_user_id,)
    ).fetchone()
    if user:
        conn.execute(
            "UPDATE users SET role = ?, full_name = ?, phone = ? WHERE id = ?",
            (role, profile.get("full_name") or user["full_name"],
             profile.get("phone"), user["id"]),
        )
        conn.commit()
        user = conn.execute("SELECT * FROM users WHERE id = ?", (user["id"],)).fetchone()
        conn.close()
        return _user_for_app(user)

    legacy_user = conn.execute(
        "SELECT * FROM users WHERE email = ? COLLATE NOCASE", (email,)
    ).fetchone()
    if legacy_user:
        email_verified = bool(auth_user.get("email_confirmed_at") or auth_user.get("confirmed_at"))
        if not email_verified or legacy_user["role"] != role:
            conn.close()
            raise PermissionError(
                "This existing account needs secure administrator provisioning before sign-in."
            )
        conn.execute(
            "UPDATE users SET auth_user_id = ?, full_name = ?, phone = ? WHERE id = ?",
            (auth_user_id, profile.get("full_name") or legacy_user["full_name"],
             profile.get("phone"), legacy_user["id"]),
        )
        conn.commit()
        user = conn.execute("SELECT * FROM users WHERE id = ?", (legacy_user["id"],)).fetchone()
        conn.close()
        return _user_for_app(user)

    preferred_username = (auth_user.get("user_metadata") or {}).get("username")
    base_username = (preferred_username or email.split("@", 1)[0]).strip()[:60] or "patient"
    username = base_username
    if conn.execute(
        "SELECT 1 FROM users WHERE username = ? COLLATE NOCASE", (username,)
    ).fetchone():
        username = f"{base_username[:45]}_{auth_user_id[:8]}"
    full_name = profile.get("full_name") or email.split("@", 1)[0]
    conn.execute(
        "INSERT INTO users (username, email, password_hash, full_name, role, phone, specialty, auth_user_id) "
        "VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
        (username, email, generate_password_hash(secrets.token_urlsafe(32)),
         full_name, role, profile.get("phone"), auth_user_id),
    )
    conn.commit()
    user = conn.execute("SELECT * FROM users WHERE auth_user_id = ?", (auth_user_id,)).fetchone()
    conn.close()
    return _user_for_app(user)


def _save_auth_session(auth_user, profile, auth_response):
    local_user = _resolve_local_user(auth_user, profile)
    access_token = auth_response.get("access_token")
    refresh_token = auth_response.get("refresh_token")
    if not access_token or not refresh_token:
        raise SupabaseError(401, "Supabase did not establish a login session")

    session_id = secrets.token_urlsafe(32)
    expires_at = time.time() + int(auth_response.get("expires_in", 3600))
    conn = get_db()
    conn.execute(
        "INSERT INTO auth_sessions (session_id, auth_user_id, user_id, access_token, refresh_token, expires_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (session_id, str(auth_user["id"]), local_user["id"], access_token, refresh_token, expires_at),
    )
    conn.commit()
    conn.close()

    session.clear()
    session["auth_session_id"] = session_id
    session["user_id"] = local_user["id"]
    session["csrf_token"] = uuid.uuid4().hex
    return local_user


@app.before_request
def load_authenticated_user():
    g.mobicare_user = None
    g.auth_error = False
    session_id = session.get("auth_session_id")
    if not session_id:
        return

    conn = get_db()
    auth_session = conn.execute(
        "SELECT * FROM auth_sessions WHERE session_id = ?", (session_id,)
    ).fetchone()
    conn.close()
    if not auth_session:
        session.clear()
        return

    auth_session = dict(auth_session)
    access_token = auth_session["access_token"]
    try:
        if auth_session["expires_at"] <= time.time() + 60:
            refreshed = supabase_client.refresh_session(auth_session["refresh_token"])
            access_token = refreshed["access_token"]
            auth_session["refresh_token"] = refreshed["refresh_token"]
            auth_session["expires_at"] = time.time() + int(refreshed.get("expires_in", 3600))
            conn = get_db()
            conn.execute(
                "UPDATE auth_sessions SET access_token = ?, refresh_token = ?, expires_at = ? WHERE session_id = ?",
                (access_token, auth_session["refresh_token"], auth_session["expires_at"], session_id),
            )
            conn.commit()
            conn.close()

        auth_user = supabase_client.get_user(access_token)
        if str(auth_user.get("id")) != auth_session["auth_user_id"]:
            raise SupabaseError(401, "Session user mismatch")
        profile = supabase_client.get_profile(access_token, auth_session["auth_user_id"])
        if not profile:
            raise SupabaseError(401, "Profile is unavailable")
        local_user = _resolve_local_user(auth_user, profile)
        if local_user["id"] != auth_session["user_id"]:
            raise SupabaseError(401, "Session mapping mismatch")
        local_user.update(
            full_name=profile.get("full_name") or local_user["full_name"],
            phone=profile.get("phone"),
            role=profile["role"],
        )
        g.mobicare_user = local_user
        session["user_id"] = local_user["id"]
    except PermissionError:
        _delete_auth_session(session_id)
        session.clear()
    except SupabaseError as error:
        if error.status in {400, 401, 403}:
            _delete_auth_session(session_id)
            session.clear()
        else:
            g.auth_error = True


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not g.mobicare_user:
            flash(
                "Session verification is temporarily unavailable. Please try again." if g.auth_error
                else "Please log in to continue.",
                "warning",
            )
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


def role_required(role):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if not g.mobicare_user or g.mobicare_user["role"] != role:
                flash("Access denied.", "danger")
                return redirect(url_for("dashboard"))
            return f(*args, **kwargs)
        return wrapper
    return decorator


def current_user():
    """Return the verified local profile mapped to the current Supabase user."""
    return getattr(g, "mobicare_user", None)


@app.context_processor
def inject_user():
    """Make the current user and form token available to templates."""
    csrf_token = session.setdefault("csrf_token", uuid.uuid4().hex)
    return {"user": current_user(), "csrf_token": csrf_token}


# ==========================================================
# PUBLIC ROUTES
# ==========================================================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if g.mobicare_user:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        _validate_csrf_token()
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm_password", "")
        full_name = request.form.get("full_name", "").strip()
        phone = request.form.get("phone", "").strip()

        if not all([username, email, password, full_name]):
            flash("Please fill in all required fields.", "danger")
            return redirect(url_for("register"))
        if password != confirm:
            flash("Passwords do not match.", "danger")
            return redirect(url_for("register"))
        if len(password) < 6:
            flash("Password must be at least 6 characters.", "danger")
            return redirect(url_for("register"))

        conn = get_db()
        existing_email = conn.execute(
            "SELECT * FROM users WHERE email = ? COLLATE NOCASE", (email,)
        ).fetchone()
        existing_username = conn.execute(
            "SELECT id, email FROM users WHERE username = ? COLLATE NOCASE", (username,)
        ).fetchone()
        conn.close()

        if existing_email and existing_email["auth_user_id"]:
            flash("An account with this email already exists. Please sign in.", "danger")
            return redirect(url_for("login"))
        if existing_email and existing_email["role"] != "patient":
            flash("This existing clinician account needs administrator provisioning before it can use Supabase sign-in.", "warning")
            return redirect(url_for("login"))
        if existing_username and (not existing_email or existing_username["id"] != existing_email["id"]):
            flash("That username is already in use.", "danger")
            return redirect(url_for("register"))

        try:
            signup = supabase_client.sign_up(
                email,
                password,
                {"full_name": full_name, "phone": phone or None, "username": username},
                url_for("login", _external=True, confirmed="1"),
            )
            auth_user = signup.get("user") or signup
            auth_user_id = auth_user.get("id")
            if not auth_user_id:
                raise SupabaseError(502, "Supabase did not return a user identity")
            if not existing_email:
                conn = get_db()
                conn.execute(
                    "INSERT INTO users (username, email, password_hash, full_name, role, phone, specialty, auth_user_id) "
                    "VALUES (?, ?, ?, ?, 'patient', ?, NULL, ?)",
                    (username, email, generate_password_hash(secrets.token_urlsafe(32)),
                     full_name, phone or None, str(auth_user_id)),
                )
                conn.commit()
                conn.close()
            flash("Account created. Check your email to confirm it, then sign in.", "success")
            return redirect(url_for("login"))
        except SupabaseError as error:
            if "password" in error.message.lower() and any(
                word in error.message.lower() for word in ("weak", "short", "characters", "strength")
            ):
                message = "Choose a stronger password that meets the account security requirements."
            elif error.status in {400, 409}:
                message = "We could not create the account. Check the details or sign in if this email is already registered."
            else:
                message = "Account creation is temporarily unavailable. Please try again."
            flash(message, "danger")
            return redirect(url_for("register"))
        except sqlite3.IntegrityError:
            flash("That username or email is already in use. Please sign in or choose another username.", "danger")
            return redirect(url_for("register"))

    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if g.mobicare_user:
        return redirect(url_for("dashboard"))
    if request.args.get("confirmed") == "1" and request.method == "GET":
        flash("Email confirmed. Sign in to continue.", "success")

    if request.method == "POST":
        _validate_csrf_token()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        if not email or not password:
            flash("Enter your email and password.", "danger")
            return render_template("login.html"), 400

        try:
            auth_response = supabase_client.sign_in(email, password)
            auth_user = auth_response.get("user") or {}
            auth_user_id = auth_user.get("id")
            access_token = auth_response.get("access_token")
            if not auth_user_id or not access_token:
                raise SupabaseError(401, "Supabase did not establish a login session")
            profile = supabase_client.get_profile(access_token, auth_user_id)
            if not profile:
                flash("Your profile is not ready yet. Confirm your email and try again.", "warning")
                return render_template("login.html"), 401
            local_user = _save_auth_session(auth_user, profile, auth_response)
        except PermissionError:
            flash("This existing account needs secure administrator provisioning before sign-in.", "warning")
            return render_template("login.html"), 403
        except SupabaseError as error:
            if error.status == 400 and "confirm" in error.message.lower():
                flash("Please confirm your email before signing in.", "warning")
            elif error.status in {400, 401, 403}:
                flash("Invalid email or password.", "danger")
            else:
                flash("Sign-in is temporarily unavailable. Please try again.", "danger")
            return render_template("login.html"), 401 if error.status in {400, 401, 403} else 503
        except sqlite3.IntegrityError:
            flash("This account could not be linked to its local MobiCare record. Contact an administrator.", "warning")
            return render_template("login.html"), 403

        flash(f"Welcome back, {profile.get('full_name') or local_user['full_name']}!", "success")
        return redirect(url_for("dashboard"))

    return render_template("login.html")


@app.route("/logout", methods=["POST"])
def logout():
    _validate_csrf_token()
    session_id = session.get("auth_session_id")
    if session_id:
        conn = get_db()
        auth_session = conn.execute(
            "SELECT access_token FROM auth_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        conn.execute("DELETE FROM auth_sessions WHERE session_id = ?", (session_id,))
        conn.commit()
        conn.close()
        if auth_session:
            try:
                supabase_client.sign_out(auth_session["access_token"])
            except SupabaseError:
                pass
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for("index"))


@app.route("/dashboard")
@login_required
def dashboard():
    if g.mobicare_user["role"] == "doctor":
        return redirect(url_for("doctor_dashboard"))
    if g.mobicare_user["role"] == "patient":
        return redirect(url_for("patient_dashboard"))
    abort(403)


# ==========================================================
# PATIENT PORTAL
# ==========================================================
@app.route("/patient/dashboard")
@login_required
@role_required("patient")
def patient_dashboard():
    user_id = session["user_id"]
    conn = get_db()

    appointments = conn.execute("""
        SELECT a.*, u.full_name AS doctor_name
        FROM appointments a
        JOIN users u ON a.doctor_id = u.id
        WHERE a.patient_id = ?
        ORDER BY a.appointment_date DESC
    """, (user_id,)).fetchall()

    records = conn.execute("""
        SELECT r.*, u.full_name AS doctor_name
        FROM health_records r
        JOIN users u ON r.doctor_id = u.id
        WHERE r.patient_id = ?
        ORDER BY r.visit_date DESC
    """, (user_id,)).fetchall()

    doctors = conn.execute(
        "SELECT id, full_name, specialty FROM users WHERE role = 'doctor'"
    ).fetchall()
    conn.close()

    try:
        vitals = _run_neon(_list_patient_vitals(user_id))
    except Exception as error:
        app.logger.error("Remote vitals load failed (%s)", type(error).__name__)
        vitals = []
        flash("Remote monitoring is temporarily unavailable.", "warning")

    return render_template(
        "patient_dashboard.html",
        appointments=[dict(a) for a in appointments],
        records=[dict(r) for r in records],
        doctors=[dict(d) for d in doctors],
        vitals=vitals,
    )


@app.route("/patient/vitals", methods=["POST"])
@login_required
@role_required("patient")
def submit_vitals():
    _validate_csrf_token()
    values = _parse_vitals_form()
    if values is None:
        flash("Enter valid values for each measurement.", "danger")
        return redirect(url_for("patient_dashboard"))

    alert_level = _classify_vitals(values)
    try:
        _run_neon(
            _insert_patient_vital(session["user_id"], values, alert_level)
        )
    except Exception as error:
        app.logger.error("Remote vitals save failed (%s)", type(error).__name__)
        flash("Your reading could not be saved. Please try again.", "danger")
        return redirect(url_for("patient_dashboard"))

    if alert_level == "urgent":
        flash(
            "Your reading was saved and flagged for urgent clinician review. "
            "This automated flag is not a diagnosis.",
            "warning",
        )
    elif alert_level == "review":
        flash(
            "Your reading was saved and flagged for clinician review. "
            "This automated flag is not a diagnosis.",
            "info",
        )
    else:
        flash("Your reading was saved.", "success")
    return redirect(url_for("patient_dashboard"))


@app.route("/patient/book", methods=["POST"])
@login_required
@role_required("patient")
def book_appointment():
    doctor_id = request.form.get("doctor_id")
    appointment_date = request.form.get("appointment_date")
    reason = request.form.get("reason", "")

    if not doctor_id or not appointment_date:
        flash("Please select a doctor and date.", "danger")
        return redirect(url_for("patient_dashboard"))

    try:
        dt = datetime.strptime(appointment_date, "%Y-%m-%dT%H:%M")
        if dt < datetime.now():
            flash("Cannot book in the past.", "danger")
            return redirect(url_for("patient_dashboard"))
    except ValueError:
        flash("Invalid date format.", "danger")
        return redirect(url_for("patient_dashboard"))

    room_id = str(uuid.uuid4())[:8]
    conn = get_db()
    conn.execute("""
        INSERT INTO appointments
        (patient_id, doctor_id, appointment_date, reason, room_id)
        VALUES (?, ?, ?, ?, ?)
    """, (session["user_id"], doctor_id, appointment_date, reason, room_id))
    conn.commit()
    conn.close()

    flash("Appointment booked successfully!", "success")
    return redirect(url_for("patient_dashboard"))


@app.route("/patient/cancel/<int:appt_id>")
@login_required
@role_required("patient")
def cancel_appointment(appt_id):
    conn = get_db()
    appt = conn.execute(
        "SELECT * FROM appointments WHERE id = ?", (appt_id,)
    ).fetchone()

    if not appt or appt["patient_id"] != session["user_id"]:
        conn.close()
        abort(403)

    conn.execute(
        "UPDATE appointments SET status = 'Cancelled' WHERE id = ?",
        (appt_id,)
    )
    conn.commit()
    conn.close()

    flash("Appointment cancelled.", "info")
    return redirect(url_for("patient_dashboard"))


# ==========================================================
# DOCTOR PORTAL
# ==========================================================
@app.route("/doctor/dashboard")
@login_required
@role_required("doctor")
def doctor_dashboard():
    user_id = session["user_id"]
    conn = get_db()

    appointments = conn.execute("""
        SELECT a.*, u.full_name AS patient_name
        FROM appointments a
        JOIN users u ON a.patient_id = u.id
        WHERE a.doctor_id = ?
        ORDER BY a.appointment_date DESC
    """, (user_id,)).fetchall()

    patients = conn.execute(
        "SELECT id, full_name FROM users WHERE role = 'patient'"
    ).fetchall()
    patient_names = {int(patient["id"]): patient["full_name"] for patient in patients}
    caseload_patient_ids = [
        int(row["patient_id"])
        for row in conn.execute(
            "SELECT DISTINCT patient_id FROM appointments "
            "WHERE doctor_id = ? AND status != 'Cancelled'",
            (user_id,),
        ).fetchall()
    ]
    conn.close()

    try:
        vitals = _run_neon(_list_vitals_for_patients(caseload_patient_ids))
        for reading in vitals:
            reading["patient_name"] = patient_names.get(
                int(reading["patient_id"]), "Patient"
            )
    except Exception as error:
        app.logger.error("Clinician vitals load failed (%s)", type(error).__name__)
        vitals = []
        flash("Remote monitoring is temporarily unavailable.", "warning")

    return render_template(
        "doctor_dashboard.html",
        appointments=[dict(a) for a in appointments],
        patients=[dict(p) for p in patients],
        vitals=vitals,
    )


@app.route("/doctor/vitals/<int:vital_id>/review", methods=["POST"])
@login_required
@role_required("doctor")
def review_vitals(vital_id):
    _validate_csrf_token()
    try:
        patient_id = _run_neon(_get_vital_patient_id(vital_id))
    except Exception as error:
        app.logger.error("Clinician vitals lookup failed (%s)", type(error).__name__)
        flash("The reading could not be reviewed right now.", "danger")
        return redirect(url_for("doctor_dashboard"))

    if patient_id is None:
        flash("This reading is no longer available.", "warning")
        return redirect(url_for("doctor_dashboard"))

    conn = get_db()
    authorized = conn.execute(
        "SELECT 1 FROM appointments "
        "WHERE doctor_id = ? AND patient_id = ? AND status != 'Cancelled' LIMIT 1",
        (session["user_id"], int(patient_id)),
    ).fetchone()
    conn.close()
    if not authorized:
        abort(403)

    try:
        updated = _run_neon(
            _mark_vital_reviewed(vital_id, session["user_id"])
        )
    except Exception as error:
        app.logger.error("Clinician vitals update failed (%s)", type(error).__name__)
        flash("The reading could not be reviewed right now.", "danger")
        return redirect(url_for("doctor_dashboard"))

    if updated:
        flash("Reading marked as reviewed.", "success")
    else:
        flash("This reading was already reviewed or is routine.", "info")
    return redirect(url_for("doctor_dashboard"))


@app.route("/doctor/add_record", methods=["POST"])
@login_required
@role_required("doctor")
def add_record():
    patient_id = request.form.get("patient_id")
    record_type = request.form.get("record_type")
    diagnosis = request.form.get("diagnosis")
    prescription = request.form.get("prescription", "")
    notes = request.form.get("notes", "")

    if not all([patient_id, record_type, diagnosis]):
        flash("Please fill in required fields.", "danger")
        return redirect(url_for("doctor_dashboard"))

    conn = get_db()
    conn.execute("""
        INSERT INTO health_records
        (patient_id, doctor_id, record_type, diagnosis, prescription, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (patient_id, session["user_id"], record_type,
          diagnosis, prescription, notes))
    conn.commit()
    conn.close()

    flash("Health record saved.", "success")
    return redirect(url_for("doctor_dashboard"))


@app.route("/doctor/complete/<int:appt_id>")
@login_required
@role_required("doctor")
def complete_appointment(appt_id):
    conn = get_db()
    appt = conn.execute(
        "SELECT * FROM appointments WHERE id = ?", (appt_id,)
    ).fetchone()

    if not appt or appt["doctor_id"] != session["user_id"]:
        conn.close()
        abort(403)

    conn.execute(
        "UPDATE appointments SET status = 'Completed' WHERE id = ?",
        (appt_id,)
    )
    conn.commit()
    conn.close()

    flash("Appointment marked as completed.", "success")
    return redirect(url_for("doctor_dashboard"))


# ==========================================================
# TELEMEDICINE CONSULTATION
# ==========================================================
@app.route("/consultation/<int:appt_id>")
@login_required
def consultation(appt_id):
    conn = get_db()
    appt = conn.execute(
        "SELECT * FROM appointments WHERE id = ?", (appt_id,)
    ).fetchone()
    conn.close()

    if not appt:
        abort(404)

    if session["user_id"] not in [appt["patient_id"], appt["doctor_id"]]:
        abort(403)

    if appt["status"] != 'Scheduled':
        flash("This consultation is no longer active.", "danger")
        return redirect(url_for("dashboard"))

    return render_template("consultation.html", appointment=dict(appt))


# ==========================================================
# ERROR HANDLERS
# ==========================================================
@app.errorhandler(403)
def forbidden(e):
    flash("403 - Access Forbidden", "danger")
    return redirect(url_for("index"))


@app.errorhandler(404)
def not_found(e):
    flash("404 - Page Not Found", "danger")
    return redirect(url_for("index"))


# ==========================================================
# MAIN
# ==========================================================
if __name__ == "__main__":
    init_db()
    print("\n" + "=" * 55)
    print(" 🏥 MobiCare Platform")
    print("=" * 55)
    print(" 🌐 http://127.0.0.1:5000")
    print(" Supabase Auth handles new patient registration and login.")
    print(" Existing SQLite clinical records remain stored locally.")
    print("=" * 55 + "\n")
    app.run(debug=False, host="0.0.0.0", port=5000)
