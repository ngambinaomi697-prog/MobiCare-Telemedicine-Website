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
import sqlite3
import threading
import uuid
from datetime import datetime
from functools import wraps

import asyncpg

from flask import (
    Flask, render_template, request, redirect,
    url_for, session, flash, abort
)
from werkzeug.security import generate_password_hash, check_password_hash

# ==========================================================
# APP CONFIGURATION
# ==========================================================
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32)
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
    conn.commit()

    # Seed demo users
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    if cur.fetchone()[0] == 0:
        cur.execute(
            "INSERT INTO users (username, email, password_hash, full_name, "
            "role, phone, specialty) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("dr_sande", "sandee@mobicare.com",
             generate_password_hash("doctor101"),
             "Dr. Micheal  Sande", "doctor", "+2650998601426",
             "General Medicine")
        )
        cur.execute(
            "INSERT INTO users (username, email, password_hash, full_name, "
            "role, phone) VALUES (?, ?, ?, ?, ?, ?)",
            ("Chisomo_Banda", "banda@mobicare.com",
             generate_password_hash("patient123"),
             "Chisomo Banda", "patient", "+2650998601427")
        )
        conn.commit()
        print("✅ Demo users created:")
        print("   Doctor : dr_sande / doctor101")
        print("   Patient: Chisomo_Banda / patient123")

    conn.close()


# ==========================================================
# AUTH DECORATORS & HELPERS
# ==========================================================
def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            flash("Please log in to continue.", "warning")
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


def role_required(role):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            if session.get("role") != role:
                flash("Access denied.", "danger")
                return redirect(url_for("dashboard"))
            return f(*args, **kwargs)
        return wrapper
    return decorator


def current_user():
    """Return current logged-in user as a dict (or None)."""
    if "user_id" not in session:
        return None
    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE id = ?", (session["user_id"],)
    ).fetchone()
    conn.close()
    return dict(user) if user else None


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
    if "user_id" in session:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm_password", "")
        full_name = request.form.get("full_name", "").strip()
        role = request.form.get("role", "patient")
        phone = request.form.get("phone", "").strip()
        specialty = request.form.get("specialty", "").strip()

        if not all([username, email, password, full_name, role]):
            flash("Please fill in all required fields.", "danger")
            return redirect(url_for("register"))

        if password != confirm:
            flash("Passwords do not match.", "danger")
            return redirect(url_for("register"))

        if len(password) < 6:
            flash("Password must be at least 6 characters.", "danger")
            return redirect(url_for("register"))

        conn = get_db()
        existing = conn.execute(
            "SELECT id FROM users WHERE username = ? OR email = ?",
            (username, email)
        ).fetchone()
        if existing:
            conn.close()
            flash("Username or email already exists.", "danger")
            return redirect(url_for("register"))

        conn.execute(
            "INSERT INTO users (username, email, password_hash, full_name, "
            "role, phone, specialty) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (username, email, generate_password_hash(password),
             full_name, role, phone,
             specialty if role == "doctor" else None)
        )
        conn.commit()
        conn.close()

        flash("Account created successfully! Please log in.", "success")
        return redirect(url_for("login"))

    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if "user_id" in session:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        conn = get_db()
        user = conn.execute(
            "SELECT * FROM users WHERE username = ?", (username,)
        ).fetchone()
        conn.close()

        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["role"] = user["role"]
            session["full_name"] = user["full_name"]
            flash(f"Welcome back, {user['full_name']}!", "success")
            return redirect(url_for("dashboard"))

        flash("Invalid username or password.", "danger")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for("index"))


@app.route("/dashboard")
@login_required
def dashboard():
    if session.get("role") == "doctor":
        return redirect(url_for("doctor_dashboard"))
    return redirect(url_for("patient_dashboard"))


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
    print(" 👤 Patient : Chisomo_Banda / patient123")
    print(" 🩺 Doctor  : dr_sande / doctor101")
    print("=" * 55 + "\n")
    app.run(debug=False, host="0.0.0.0", port=5000)
