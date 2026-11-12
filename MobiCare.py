"""
==========================================================
 compare - Telemedicine Platform
 Pure Python backend that connects to your existing HTML files
 Run: python MobiCare.py
==========================================================
"""

import os
import uuid
import sqlite3
from datetime import datetime
from functools import wraps

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
DATABASE = "mobicare.db"


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
            "role, phone) VALUES (?, ?, ?, ?, ?, ?, ?)",
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
    """Make `user` available in every template automatically."""
    return {"user": current_user()}


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

    return render_template(
        "patient_dashboard.html",
        appointments=[dict(a) for a in appointments],
        records=[dict(r) for r in records],
        doctors=[dict(d) for d in doctors]
    )


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
    conn.close()

    return render_template(
        "doctor_dashboard.html",
        appointments=[dict(a) for a in appointments],
        patients=[dict(p) for p in patients]
    )


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
