import os
import re
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import MobiCare


class AuthFlowTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_database = MobiCare.DATABASE
        MobiCare.DATABASE = os.path.join(self.temp_dir.name, "test.db")
        MobiCare.init_db()
        MobiCare.app.config.update(TESTING=True, WTF_CSRF_ENABLED=False)
        self.client = MobiCare.app.test_client()
        self.auth_user = {
            "id": "7da14c1a-bfd8-4a92-a6f1-d5c29a834103",
            "email": "patient@example.org",
            "email_confirmed_at": "2026-01-01T00:00:00Z",
            "user_metadata": {"username": "patient_one"},
        }
        self.profile = {
            "id": self.auth_user["id"],
            "full_name": "Test Patient",
            "phone": "5550100",
            "role": "patient",
        }

    def tearDown(self):
        MobiCare.DATABASE = self.original_database
        self.temp_dir.cleanup()

    def _csrf(self, path):
        response = self.client.get(path)
        match = re.search(rb'name="csrf_token" value="([^"]+)"', response.data)
        self.assertIsNotNone(match)
        return match.group(1).decode()

    def test_session_cookie_is_available_in_embedded_preview(self):
        response = self.client.get("/register")
        cookie = response.headers.get("Set-Cookie", "")
        self.assertIn("SameSite=None", cookie)
        self.assertIn("Secure", cookie)
        self.assertIn("HttpOnly", cookie)

    @patch("MobiCare.supabase_client.sign_up")
    def test_registration_always_creates_patient_role(self, sign_up):
        sign_up.return_value = {"user": self.auth_user}
        token = self._csrf("/register")
        response = self.client.post("/register", data={
            "csrf_token": token,
            "username": "patient_one",
            "email": "patient@example.org",
            "password": "a-strong-test-password",
            "confirm_password": "a-strong-test-password",
            "full_name": "Test Patient",
            "phone": "5550100",
            "role": "admin",
        })
        self.assertEqual(response.status_code, 302)
        self.assertEqual(sign_up.call_args.args[2], {
            "full_name": "Test Patient",
            "phone": "5550100",
            "username": "patient_one",
        })
        conn = sqlite3.connect(MobiCare.DATABASE)
        row = conn.execute("SELECT role, auth_user_id FROM users WHERE email = ?", ("patient@example.org",)).fetchone()
        conn.close()
        self.assertEqual(row, ("patient", self.auth_user["id"]))

    @patch("MobiCare.supabase_client.get_profile")
    @patch("MobiCare.supabase_client.sign_in")
    def test_login_uses_supabase_profile_and_logout_revokes_session(self, sign_in, get_profile):
        sign_in.return_value = {
            "access_token": "access-test-token",
            "refresh_token": "refresh-test-token",
            "expires_in": 3600,
            "user": self.auth_user,
        }
        get_profile.return_value = self.profile
        signup_token = self._csrf("/register")
        self.client.post("/register", data={
            "csrf_token": signup_token,
            "username": "patient_one",
            "email": "patient@example.org",
            "password": "a-strong-test-password",
            "confirm_password": "a-strong-test-password",
            "full_name": "Test Patient",
            "phone": "5550100",
        })

        login_token = self._csrf("/login")
        response = self.client.post("/login", data={
            "csrf_token": login_token,
            "email": "patient@example.org",
            "password": "a-strong-test-password",
        })
        self.assertEqual(response.status_code, 302)
        with patch("MobiCare.supabase_client.get_user", return_value=self.auth_user), patch(
            "MobiCare.supabase_client.get_profile", return_value=self.profile
        ):
            dashboard = self.client.get("/dashboard")
        self.assertEqual(dashboard.status_code, 302)
        self.assertIn("/patient/dashboard", dashboard.headers["Location"])

        with self.client.session_transaction() as flask_session:
            logout_token = flask_session["csrf_token"]
        with patch("MobiCare.supabase_client.get_user", return_value=self.auth_user), patch(
            "MobiCare.supabase_client.get_profile", return_value=self.profile
        ), patch("MobiCare.supabase_client.sign_out") as sign_out:
            logout = self.client.post("/logout", data={"csrf_token": logout_token})
        self.assertEqual(logout.status_code, 302)
        sign_out.assert_called_once_with("access-test-token")
        conn = sqlite3.connect(MobiCare.DATABASE)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM auth_sessions").fetchone()[0], 0)
        conn.close()

    def test_database_initialization_preserves_existing_clinical_rows(self):
        conn = sqlite3.connect(MobiCare.DATABASE)
        conn.executemany(
            "INSERT INTO users (id, username, email, password_hash, full_name, role) VALUES (?, ?, ?, ?, ?, ?)",
            [
                (11, "legacy_patient", "legacy-patient@example.org", "hash-a", "Legacy Patient", "patient"),
                (12, "legacy_doctor", "legacy-doctor@example.org", "hash-b", "Legacy Doctor", "doctor"),
            ],
        )
        conn.execute(
            "INSERT INTO appointments (patient_id, doctor_id, appointment_date, reason) VALUES (11, 12, '2026-11-19', 'follow up')"
        )
        conn.execute(
            "INSERT INTO health_records (patient_id, doctor_id, record_type, diagnosis) VALUES (11, 12, 'visit', 'retained diagnosis')"
        )
        conn.commit()
        conn.close()

        MobiCare.init_db()
        conn = sqlite3.connect(MobiCare.DATABASE)
        self.assertEqual(conn.execute("SELECT count(*) FROM users").fetchone()[0], 2)
        self.assertEqual(conn.execute("SELECT count(*) FROM appointments").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT count(*) FROM health_records").fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT role FROM users WHERE id = 12").fetchone()[0], "doctor")
        conn.close()

    def test_legacy_role_mismatch_cannot_become_a_self_assigned_role(self):
        conn = sqlite3.connect(MobiCare.DATABASE)
        conn.execute(
            "INSERT INTO users (id, username, email, password_hash, full_name, role) VALUES (?, ?, ?, ?, ?, ?)",
            (42, "old_doctor", "doctor@example.org", "retained-hash", "Old Doctor", "doctor"),
        )
        conn.commit()
        conn.close()
        auth_user = {
            **self.auth_user,
            "id": "3c2dc37d-583d-4620-9bee-9d437db10fb0",
            "email": "doctor@example.org",
        }
        patient_profile = {**self.profile, "id": auth_user["id"], "role": "patient"}
        with self.assertRaises(PermissionError):
            MobiCare._resolve_local_user(auth_user, patient_profile)
        conn = sqlite3.connect(MobiCare.DATABASE)
        row = conn.execute("SELECT id, role, auth_user_id FROM users WHERE id = 42").fetchone()
        conn.close()
        self.assertEqual(row, (42, "doctor", None))


if __name__ == "__main__":
    unittest.main()
