# Mailer.py
import os
import time
import sqlite3
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

DB_PATH = (BASE_DIR / os.getenv("DB_PATH", "metacrawler.db")).resolve()
print("USING DB:", DB_PATH)

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)

rp = Path(os.getenv("RESUME_PATH", "resume.pdf"))
RESUME_PATH = rp if rp.is_absolute() else (BASE_DIR / rp).resolve()

SEND_DELAY = float(os.getenv("SEND_DELAY_SECONDS", "45"))
MAX_PER_RUN = int(os.getenv("MAX_EMAILS_PER_RUN", "20"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_message(to_email: str, subject: str, body: str) -> EmailMessage:
    msg = EmailMessage()
    msg["From"] = FROM_EMAIL
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    if not RESUME_PATH.exists():
        raise FileNotFoundError(f"Resume not found: {RESUME_PATH}")

    data = RESUME_PATH.read_bytes()
    msg.add_attachment(
        data,
        maintype="application",
        subtype="pdf",
        filename=RESUME_PATH.name
    )
    return msg


def fetch_send_queue(conn: sqlite3.Connection, limit: int):
    cur = conn.cursor()

    # First, let's debug what's in the database
    cur.execute("SELECT COUNT(*) FROM contacts WHERE contacted = 0")
    pending_count = cur.fetchone()[0]
    print(f"DEBUG: Found {pending_count} pending contacts in database")

    cur.execute("SELECT COUNT(*) FROM companies")
    company_count = cur.fetchone()[0]
    print(f"DEBUG: Found {company_count} companies in database")

    # Show first few pending contacts
    cur.execute("""
        SELECT c.id, c.email, c.name, c.confidence, c.type, co.domain, co.category
        FROM contacts c
        LEFT JOIN companies co ON co.id = c.company_id
        WHERE c.contacted = 0
        LIMIT 5
    """)
    sample = cur.fetchall()
    print(f"DEBUG: Sample pending contacts: {sample}")

    # Now run the actual query
    cur.execute("""
        SELECT c.id, c.email, c.name, c.confidence, c.type, co.domain, co.category, c.last_error
        FROM contacts c
        JOIN companies co ON co.id = c.company_id
        WHERE c.contacted = 0
        ORDER BY c.confidence DESC NULLS LAST, c.id ASC
        LIMIT ?
    """, (limit,))
    return cur.fetchall()


def mark_sent(conn: sqlite3.Connection, contact_id: int):
    conn.execute("""
        UPDATE contacts
        SET contacted = 1, contacted_at = ?, last_error = NULL
        WHERE id = ?
    """, (utc_now_iso(), contact_id))


def dismiss_failed(conn: sqlite3.Connection, contact_id: int, err: str):
    conn.execute("""
        UPDATE contacts
        SET contacted = -1, contacted_at = ?, last_error = ?
        WHERE id = ?
    """, (utc_now_iso(), (err or "")[:500], contact_id))


def default_body(domain: str, category: str | None, name: str | None = None, email_type: str | None = None) -> str:
    cat = category or "engineering"

    # Don't personalize generic inboxes (jobs@, info@, etc.)
    first = None
    if (email_type or "").lower() != "generic":
        if name and name.strip() and name.strip().upper() != "N/A":
            first = name.strip().split()[0]

    greeting = f"Hello {first}," if first else "Hello,"
    return f"""{greeting}

I'm reaching out regarding {cat} roles at {domain}.

Resume attached. If there's a better contact or process, I'd appreciate a pointer.

Best,
NAME
"""


def check_and_import_json_if_empty():
    """Check if database is empty and import from JSON if needed"""
    print("Checking database status...")

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        # Check if we have any pending contacts
        cur.execute("SELECT COUNT(*) FROM contacts WHERE contacted = 0")
        pending = cur.fetchone()[0]

        if pending > 0:
            print(f"✅ Found {pending} pending contacts in database.")
            return True

        # No pending contacts, check if database is empty
        cur.execute("SELECT COUNT(*) FROM contacts")
        total = cur.fetchone()[0]

        if total == 0:
            print("⚠ Database is empty. Checking for JSON files...")

            # Look for JSON files
            json_files = list(BASE_DIR.glob("contacts*.json"))

            if json_files:
                # Sort by modification time (newest first)
                json_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
                latest_file = json_files[0]

                print(f"📥 Found JSON file: {latest_file.name}")
                print("Would you like to import contacts from JSON?")
                print("Add this function to mailer.py to enable auto-import.")
                print("For now, run crawler.py first to import JSON data.")
                return False
            else:
                print("❌ No JSON files found. Database is empty.")
                print("   Run crawler.py to collect companies and contacts.")
                return False
        else:
            print(f"⚠ Database has {total} contacts but all are already processed.")
            print("   All contacts have been contacted (contacted=1) or failed (contacted=-1).")
            print("   Run crawler.py to find more companies.")
            return False

def run_mailer():
    # Check database first
    if not check_and_import_json_if_empty():
        print("\nCannot proceed. Please run crawler.py first to populate database.")
        return

    if not RESUME_PATH.exists():
        raise FileNotFoundError(f"Resume not found: {RESUME_PATH}")

    with sqlite3.connect(DB_PATH) as conn:
        rows = fetch_send_queue(conn, MAX_PER_RUN)
        if not rows:
            print("No pending contacts to email.")
            return

        if DRY_RUN:
            server = None
            print(f"[DRY RUN] Would send up to {len(rows)} emails. No SMTP connection will be made.")
        else:
            if not (SMTP_HOST and SMTP_USER and SMTP_PASS and FROM_EMAIL):
                raise RuntimeError("Missing SMTP_* or FROM_EMAIL env vars")

            ctx = ssl.create_default_context()
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
            server.ehlo()
            server.starttls(context=ctx)
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASS)

        try:
            sent = 0
            failed = 0
            for r in rows:
                contact_id, to_email, name, confidence, email_type, domain, category, last_error = r

                subject = f"Application: {(category or 'engineering')} roles"
                body = default_body(domain, category, name=name, email_type=email_type)

                try:
                    msg = build_message(to_email, subject, body)

                    if DRY_RUN:
                        print(f"[DRY RUN] Would send -> {to_email} ({domain})")
                    else:
                        server.send_message(msg)

                    mark_sent(conn, contact_id)
                    conn.commit()
                    sent += 1
                    print(f"Sent [{sent}/{len(rows)}] -> {to_email}")

                except Exception as e:
                    dismiss_failed(conn, contact_id, str(e))
                    conn.commit()
                    print(f"Dismissed (failed) -> {to_email}: {e}")
                    failed += 1
                time.sleep(SEND_DELAY)

        finally:
            if server is not None:
                server.quit()

        print(f"\n=== Summary ===")
        print(f"Sent: {sent}")
        print(f"Failed: {failed}")
        print(f"Total processed: {sent + failed}")

if __name__ == "__main__":
    run_mailer()
