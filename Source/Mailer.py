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
import sys
import socket
import argparse

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
WHITELIST_MODE = os.getenv("WHITELIST_MODE", "false").lower() == "true"

DB_PATH = (BASE_DIR / os.getenv("DB_PATH", "metacrawler.db")).resolve()
print("USING DB:", DB_PATH)

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)
HELO_DOMAIN = os.getenv("HELO_DOMAIN", "presspassla.com")  # For whitelist identification

rp = Path(os.getenv("RESUME_PATH", "resume.pdf"))
RESUME_PATH = rp if rp.is_absolute() else (BASE_DIR / rp).resolve()

SEND_DELAY = float(os.getenv("SEND_DELAY_SECONDS"))
MAX_PER_RUN = int(os.getenv("MAX_EMAILS_PER_RUN"))


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
        SELECT c.id, c.email, c.name, c.confidence, c.type, 
               COALESCE(co.domain, 'Unknown') as domain,
               COALESCE(co.category, 'Open') as category,
               c.last_error
        FROM contacts c
        LEFT JOIN companies co ON co.id = c.company_id
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
    cat = category or "any"

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
FLE

GitHub: github.com/f100001e
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


def test_smtp_connection(server):
    """Test if SMTP connection is still alive"""
    try:
        status = server.noop()[0]
        return status == 250
    except:
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
            if not SMTP_HOST:
                raise RuntimeError("Missing SMTP_HOST env var")
            if not FROM_EMAIL:
                raise RuntimeError("Missing FROM_EMAIL env var")

            print(f"🔌 Connecting to {SMTP_HOST}:{SMTP_PORT}...")
            print(f"   Mode: {'IP Whitelist' if WHITELIST_MODE else 'SMTP Auth'}")
            print(f"   HELO Domain: {HELO_DOMAIN}")

            ctx = ssl.create_default_context()

            # CHECK IF IN GOOGLE ADMIN MODE
            import sys
            if '--google-admin' in sys.argv:
                # Force IPv4 connection
                import socket
                print("⚡ Google Admin mode: Forcing IPv4...")
                addrinfos = socket.getaddrinfo(
                    SMTP_HOST, SMTP_PORT,
                    socket.AF_INET,  # IPv4 only
                    socket.SOCK_STREAM,
                    socket.IPPROTO_TCP
                )

                if not addrinfos:
                    raise socket.gaierror(f"No IPv4 addresses found for {SMTP_HOST}")

                ip, port = addrinfos[0][4][0], addrinfos[0][4][1]
                print(f"   Connecting via IPv4: {ip}:{port}")

                # Create custom SSL context for IP connection
                ipv4_ctx = ssl.create_default_context()
                ipv4_ctx.check_hostname = False  # Allow IP connection
                ipv4_ctx.verify_mode = ssl.CERT_NONE  # Skip cert verification

                server = smtplib.SMTP(ip, port, timeout=30)

                # Always identify with HELO domain
                server.ehlo(HELO_DOMAIN)
                server.starttls(context=ipv4_ctx)  # Use custom context
                server.ehlo(HELO_DOMAIN)  # Again after STARTTLS

            else:
                # Normal connection
                server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)

                # Always identify with HELO domain
                server.ehlo(HELO_DOMAIN)
                server.starttls(context=ctx)
                server.ehlo(HELO_DOMAIN)  # Again after STARTTLS

            # Only authenticate if NOT in whitelist mode
            if not WHITELIST_MODE:
                if SMTP_USER and SMTP_PASS:
                    print(f"🔐 Authenticating as {SMTP_USER}...")
                    server.login(SMTP_USER, SMTP_PASS)
                    print("✅ Authentication successful")
                else:
                    print("⚠ No credentials provided for auth mode. Assuming IP whitelist.")
            else:
                print("✅ Using IP whitelist (no authentication)")

        try:
            sent = 0
            failed = 0
            for i, r in enumerate(rows, 1):
                contact_id, to_email, name, confidence, email_type, domain, category, last_error = r

                subject = f"Application: {(category or 'Open')} roles"
                body = default_body(domain, category, name=name, email_type=email_type)

                try:
                    msg = build_message(to_email, subject, body)

                    if DRY_RUN:
                        print(f"[DRY RUN {i}/{len(rows)}] Would send -> {to_email} ({domain})")
                    else:
                        # Check connection before sending
                        if not test_smtp_connection(server):
                            print("⚠ Connection lost, reconnecting...")
                            server.ehlo(HELO_DOMAIN)

                        server.send_message(msg)

                    mark_sent(conn, contact_id)
                    conn.commit()
                    sent += 1
                    print(f"✅ Sent [{sent}/{len(rows)}] -> {to_email}")

                except Exception as e:
                    dismiss_failed(conn, contact_id, str(e))
                    conn.commit()
                    print(f"❌ Failed -> {to_email}: {e}")
                    failed += 1

                # Add delay between emails
                if i < len(rows):  # Don't wait after the last one
                    print(f"⏳ Waiting {SEND_DELAY} seconds...")
                    time.sleep(SEND_DELAY)

        finally:
            if server is not None:
                server.quit()
                print("🔌 SMTP connection closed")

        print(f"\n{'=' * 40}")
        print(f"📊 SUMMARY")
        print(f"{'=' * 40}")
        print(f"✅ Sent: {sent}")
        print(f"❌ Failed: {failed}")
        print(f"📋 Total: {sent + failed}/{len(rows)}")
        if sent + failed < len(rows):
            print(f"⚠ Skipped: {len(rows) - (sent + failed)}")


# ===== GOOGLE ADMIN IPv4 OPTION =====

def run_google_admin_ipv4():
    """
    Run mailer with Google Admin IPv4-only connection
    This is a specialized mode that forces IPv4 for IP whitelist auth
    """
    print("\n" + "=" * 60)
    print("GOOGLE ADMIN IPv4-ONLY MAILER")
    print("=" * 60)

    # Force IPv4 resolution
    import socket

    host = "smtp-relay.gmail.com"
    port = 587

    print("🔌 Forcing IPv4 connection for Google Admin...")

    # Resolve hostname to IPv4 only
    addrinfos = socket.getaddrinfo(
        host, port,
        socket.AF_INET,  # IPv4 only
        socket.SOCK_STREAM,
        socket.IPPROTO_TCP
    )

    if not addrinfos:
        raise socket.gaierror(f"No IPv4 addresses found for {host}")

    # Get first IPv4 address
    ip, port = addrinfos[0][4][0], addrinfos[0][4][1]
    print(f"   Resolved to IPv4: {ip}:{port}")

    # Override SMTP_HOST with IPv4 address
    import os
    os.environ["SMTP_HOST"] = ip  # Temporarily override

    # Now run the normal mailer but with IPv4 host
    run_mailer()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--google-admin":
        # Run in Google Admin IPv4 mode
        run_google_admin_ipv4()
    else:
        # Run normal mailer
        run_mailer()

def send_test_email(test_email):
    """Send a single test email using forced IPv4 to check deliverability"""
    print(f"\n{'=' * 60}")
    print(f"📧 TEST MODE: Sending single test email")
    print(f"{'=' * 60}")
    print(f"To: {test_email}")
    print(f"From: {FROM_EMAIL}")
    print(f"Resume: {RESUME_PATH}")
    print(f"{'=' * 60}\n")

    if not RESUME_PATH.exists():
        raise FileNotFoundError(f"Resume not found: {RESUME_PATH}")

    # Force IPv4 for test mode (Google Admin style)
    print("🔌 Forcing IPv4 connection for test email...")

    # Check if we're in Google Admin mode (--google-admin flag)
    google_admin_mode = '--google-admin' in sys.argv

    if google_admin_mode:
        # Use Google Admin IPv4 connection
        host = SMTP_HOST
        port = SMTP_PORT

        # Resolve to IPv4 only
        addrinfos = socket.getaddrinfo(
            host, port,
            socket.AF_INET,  # IPv4 only
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP
        )

        if not addrinfos:
            raise socket.gaierror(f"No IPv4 addresses found for {host}")

        ip, port = addrinfos[0][4][0], addrinfos[0][4][1]
        print(f"   Resolved to IPv4: {ip}:{port}")

        # Create custom SSL context for IP connection
        ipv4_ctx = ssl.create_default_context()
        ipv4_ctx.check_hostname = False
        ipv4_ctx.verify_mode = ssl.CERT_NONE

        server = smtplib.SMTP(ip, port, timeout=30)
        server.ehlo(HELO_DOMAIN)
        server.starttls(context=ipv4_ctx)
        server.ehlo(HELO_DOMAIN)
    else:
        # Normal connection
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        server.ehlo(HELO_DOMAIN)
        server.starttls(context=ssl.create_default_context())
        server.ehlo(HELO_DOMAIN)

    # Authenticate if needed
    if not WHITELIST_MODE and SMTP_USER and SMTP_PASS:
        print(f"🔐 Authenticating as {SMTP_USER}...")
        server.login(SMTP_USER, SMTP_PASS)

    # Build test message
    subject = "TEST: Resume Application - Please Confirm Delivery"
    body = f"""Hello,

This is a TEST email from the Press Pass LA job application system.

TO: {test_email}
TIME: {utc_now_iso()}
MODE: {'IPv4 Forced' if google_admin_mode else 'Normal'}

This test confirms that:
✅ SMTP connection is working
✅ Resume attachment is properly formatted
✅ Email can be delivered to this address

If you received this, please reply with "RECEIVED" so we can confirm deliverability.

Thank you,
FLE
Press Pass LA
"""

    try:
        msg = build_message(test_email, subject, body)
        server.send_message(msg)
        print(f"\n✅ TEST EMAIL SENT SUCCESSFULLY!")
        print(f"   To: {test_email}")
        print(f"   Mode: {'IPv4 Forced' if google_admin_mode else 'Normal'}")
        print(f"\n📌 IMPORTANT:")
        print(f"   1. Check your spam folder")
        print(f"   2. If found in spam, mark as 'Not Spam'")
        print(f"   3. Add {FROM_EMAIL} to your contacts")
        print(f"   4. Reply to confirm receipt")
    except Exception as e:
        print(f"\n❌ TEST EMAIL FAILED: {e}")
        raise
    finally:
        server.quit()
        print("🔌 SMTP connection closed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Job Application Mailer')
    parser.add_argument('--google-admin', action='store_true', help='Use Google Admin IPv4 mode')
    parser.add_argument('--test-send', type=str, help='Send a single test email to the specified address')

    args = parser.parse_args()

    if args.test_send:
        send_test_email(args.test_send)
    elif args.google_admin:
        run_google_admin_ipv4() 
    else:
        run_mailer()