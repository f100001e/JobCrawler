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
import csv

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
CC_EMAIL = os.getenv("CC_EMAIL", "").strip()
HELO_DOMAIN = os.getenv("HELO_DOMAIN", "presspassla.com")  # For whitelist identification

rp = Path(os.getenv("RESUME_PATH", "resume.pdf"))
RESUME_PATH = rp if rp.is_absolute() else (BASE_DIR / rp).resolve()

MAX_EMAILS_PER_RUN = int(os.getenv("MAX_EMAILS_PER_RUN", "100"))
SEND_DELAY_SECONDS = float(os.getenv("SEND_DELAY_SECONDS", "2.0"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_message(to_email: str, subject: str, body: str, cc: bool = False) -> EmailMessage:
    msg = EmailMessage()
    msg["From"] = FROM_EMAIL
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    if cc and CC_EMAIL:
        msg["Cc"] = CC_EMAIL
    
    if CAMPAIGN == "job" and RESUME_PATH.is_file():
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
    cur.execute("""
        SELECT c.id, c.email, c.name, c.confidence, c.type, 
               COALESCE(co.domain, 'Unknown') as domain,
               'Open' as category,
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

CAMPAIGN = os.getenv("CAMPAIGN", "job").lower()

def build_body(first: str, domain: str, cat: str = "open") -> str:
    greeting = f"Hi {first}," if first else "Hi,"
    
    if CAMPAIGN == "ppla":
        return f"""{greeting}

Press Pass LA is celebrating 15 years of entertainment coverage, and we're marking the milestone with a special Hollywood event on November 10, from 6–9 PM, at White Rabbit Sky Lounge.

The evening will bring together entertainment industry professionals, VIPs, and talent, alongside the unveiling of our Winter 2027 print issue cover and a preview of our new content studio.

We're currently exploring brand partnerships and sponsorship opportunities for the celebration and welcome introductions to businesses interested in participating. Referral compensation may be available for successful sponsorship introductions.

For partnership details or to connect us with a potential sponsor, please contact Jennifer@presspassla.com.

Best,
Jennifer Buonantony
Founder & CEO, Press Pass LA
presspassla.com

"""
    else:
        return f"""{greeting}

I'm reaching out regarding {cat} roles at {domain}.

Looking to speak to whomever handles the hiring, otherwise disregard.

Best,
FLE

GitHub: github.com/f100001e
LinkedIn: linkedin.com/in/frank-l-elliott
Columns: sxhx.news
"""
    
def default_body(domain: str, category: str | None, name: str | None = None, email_type: str | None = None) -> str:
    first = None
    if (email_type or "").lower() != "generic":
        if name and name.strip() and name.strip().upper() != "N/A":
            first = name.strip().split()[0]
    return build_body(first, domain)

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

def get_one_contact_per_domain(conn) -> list:
    """Pick highest priority contact per domain"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT c.id, c.email, c.name, co.domain, co.organization
        FROM contacts c
        JOIN companies co ON c.company_id = co.id
        WHERE c.contacted = 0
        ORDER BY co.domain, c.confidence DESC
    """)
    rows = cursor.fetchall()
    seen_domains = set()
    selected = []
    for row in rows:
        domain = row[3]
        if domain not in seen_domains:
            seen_domains.add(domain)
            selected.append(row)
    return selected

def run_mailer(force_ipv4=False):
    # Check database first
    if not check_and_import_json_if_empty():
        print("\nCannot proceed. Please run crawler.py first to populate database.")
        return

    if CAMPAIGN == "job" and not RESUME_PATH.is_file():
        raise FileNotFoundError(f"Resume not found: {RESUME_PATH}")

    with sqlite3.connect(DB_PATH) as conn:
        rows = get_one_contact_per_domain(conn)[:MAX_EMAILS_PER_RUN]
        if not rows:
            print("No pending contacts to email.")
            return

        if DRY_RUN:
            print(f"[DRY RUN] Previewing {len(rows)} emails.")

            for i, row in enumerate(rows, 1):
                contact_id, to_email, name, domain, organization = row

                print(
                    f"[DRY RUN {i}/{len(rows)}] "
                    f"Would send -> {to_email} ({domain})"
                )

            print("No emails sent. Database unchanged.")
            return
        else:
            if not SMTP_HOST:
                raise RuntimeError("Missing SMTP_HOST env var")
            if not FROM_EMAIL:
                raise RuntimeError("Missing FROM_EMAIL env var")

            print(f"🔌 Connecting to {SMTP_HOST}:{SMTP_PORT}...")
            print(f"   Mode: {'IP Whitelist' if WHITELIST_MODE else 'SMTP Auth'}")
            print(f"   HELO Domain: {HELO_DOMAIN}")

        ctx = ssl.create_default_context()

        # Select encryption based on port
        if SMTP_PORT == 465:
            # Implicit TLS — Titan
            server = smtplib.SMTP_SSL(
                SMTP_HOST,
                SMTP_PORT,
                timeout=30,
                context=ctx
            )
            server.ehlo(HELO_DOMAIN)

        else:
            # STARTTLS — Google relay on port 587
            if force_ipv4:
                if SMTP_PORT != 587:
                    raise ValueError("Forced IPv4 mode requires port 587")

                addrinfos = socket.getaddrinfo(
                    SMTP_HOST,
                    SMTP_PORT,
                    socket.AF_INET,
                    socket.SOCK_STREAM,
                    socket.IPPROTO_TCP
                )

                ip, port = addrinfos[0][4]
                print(f"🔌 Connecting via IPv4: {ip}:{port}")

                server = smtplib.SMTP(ip, port, timeout=30)
                server._host = SMTP_HOST
            else:
                server = smtplib.SMTP(
                    SMTP_HOST,
                    SMTP_PORT,
                    timeout=30
                )

            server.ehlo(HELO_DOMAIN)
            server.starttls(context=ctx)
            server.ehlo(HELO_DOMAIN)

        # Select authentication mode
        if WHITELIST_MODE:
            print("✅ Using IP whitelist — SMTP login skipped")
        else:
            if not SMTP_USER or not SMTP_PASS:
                raise RuntimeError(
                    "SMTP username/password required in authentication mode"
                )

            print(f"🔐 Authenticating as {SMTP_USER}...")
            server.login(SMTP_USER, SMTP_PASS)
            print("✅ Authentication successful")

        try:
            sent = 0
            failed = 0
            sent_rows = []
            for i, r in enumerate(rows, 1):
                contact_id, to_email, name, domain, organization = r
                email_type = None  # not available from this query
                category = "Open"
                if CAMPAIGN == "ppla":
                    subject = "PPLA Social + PR — Introduction"
                else:
                    subject = f"{category} roles at {domain}"
                body = default_body(domain, category, name=name, email_type=email_type)

                try:
                    msg = build_message(to_email, subject, body, cc=(i == 1))

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
                    sent_rows.append([to_email, name, domain, organization, utc_now_iso()])
                    print(f"✅ Sent [{sent}/{len(rows)}] -> {to_email}")

                except Exception as e:
                    dismiss_failed(conn, contact_id, str(e))
                    conn.commit()
                    print(f"❌ Failed -> {to_email}: {e}")
                    failed += 1

                # Add delay between emails
                if i < len(rows):  # Don't wait after the last one
                    print(f"⏳ Waiting {SEND_DELAY_SECONDS} seconds...")
                    time.sleep(SEND_DELAY_SECONDS)

        finally:
            if server is not None:
                try:
                    server.quit()
                except (smtplib.SMTPServerDisconnected, OSError):
                    server.close()
                print("🔌 SMTP connection closed")

        print(f"\n{'=' * 40}")
        print(f"📊 SUMMARY")
        print(f"{'=' * 40}")
        print(f"✅ Sent: {sent}")
        print(f"❌ Failed: {failed}")
        print(f"📋 Total: {sent + failed}/{len(rows)}")
        if sent + failed < len(rows):
            print(f"⚠ Skipped: {len(rows) - (sent + failed)}")

        if sent > 0:
            csv_file = BASE_DIR / f"sent_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['email', 'name', 'domain', 'organization', 'sent_at'])
                for row in sent_rows:
                    writer.writerow(row)
            print(f"📄 CSV saved: {csv_file.name}")


# ===== GOOGLE ADMIN IPv4 OPTION =====

def run_google_admin_ipv4():
    print("\n" + "=" * 60)
    print("GOOGLE ADMIN IPv4-ONLY MAILER")
    print("=" * 60)

    run_mailer(force_ipv4=True)


def send_test_email(test_email):
    """Send a single test email using the configured SMTP provider."""

    print(f"\n{'=' * 60}")
    print("📧 TEST MODE: Sending single test email")
    print(f"{'=' * 60}")
    print(f"To: {test_email}")
    print(f"From: {FROM_EMAIL}")
    print(f"Resume: {RESUME_PATH}")
    print(f"{'=' * 60}\n")
    print("ENV file:", BASE_DIR / ".env")
    print("SMTP host:", SMTP_HOST)
    print("SMTP port:", SMTP_PORT)
    print("Whitelist mode:", WHITELIST_MODE)
    print("HELO domain:", HELO_DOMAIN)

    if CAMPAIGN == "job" and not RESUME_PATH.is_file():
     raise FileNotFoundError(f"Resume not found: {RESUME_PATH}")

    if not SMTP_HOST:
        raise RuntimeError("Missing SMTP_HOST env var")

    tls_context = ssl.create_default_context()

    google_admin_mode = "--google-admin" in sys.argv

    if google_admin_mode:
        if SMTP_PORT != 587:
            raise RuntimeError("Forced IPv4 Google relay requires port 587")

        addrinfos = socket.getaddrinfo(
            SMTP_HOST,
            SMTP_PORT,
            socket.AF_INET,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP
        )

        if not addrinfos:
            raise socket.gaierror(
                f"No IPv4 addresses found for {SMTP_HOST}"
            )

        ipv4_host, ipv4_port = addrinfos[0][4]
        print(f"🔌 Connecting via IPv4: {ipv4_host}:{ipv4_port}")

        server = smtplib.SMTP(ipv4_host, ipv4_port, timeout=30)

        # Use the real hostname for secure certificate verification
        server._host = SMTP_HOST

        server.ehlo(HELO_DOMAIN)
        server.starttls(context=tls_context)
        server.ehlo(HELO_DOMAIN)

    elif SMTP_PORT == 465:
        print("🔌 Connecting with implicit TLS...")
        server = smtplib.SMTP_SSL(
            SMTP_HOST,
            SMTP_PORT,
            timeout=30,
            context=tls_context
        )
        server.ehlo(HELO_DOMAIN)

    else:
        print("🔌 Connecting with STARTTLS...")
        server = smtplib.SMTP(
            SMTP_HOST,
            SMTP_PORT,
            timeout=30
        )
        server.ehlo(HELO_DOMAIN)
        server.starttls(context=tls_context)
        server.ehlo(HELO_DOMAIN)

    if WHITELIST_MODE:
        print("✅ Using IP whitelist — SMTP login skipped")
    else:
        if not SMTP_USER or not SMTP_PASS:
            server.quit()
            raise RuntimeError(
                "SMTP username/password required in authentication mode"
            )

        print(f"🔐 Authenticating as {SMTP_USER}...")
        server.login(SMTP_USER, SMTP_PASS)
        print("✅ Authentication successful")

    # Build test message
    name = "Test User"
    domain = "example.com"
    organization = "Test Organization"
    email_type = "test"
    category = "Tech"

    if CAMPAIGN == "ppla":
        subject = "PPLA Social + PR — Introduction"
    else:
        subject = f"{category} roles at {domain}"

    body = default_body(domain, category, name=name, email_type=email_type)

    try:
        msg = build_message(test_email, subject, body)
        
        server.send_message(msg)
        print(f"\n✅ TEST EMAIL SENT SUCCESSFULLY!")
        print(f"   To: {test_email}")
        print(f"   Mode: {'Whitelist' if WHITELIST_MODE else 'Authenticated'}")
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

PERSONAL_EMAIL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
}


def csv_value(row, *names):
    """Return first non-empty matching CSV column."""
    for name in names:
        value = row.get(name)
        if value and value.strip():
            return value.strip()
    return ""

def connect_csv_smtp(google_admin=False):
    ctx = ssl.create_default_context()

    if google_admin:
        host = "smtp-relay.gmail.com"
        port = 587

        addrinfos = socket.getaddrinfo(
            host,
            port,
            socket.AF_INET,
            socket.SOCK_STREAM,
            socket.IPPROTO_TCP
        )

        if not addrinfos:
            raise socket.gaierror(f"No IPv4 addresses found for {host}")

        ip, port = addrinfos[0][4]

        print(f"🔌 Google Admin IPv4: {ip}:{port}")

        server = smtplib.SMTP(ip, port, timeout=30)

        # Certificate still validates against Google's hostname
        server._host = host

        server.ehlo(HELO_DOMAIN)
        server.starttls(context=ctx)
        server.ehlo(HELO_DOMAIN)

    elif SMTP_PORT == 465:

        print(f"🔌 Normal SMTP SSL: {SMTP_HOST}:{SMTP_PORT}")

        server = smtplib.SMTP_SSL(
            SMTP_HOST,
            SMTP_PORT,
            timeout=30,
            context=ctx
        )

        server.ehlo(HELO_DOMAIN)

    else:

        print(f"🔌 Normal SMTP STARTTLS: {SMTP_HOST}:{SMTP_PORT}")

        server = smtplib.SMTP(
            SMTP_HOST,
            SMTP_PORT,
            timeout=30
        )

        server.ehlo(HELO_DOMAIN)
        server.starttls(context=ctx)
        server.ehlo(HELO_DOMAIN)

    if WHITELIST_MODE:
        print("✅ Using IP whitelist")
    else:
        if not SMTP_USER or not SMTP_PASS:
            server.quit()
            raise RuntimeError("SMTP credentials required")

        server.login(SMTP_USER, SMTP_PASS)
        print("✅ SMTP authentication successful")

    return server

def load_csv_recipients(csv_path):
    """
    Load recipients directly from CSV.
    Nothing is written to the database.
    """

    recipients = []
    seen = set()

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Handles our own CSVs and Google Contacts-style exports
            email = csv_value(
                row,
                "email",
                "Email",
                "E-mail 1 - Value",
                "E-mail Address",
            )

            if not email or "@" not in email:
                continue

            email = email.strip()
            email_key = email.lower()

            # Deduplicate within this CSV
            if email_key in seen:
                continue

            seen.add(email_key)

            name = csv_value(
                row,
                "name",
                "Name",
                "Given Name",
            )

            company = csv_value(
                row,
                "company",
                "Company",
                "Organization",
                "Organization 1 - Name",
            )

            recipients.append((email, name, company))

    return recipients

def run_csv_mailer(csv_path, google_admin=False):
    csv_path = Path(csv_path).resolve()

    if not csv_path.exists():
        print(f"❌ CSV not found: {csv_path}")
        return

    recipients = load_csv_recipients(csv_path)

    if not recipients:
        print("❌ No valid email addresses found.")
        return

    recipients = recipients[:MAX_EMAILS_PER_RUN]

    print("\n" + "=" * 60)
    print("CSV MAILER")
    print("=" * 60)
    print(f"File: {csv_path.name}")
    print(f"Recipients: {len(recipients)}")
    print(f"Mode: {'Google Admin IPv4' if google_admin else 'Normal SMTP'}")
    print("=" * 60)

    if DRY_RUN:
        server = None
    else:
        server = connect_csv_smtp(google_admin)

    sent = 0
    failed = 0

    try:
        for i, (email, name, company) in enumerate(recipients, 1):

            domain = email.rsplit("@", 1)[-1].lower()

            # Never call someone's company "gmail.com"
            if company:
                target = company
            elif domain not in PERSONAL_EMAIL_DOMAINS:
                target = domain
            else:
                target = "your organization"

            if CAMPAIGN == "ppla":
                subject = "PPLA Social + PR — Introduction"
            else:
                subject = f"Open roles at {target}"

            body = default_body(
                target,
                "Open",
                name=name,
                email_type="personal"
            )

            try:
                msg = build_message(
                    email,
                    subject,
                    body,
                    cc=(i == 1)
                )

                if DRY_RUN:
                    print(f"[DRY RUN {i}/{len(recipients)}] {email}")
                else:
                    server.send_message(msg)
                    print(f"✅ Sent [{i}/{len(recipients)}] -> {email}")

                sent += 1

            except Exception as e:
                failed += 1
                print(f"❌ Failed -> {email}: {e}")

            if i < len(recipients):
                time.sleep(SEND_DELAY_SECONDS)

    finally:
        if server:
            server.quit()
            print("🔌 SMTP connection closed")

    print("\n" + "=" * 40)
    print("CSV MAILER SUMMARY")
    print("=" * 40)
    print(f"✅ Sent: {sent}")
    print(f"❌ Failed: {failed}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Job Application Mailer")

    parser.add_argument(
        "--google-admin",
        action="store_true",
        help="Use Google Admin IPv4 mode"
    )

    parser.add_argument(
        "--test-send",
        type=str,
        help="Send a single test email"
    )

    parser.add_argument(
        "--csv",
        type=str,
        help="Send directly to recipients from CSV"
    )

    args = parser.parse_args()

    if args.test_send:
        send_test_email(args.test_send)

    elif args.csv:
        run_csv_mailer(
            args.csv,
            google_admin=args.google_admin
        )

    elif args.google_admin:
        run_google_admin_ipv4()

    else:
        run_mailer()
