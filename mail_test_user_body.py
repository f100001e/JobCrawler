#!/usr/bin/env python3
"""
test_email_template.py - Test email formatting without personal info
Users should copy to test_email.py and fill in their own details
"""

import os
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Config - Users MUST set these in .env
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)

# Resume path
rp = Path(os.getenv("RESUME_PATH", "resume.pdf"))
RESUME_PATH = rp if rp.is_absolute() else (BASE_DIR / rp).resolve()

# Users should change this to their own email
TEST_EMAIL = os.getenv("TEST_EMAIL", "your-email@example.com")


def send_test_email():
    """Send a test email to yourself to verify formatting"""
    print("📧 Sending test email...")

    if not SMTP_HOST or not SMTP_USER:
        print("❌ Missing SMTP configuration in .env file")
        print("   Please set: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS")
        return False

    if TEST_EMAIL == "your-email@example.com":
        print("❌ Please set TEST_EMAIL in .env or edit this script")
        return False

    # Example company data (like what crawler finds)
    domain = "example-company.com"
    category = "software engineering"
    contact_name = "Hiring Manager"

    # Build email body (same logic as Mailer.py)
    cat = category or "engineering"

    # Determine greeting
    first_name = None
    if contact_name and contact_name.strip().upper() != "N/A":
        # Extract first name
        first_name = contact_name.strip().split()[0]

    greeting = f"Hello {first_name}," if first_name else "Hello,"

    body = f"""{greeting}

I'm reaching out regarding {cat} roles at {domain}.

Resume attached. If there's a better contact or process, I'd appreciate a pointer.

Best,
[Your Name Here]
"""

    subject = f"Test: Application for {cat} roles"

    print(f"\n📄 Email Preview:")
    print(f"   From: {FROM_EMAIL}")
    print(f"   To: {TEST_EMAIL}")
    print(f"   Subject: {subject}")
    print(f"   Body preview: {body[:100]}...")

    # Create email
    msg = EmailMessage()
    msg["From"] = FROM_EMAIL
    msg["To"] = TEST_EMAIL
    msg["Subject"] = subject
    msg.set_content(body)

    # Add resume if exists
    if RESUME_PATH.exists():
        try:
            data = RESUME_PATH.read_bytes()
            msg.add_attachment(
                data,
                maintype="application",
                subtype="pdf",
                filename=RESUME_PATH.name
            )
            print(f"   Attached: {RESUME_PATH.name}")
        except Exception as e:
            print(f"   Warning: Could not attach resume: {e}")
    else:
        print(f"   Note: No resume found at {RESUME_PATH}")

    # Send it
    try:
        print(f"\n🚀 Connecting to {SMTP_HOST}:{SMTP_PORT}...")
        ctx = ssl.create_default_context()

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        server.ehlo()
        server.starttls(context=ctx)
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)

        server.send_message(msg)
        server.quit()

        print(f"✅ Test email sent to {TEST_EMAIL}!")
        print("   Check your inbox to verify formatting.")
        return True

    except smtplib.SMTPAuthenticationError:
        print("❌ SMTP Authentication failed")
        print("   Check SMTP_USER and SMTP_PASS in .env")
    except smtplib.SMTPException as e:
        print(f"❌ SMTP Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    return False


def show_instructions():
    """Show setup instructions"""
    print("\n" + "=" * 60)
    print("EMAIL TESTER INSTRUCTIONS")
    print("=" * 60)
    print("\n1. Copy .env.example to .env:")
    print("   cp .env.example .env")
    print("\n2. Edit .env with your email credentials:")
    print("   SMTP_HOST=smtp.gmail.com")
    print("   SMTP_PORT=587")
    print("   SMTP_USER=your-email@gmail.com")
    print("   SMTP_PASS=your-app-password")
    print("   TEST_EMAIL=your-test-email@gmail.com")
    print("\n3. Run this test:")
    print("   python test_email_template.py")
    print("\n4. Check your inbox for the test email!")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    show_instructions()

    # Ask for confirmation
    response = input("Send test email? (y/n): ").strip().lower()
    if response == 'y':
        send_test_email()
    else:
        print("Test cancelled.")