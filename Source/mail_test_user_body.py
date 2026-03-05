#!/usr/bin/env python3
"""
Email Deliverability Tool
A comprehensive tool to diagnose and fix email deliverability issues.
Tests Google Admin SMTP, DNS records, spam triggers, and IP reputation.
"""

import os
import sys
import socket
import smtplib
import ssl
from email.message import EmailMessage
from pathlib import Path
from dotenv import load_dotenv
import subprocess

# Try to import dnspython (optional)
try:
    import dns.resolver
    import dns.reversename

    DNS_PYTHON_AVAILABLE = True
except ImportError:
    DNS_PYTHON_AVAILABLE = False

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Force IPv4 for Google Admin compatibility
original_socket = socket.socket


def ipv4_socket(*args, **kwargs):
    """Force IPv4 socket creation"""
    if args and args[0] == socket.AF_INET6:
        args = (socket.AF_INET,) + args[1:]
    elif not args:
        kwargs['family'] = socket.AF_INET
    return original_socket(*args, **kwargs)


socket.socket = ipv4_socket


class EmailDeliverabilityTool:
    """Main tool class for email deliverability testing"""

    def __init__(self):
        self.FROM_EMAIL = os.getenv("FROM_EMAIL", "")
        self.HELO_DOMAIN = os.getenv("HELO_DOMAIN", "")
        self.SMTP_HOST = os.getenv("SMTP_HOST", "smtp-relay.gmail.com")
        self.SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
        self.TEST_EMAIL = os.getenv("TEST_EMAIL", "test@example.com")
        self.USER_NAME = os.getenv("USER_NAME", "Your Name")
        self.USER_TITLE = os.getenv("USER_TITLE", "Software Engineer")

        if not self.FROM_EMAIL:
            print("❌ FROM_EMAIL not set in .env file")
            self.show_env_setup()
            sys.exit(1)

        if not self.HELO_DOMAIN:
            # Extract domain from FROM_EMAIL
            self.HELO_DOMAIN = self.FROM_EMAIL.split('@')[-1]

    def show_env_setup(self):
        """Show how to set up .env file"""
        print("\n" + "=" * 60)
        print("ENVIRONMENT SETUP")
        print("=" * 60)
        print("Create a .env file with:")
        print("\n# Required for Google Admin IP Whitelist:")
        print("FROM_EMAIL=your-email@yourdomain.com")
        print("HELO_DOMAIN=yourdomain.com")
        print("SMTP_HOST=smtp-relay.gmail.com")
        print("SMTP_PORT=587")
        print("\n# Optional:")
        print("USER_NAME=Your Name")
        print("USER_TITLE=Your Title")
        print("TEST_EMAIL=test@example.com")
        print("RESUME_PATH=resume.pdf")
        print("\n# No SMTP_USER/SMTP_PASS needed for IP whitelist")

    def check_dns(self, domain=None):
        """Check DNS records for email deliverability"""
        if not domain:
            domain = self.FROM_EMAIL.split('@')[-1]

        print("\n" + "=" * 60)
        print(f"DNS CHECK: {domain}")
        print("=" * 60)

        if DNS_PYTHON_AVAILABLE:
            self._check_dns_with_dnspython(domain)
        else:
            self._check_dns_with_nslookup(domain)

    def _check_dns_with_dnspython(self, domain):
        """Check DNS using dnspython library"""
        try:
            resolver = dns.resolver.Resolver()

            # A Records
            print("\n📡 A Records:")
            try:
                answers = resolver.resolve(domain, 'A')
                for answer in answers:
                    print(f"   ✅ {answer.address}")
            except:
                print("   ❌ No A records found")

            # MX Records
            print("\n📨 MX Records:")
            try:
                answers = resolver.resolve(domain, 'MX')
                for answer in answers:
                    print(f"   ✅ {answer.preference} {answer.exchange}")
            except:
                print("   ⚠️  No MX records (OK for SMTP relay)")

            # SPF Record (Critical)
            print("\n🛡️  SPF Record:")
            try:
                answers = resolver.resolve(domain, 'TXT')
                spf_found = False
                for answer in answers:
                    txt = str(answer)
                    if 'v=spf1' in txt:
                        spf_found = True
                        print(f"   ✅ Found: {txt}")

                        if 'include:_spf.google.com' in txt or 'include:spf.google.com' in txt:
                            print("   ✅ Includes Google SPF")
                        else:
                            print("   ❌ Missing Google include")
                        break

                if not spf_found:
                    print("   ❌ NO SPF RECORD FOUND")
                    print("\n   Add this TXT record:")
                    print(f"   v=spf1 include:_spf.google.com ~all")

            except:
                print("   ❌ Could not check SPF")

        except Exception as e:
            print(f"   Error: {e}")

    def _check_dns_with_nslookup(self, domain):
        """Check DNS using system nslookup command"""
        print("\n⚠️  Using nslookup (install dnspython for better checks)")

        # Check SPF with nslookup
        print("\n🔍 Checking SPF with nslookup...")
        try:
            cmd = ["nslookup", "-type=txt", domain]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

            if "v=spf1" in result.stdout:
                print("   ✅ SPF record found")
                for line in result.stdout.split('\n'):
                    if "v=spf1" in line:
                        print(f"   {line.strip()}")
                        if "google.com" in line:
                            print("   ✅ Includes Google")
                        else:
                            print("   ❌ Missing Google include")
                        break
            else:
                print("   ❌ No SPF record found")
                print("\n   Add this TXT record:")
                print(f"   v=spf1 include:_spf.google.com ~all")

        except Exception as e:
            print(f"   Error: {e}")

    def check_ip_reputation(self):
        """Check if your IP is blacklisted"""
        print("\n" + "=" * 60)
        print("IP REPUTATION CHECK")
        print("=" * 60)

        if not DNS_PYTHON_AVAILABLE:
            print("⚠️  Install dnspython for IP reputation check:")
            print("   pip install dnspython")
            return

        try:
            # Get public IP
            resolver = dns.resolver.Resolver()
            test_domain = "o-o.myaddr.l.google.com"
            answers = resolver.resolve(test_domain, 'TXT')
            public_ip = str(answers[0]).strip('"')

            print(f"Your public IP: {public_ip}")

            # Check blacklists
            blacklists = [
                ("Spamhaus", "zen.spamhaus.org"),
                ("Barracuda", "b.barracudacentral.org"),
            ]

            print("\nChecking blacklists:")
            listed = False
            for name, bl_domain in blacklists:
                try:
                    reversed_ip = '.'.join(reversed(public_ip.split('.')))
                    lookup = f"{reversed_ip}.{bl_domain}"
                    resolver.resolve(lookup, 'A')
                    print(f"   ❌ Listed on {name}")
                    listed = True
                except:
                    print(f"   ✅ Not listed on {name}")

            if listed:
                print("\n🚨 IP IS BLACKLISTED!")
                print(f"Visit: https://www.spamhaus.org/lookup/{public_ip}")
            else:
                print("\n✅ IP not on major blacklists")

        except Exception as e:
            print(f"Could not check reputation: {e}")

    def test_google_admin_connection(self):
        """Test Google Admin SMTP relay connection"""
        print("\n" + "=" * 60)
        print("GOOGLE ADMIN CONNECTION TEST")
        print("=" * 60)

        print(f"SMTP: {self.SMTP_HOST}:{self.SMTP_PORT}")
        print(f"HELO: {self.HELO_DOMAIN}")
        print(f"From: {self.FROM_EMAIL}")

        try:
            server = smtplib.SMTP(self.SMTP_HOST, self.SMTP_PORT, timeout=30)

            # Test HELO
            response = server.ehlo(self.HELO_DOMAIN)
            print(f"\nHELO Response: {response[0]}")

            if response[0] == 250:
                print("✅ HELO accepted by Google Admin")

                # Test STARTTLS
                ctx = ssl.create_default_context()
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE

                server.starttls(context=ctx)
                response = server.ehlo(self.HELO_DOMAIN)
                print(f"TLS HELO: {response[0]}")

                if response[0] == 250:
                    print("✅ TLS encryption successful")
                    server.quit()
                    print("\n✅ Google Admin connection test PASSED")
                    return True
                else:
                    print("❌ TLS failed")
            else:
                print("❌ HELO rejected")
                print("\nPossible issues:")
                print("1. IP not whitelisted in Google Admin")
                print("2. HELO domain not authorized")
                print("3. SMTP relay not enabled")

            server.quit()
            return False

        except Exception as e:
            print(f"\n❌ Connection failed: {e}")
            return False

    def send_test_email(self, version="clean"):
        """Send test email with different versions"""
        print("\n" + "=" * 60)
        print("SENDING TEST EMAIL")
        print("=" * 60)

        # Email versions
        versions = {
            "original": {
                "name": "Original (likely spam)",
                "subject": f"Application: {self.USER_TITLE} Roles",
                "body": f"""Hello,

I'm reaching out regarding {self.USER_TITLE.lower()} roles.

Resume attached. If there's a better contact or process, I'd appreciate a pointer.

Best,
{self.USER_NAME[:3].upper()}""",
                "filename": "resume.pdf"
            },
            "clean": {
                "name": "Clean (inbox optimized)",
                "subject": f"Introduction: {self.USER_NAME}",
                "body": f"""Hello,

I came across your company and wanted to introduce myself as a {self.USER_TITLE.lower()}.

I've attached my background information for your reference.

Sincerely,
{self.USER_NAME}
{self.USER_TITLE}""",
                "filename": f"{self.USER_NAME.replace(' ', '_')}_Background.pdf"
            },
            "professional": {
                "name": "Professional",
                "subject": f"{self.USER_NAME} - {self.USER_TITLE} Inquiry",
                "body": f"""Hello,

I'm writing to inquire about {self.USER_TITLE.lower()} opportunities.

My resume is attached for your consideration.

Best regards,
{self.USER_NAME}""",
                "filename": f"{self.USER_NAME.replace(' ', '_')}_Resume.pdf"
            }
        }

        config = versions.get(version, versions["clean"])

        print(f"Version: {config['name']}")
        print(f"To: {self.TEST_EMAIL}")
        print(f"Subject: {config['subject']}")

        # Create email
        msg = EmailMessage()
        msg["From"] = f"{self.USER_NAME} <{self.FROM_EMAIL}>"
        msg["To"] = self.TEST_EMAIL
        msg["Subject"] = config["subject"]
        msg["Reply-To"] = self.FROM_EMAIL
        msg.set_content(config["body"])

        # Add attachment if exists
        rp = Path(os.getenv("RESUME_PATH", "resume.pdf"))
        resume_path = rp if rp.is_absolute() else (BASE_DIR / rp).resolve()

        if resume_path.exists():
            try:
                data = resume_path.read_bytes()
                msg.add_attachment(
                    data,
                    maintype="application",
                    subtype="pdf",
                    filename=config["filename"]
                )
                print(f"Attachment: {config['filename']}")
            except Exception as e:
                print(f"⚠️  Could not attach: {e}")

        # Send email
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

            server = smtplib.SMTP(self.SMTP_HOST, self.SMTP_PORT, timeout=30)
            server.ehlo(self.HELO_DOMAIN)
            server.starttls(context=ctx)
            server.ehlo(self.HELO_DOMAIN)

            server.send_message(msg)
            server.quit()

            print("\n✅ TEST EMAIL SENT!")
            print("📧 Check ALL folders in your email:")
            print("   • Inbox")
            print("   • Promotions")
            print("   • Updates")
            print("   • Spam")

            if version == "original":
                print("\n⚠️  This version has spam triggers and may go to spam")
            elif version == "clean":
                print("\n✅ This version is optimized for inbox delivery")

        except Exception as e:
            print(f"\n❌ Failed to send: {e}")

    def analyze_spam_triggers(self):
        """Analyze email content for spam triggers"""
        print("\n" + "=" * 60)
        print("SPAM TRIGGER ANALYSIS")
        print("=" * 60)

        print("\n❌ Common Spam Triggers:")
        print("   • 'Application:' in subject (+2 points)")
        print("   • 'I'm reaching out' phrase (+1 point)")
        print("   • 'Resume attached' (+1 point)")
        print("   • Abbreviated signature (+1.5 points)")
        print("   • Generic filename 'resume.pdf' (+0.5 points)")

        print("\n✅ Recommended Fixes:")
        print("   1. Change subject to 'Introduction:' or 'Inquiry:'")
        print("   2. Use 'I'm writing to' or 'I wanted to introduce'")
        print("   3. Say 'background information' instead of 'resume'")
        print("   4. Use full name in signature")
        print("   5. Use personalized filename")

        print("\n📊 Spam Score Threshold:")
        print("   • 0-3 points: Likely inbox")
        print("   • 4+ points: Likely spam folder")

    def show_dns_instructions(self):
        """Show DNS setup instructions"""
        domain = self.FROM_EMAIL.split('@')[-1]

        print("\n" + "=" * 60)
        print("DNS SETUP INSTRUCTIONS")
        print("=" * 60)

        print(f"\nFor domain: {domain}")
        print("\n1. SPF Record (REQUIRED):")
        print("   Type: TXT")
        print(f"   Name: @ (or {domain})")
        print("   Value: v=spf1 include:_spf.google.com ~all")
        print("   TTL: 3600")

        print("\n2. DMARC Record (RECOMMENDED):")
        print("   Type: TXT")
        print(f"   Name: _dmarc.{domain}")
        print("   Value: v=DMARC1; p=none; rua=mailto:dmarc-reports@{domain}")
        print("   TTL: 3600")

        print("\n📍 Where to add:")
        print("   • Domain registrar's DNS management")
        print("   • Cloudflare, GoDaddy, Namecheap, etc.")
        print("\n⏰ DNS changes take 24-48 hours to propagate")

    def main_menu(self):
        """Display main menu"""
        while True:
            print("\n" + "=" * 60)
            print("📧 EMAIL DELIVERABILITY TOOL")
            print("=" * 60)
            print(f"Domain: {self.FROM_EMAIL.split('@')[-1]}")
            print(f"From: {self.FROM_EMAIL}")
            print("=" * 60)
            print("1. Check DNS Records")
            print("2. Check IP Reputation")
            print("3. Test Google Admin Connection")
            print("4. Send Test Email (Original)")
            print("5. Send Test Email (Clean)")
            print("6. Send Test Email (Professional)")
            print("7. Analyze Spam Triggers")
            print("8. Show DNS Instructions")
            print("9. Show .env Setup")
            print("0. Exit")
            print("=" * 60)

            choice = input("\nSelect option (0-9): ").strip()

            if choice == "1":
                self.check_dns()
            elif choice == "2":
                self.check_ip_reputation()
            elif choice == "3":
                self.test_google_admin_connection()
            elif choice == "4":
                self.send_test_email("original")
            elif choice == "5":
                self.send_test_email("clean")
            elif choice == "6":
                self.send_test_email("professional")
            elif choice == "7":
                self.analyze_spam_triggers()
            elif choice == "8":
                self.show_dns_instructions()
            elif choice == "9":
                self.show_env_setup()
            elif choice == "0":
                print("\nGoodbye! Remember: DNS changes take 24-48 hours.")
                break
            else:
                print("❌ Invalid choice")


def main():
    """Main entry point"""
    print("\n" + "=" * 60)
    print("EMAIL DELIVERABILITY TOOL")
    print("=" * 60)
    print("Diagnose and fix email deliverability issues")
    print("Perfect for Google Admin SMTP with IP whitelist")

    tool = EmailDeliverabilityTool()
    tool.main_menu()


if __name__ == "__main__":
    main()