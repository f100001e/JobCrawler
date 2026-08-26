#!/usr/bin/env python3
# run.py - Unified launcher for the job crawler system
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import sqlite3
import json
import os
from dotenv import load_dotenv
import csv

BASE_DIR = Path(__file__).resolve().parent

# Load database path from .env or use default
load_dotenv(BASE_DIR / ".env")
DB_PATH = (BASE_DIR / os.getenv("DB_PATH", "metacrawler.db")).resolve()


def show_menu():
    print("\n" + "=" * 60)
    print("JOB CRAWLER SYSTEM")
    print("=" * 60)
    print("\nWhat would you like to do?")
    print("1. Run crawler (ALL sources - find companies & get emails)")
    print("2. Run crawler (LOCAL FILE only - target_companies.txt)")
    print("3. Run crawler (YAML FILE only - companies.yaml)")
    print("4. Run crawler (APOLLO only - direct contact import)")
    print("5. Run mailer (send emails) - Normal SMTP")
    print("6. Run mailer - Google Admin IPv4 only")
    print("7. 📧 Test Email (preview formatting, forced IPv4)")
    print("8. 📧 Test Email - Normal SMTP (NO IPv4)")  # <-- NEW
    print("9. Import JSON contacts only")
    print("10. Check database status")
    print("11. Reset contacted status")
    print("12. Reset Apollo pagination state")
    print("13. Show Apollo pagination status")
    print("14. Show crawler help")
    print("15. Exit")

    choice = input("\nEnter choice (1-15): ").strip()
    return choice


def run_crawler_yaml_only():
    print("\n" + "=" * 60)
    print("RUNNING CRAWLER (YAML FILE ONLY)")
    print("=" * 60)
    try:
        subprocess.run([sys.executable, "crawler.py", "--yaml-only"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ YAML-only crawler failed: {e}")
    except FileNotFoundError:
        print("❌ crawler.py not found!")


def run_crawler_apollo_only():
    print("\n" + "=" * 60)
    print("RUNNING CRAWLER (APOLLO ONLY)")
    print("=" * 60)
    try:
        subprocess.run([sys.executable, "crawler.py", "--apollo-only"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Apollo-only crawler failed: {e}")
    except FileNotFoundError:
        print("❌ crawler.py not found!")


def run_crawler_local_only():
    """Run crawler only for companies in local target_companies.txt file"""
    print("\n" + "=" * 60)
    print("RUNNING CRAWLER (LOCAL FILE ONLY)")
    print("=" * 60)
    try:
        subprocess.run([sys.executable, "crawler.py", "--local-only"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Local-only crawler failed with error: {e}")
    except FileNotFoundError:
        print("❌ crawler.py not found!")


def run_crawler():
    print("\n" + "=" * 60)
    print("RUNNING CRAWLER (ALL SOURCES)")
    print("=" * 60)
    try:
        subprocess.run([sys.executable, "crawler.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Crawler failed with error: {e}")
    except FileNotFoundError:
        print("❌ crawler.py not found!")


def run_mailer():
    print("\n" + "=" * 60)
    print("RUNNING MAILER")
    print("=" * 60)
    try:
        subprocess.run([sys.executable, "mailer.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Mailer failed with error: {e}")
    except FileNotFoundError:
        print("❌ mailer.py not found!")


def run_mailer_google_admin():
    """Run mailer with Google Admin IPv4-only connection"""
    print("\n" + "=" * 60)
    print("RUNNING MAILER (GOOGLE ADMIN IPv4 MODE)")
    print("=" * 60)
    try:
        subprocess.run([sys.executable, "mailer.py", "--google-admin"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Google Admin mailer failed with error: {e}")
    except FileNotFoundError:
        print("❌ mailer.py not found!")

def test_email_normal_smtp():
    """
    Send a test email using Normal SMTP (NO IPv4 forcing)
    Uses your Google App Password from .env
    """
    print("\n" + "=" * 60)
    print("📧 TEST EMAIL - NORMAL SMTP (NO IPv4)")
    print("=" * 60)
    print("   Mode: Normal SMTP (not forced IPv4)")
    print("   Auth: SMTP with Google App Password")
    print("=" * 60)

    mailer_file = BASE_DIR / "mailer.py"
    if not mailer_file.exists():
        print(f"❌ mailer.py not found!")
        return

    email = input("\nEnter test email address: ").strip()
    if not email:
        print("❌ Email address required!")
        return

    print(f"\n📨 Sending test email to: {email}")
    print("   Mode: Normal SMTP (no IPv4 forcing)")

    # Don't use --google-admin flag for normal SMTP
    cmd = [sys.executable, "mailer.py", "--test-send", email]

    try:
        subprocess.run(cmd, check=True)
        print(f"\n✅ Test email sent with Normal SMTP!")
        print(f"   Please check {email} (including spam folder)")
    except subprocess.CalledProcessError as e:
        print(f"❌ Test failed with error: {e}")
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")


def test_email_ipv4():
    """Send a test email using forced IPv4 connection"""
    print("\n" + "=" * 60)
    print("📧 TEST EMAIL - FORCED IPv4 MODE")
    print("=" * 60)

    mailer_file = BASE_DIR / "mailer.py"
    if not mailer_file.exists():
        print(f"❌ mailer.py not found!")
        return

    # Check for .env file
    env_file = BASE_DIR / ".env"
    if not env_file.exists():
        print(f"❌ .env file not found! SMTP settings are required.")
        return

    email = input("\nEnter test email address: ").strip()
    if not email:
        print("❌ Email address required!")
        return

    print(f"\n📨 Sending test resume email to: {email}")
    print("   Mode: Forced IPv4 (Google Admin mode)")
    print("   This helps ensure emails don't go to spam")
    print("   Check your spam folder and mark as 'Not Spam' if needed")

    cmd = [sys.executable, "mailer.py", "--test-send", email, "--google-admin"]

    try:
        subprocess.run(cmd, check=True)
        print(f"\n✅ Test email sent successfully!")
        print(f"   Please check {email} (including spam folder)")
        print(f"   If in spam, mark as 'Not Spam' to train the filter")
    except subprocess.CalledProcessError as e:
        print(f"❌ Test failed with error: {e}")
    except KeyboardInterrupt:
        print("\n⚠️  Test interrupted by user")


def import_json_only():
    """Import JSON contacts into database with file selection"""
    print("\n" + "=" * 60)
    print("📥 IMPORT JSON CONTACTS TO DATABASE")
    print("=" * 60)

    json_files = list(BASE_DIR.glob("contacts*.json"))
    if not json_files:
        print("❌ No contacts*.json files found!")
        print("💡 Run the crawler first to generate contact files")
        return

    json_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    print("\n📄 Available JSON files:")
    for i, json_file in enumerate(json_files[:10], 1):
        mtime = datetime.fromtimestamp(json_file.stat().st_mtime)
        mtime_str = mtime.strftime("%Y-%m-%d %H:%M")
        size = json_file.stat().st_size / 1024
        print(f"   {i}. {json_file.name} ({size:.1f} KB, {mtime_str})")

    print("\n   Enter number to import specific file")
    print("   Press Enter for latest file")
    print("   Type 'all' to import all files")

    file_choice = input("\nSelect file to import: ").strip()

    files_to_import = []

    if file_choice.lower() == 'all':
        files_to_import = json_files
        print(f"\n📋 Will import ALL {len(files_to_import)} files")
    elif file_choice == '':
        files_to_import = [json_files[0]]
        print(f"\n📋 Using latest file: {json_files[0].name}")
    elif file_choice.isdigit():
        idx = int(file_choice) - 1
        if 0 <= idx < len(json_files):
            files_to_import = [json_files[idx]]
            print(f"\n📋 Using file: {json_files[idx].name}")
        else:
            print("❌ Invalid choice, using latest file.")
            files_to_import = [json_files[0]]
    else:
        print("❌ Invalid choice, using latest file.")
        files_to_import = [json_files[0]]

    total_imported = 0
    total_skipped = 0
    total_files = 0

    for json_file in files_to_import:
        print(f"\n{'=' * 60}")
        print(f"📖 Processing: {json_file.name}")
        print(f"{'=' * 60}")

        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not data:
                print("❌ JSON file is empty!")
                continue

            print(f"📊 Found {len(data)} companies in JSON")

            companies_with_contacts = []
            for company in data:
                contacts = company.get('contacts', [])
                if contacts and len(contacts) > 0:
                    companies_with_contacts.append(company)

            print(f"✅ {len(companies_with_contacts)} companies have contacts")

            if not companies_with_contacts:
                print("❌ No companies with contacts found in JSON!")
                continue

            conn = sqlite3.connect(DB_PATH)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=5000")
            cursor = conn.cursor()

            imported = 0
            skipped = 0

            for company_data in companies_with_contacts:
                domain = company_data.get('domain', '')
                if not domain:
                    continue

                cursor.execute("""
                    INSERT OR IGNORE INTO companies (domain, organization, source_name)
                    VALUES (?, ?, ?)
                """, (domain, company_data.get('organization', company_data.get('company', domain)), json_file.name))

                result = cursor.execute("SELECT id FROM companies WHERE domain = ?", (domain,)).fetchone()
                if result is None:
                    continue
                company_id = result[0]

                contacts = company_data.get('contacts', [])
                for contact in contacts:
                    email = contact.get('email', '').strip()
                    if not email:
                        continue

                    cursor.execute("""
                        SELECT id FROM contacts 
                        WHERE company_id = ? AND email = ?
                    """, (company_id, email))

                    if cursor.fetchone():
                        skipped += 1
                        continue

                    cursor.execute("""
                        INSERT INTO contacts
                        (company_id, email, name, confidence, type, contacted)
                        VALUES (?, ?, ?, ?, ?, 0)
                    """, (
                        company_id,
                        email,
                        contact.get('name', ''),
                        contact.get('confidence', 70),
                        contact.get('type', 'personal'),
                    ))

                    imported += 1

            conn.commit()
            conn.close()

            print(f"   📥 Imported: {imported} new contacts")
            print(f"   ⏭️  Skipped: {skipped} duplicates")
            print(f"   ✅ Done with {json_file.name}")

            total_imported += imported
            total_skipped += skipped
            total_files += 1

        except Exception as e:
            print(f"❌ Error importing {json_file.name}: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n{'=' * 60}")
    print("📊 IMPORT SUMMARY")
    print(f"{'=' * 60}")
    print(f"📁 Files processed: {total_files}")
    print(f"📥 New contacts imported: {total_imported}")
    print(f"⏭️  Duplicates skipped: {total_skipped}")
    print(f"💾 Database: {DB_PATH.name}")
    print(f"{'=' * 60}")

    if total_imported > 0:
        csv_file = BASE_DIR / f"imported_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['email', 'name', 'domain', 'organization', 'type', 'imported_at'])
            
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT c.email, c.name, co.domain, co.organization, c.type, c.contacted_at
                FROM contacts c
                JOIN companies co ON c.company_id = co.id
                ORDER BY co.domain, c.email
                LIMIT 1000
            """)
            for row in cursor.fetchall():
                writer.writerow(row)
            conn.close()
        
        print(f"📄 CSV saved: {csv_file.name} (first 1000 contacts)")

    check_database()


def check_database():
    print("\n" + "=" * 60)
    print("DATABASE STATUS")
    print("=" * 60)

    if not DB_PATH.exists():
        print("❌ Database file not found!")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    # Counts
    cur.execute("SELECT COUNT(*) FROM companies")
    companies = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM contacts")
    contacts = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM contacts WHERE contacted = 0")
    pending = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM contacts WHERE contacted = 1")
    sent = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM contacts WHERE contacted = -1")
    failed = cur.fetchone()[0]

    # Count decision makers
    cur.execute("SELECT COUNT(*) FROM contacts WHERE is_decision_maker = 1")
    decision_makers = cur.fetchone()[0]

    print(f"\n📊 Database Statistics:")
    print(f"   Companies: {companies}")
    print(f"   Total contacts: {contacts}")
    print(f"   Decision makers: {decision_makers}")
    print(f"   Pending (contacted=0): {pending}")
    print(f"   Sent (contacted=1): {sent}")
    print(f"   Failed (contacted=-1): {failed}")

    # Show recent imports
    cur.execute("""
        SELECT source_name, COUNT(*) 
        FROM companies 
        WHERE source_name IS NOT NULL 
        GROUP BY source_name 
        ORDER BY COUNT(*) DESC 
        LIMIT 5
    """)
    sources = cur.fetchall()
    if sources:
        print(f"\n📂 Top sources:")
        for source, count in sources:
            print(f"   - {source}: {count} companies")

    # Show JSON files
    json_files = list(BASE_DIR.glob("contacts*.json"))
    print(f"\n📄 JSON Files: {len(json_files)}")
    for json_file in sorted(json_files, key=lambda p: p.stat().st_mtime, reverse=True)[:3]:
        mtime = datetime.fromtimestamp(json_file.stat().st_mtime)
        print(f"   - {json_file.name} ({mtime.strftime('%Y-%m-%d %H:%M')})")

    # Show Apollo pagination status
    try:
        cur.execute("""
            SELECT last_page, total_processed, updated_at 
            FROM crawler_metadata 
            WHERE source_name = 'apollo_people'
        """)
        apollo_state = cur.fetchone()
        if apollo_state:
            print(f"\n🚀 Apollo Status:")
            print(f"   Last page: {apollo_state[0]}")
            print(f"   Total processed: {apollo_state[1]}")
            print(f"   Last updated: {apollo_state[2][:19] if apollo_state[2] else 'N/A'}")
    except:
        pass

    conn.close()


def reset_contacts():
    print("\n" + "=" * 60)
    print("RESET CONTACT STATUS")
    print("=" * 60)

    if not DB_PATH.exists():
        print("❌ Database file not found!")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    cur.execute("SELECT contacted, COUNT(*) FROM contacts GROUP BY contacted")
    print("Current status:")
    for status, count in cur.fetchall():
        if status == 0:
            status_str = "Pending"
        elif status == 1:
            status_str = "Sent"
        elif status == -1:
            status_str = "Failed"
        else:
            status_str = f"Unknown ({status})"
        print(f"  {status_str}: {count}")

    print("\nOptions:")
    print("1. Reset ALL to pending (contacted=0)")
    print("2. Reset only failed (contacted=-1) to pending")
    print("3. Reset decision makers only")
    print("4. Cancel")

    choice = input("\nEnter choice (1-4): ").strip()

    if choice == "1":
        cur.execute("UPDATE contacts SET contacted = 0, contacted_at = NULL, last_error = NULL, retry_count = 0")
        print(f"✅ Reset ALL contacts to pending")
    elif choice == "2":
        cur.execute("UPDATE contacts SET contacted = 0, contacted_at = NULL, last_error = NULL WHERE contacted = -1")
        print(f"✅ Reset failed contacts to pending")
    elif choice == "3":
        cur.execute("UPDATE contacts SET contacted = 0, contacted_at = NULL, last_error = NULL WHERE is_decision_maker = 1")
        print(f"✅ Reset decision makers to pending")
    else:
        print("Cancelled.")
        conn.close()
        return

    conn.commit()
    conn.close()


def reset_apollo_state():
    print("\n" + "=" * 60)
    print("RESET APOLLO PAGINATION STATE")
    print("=" * 60)
    try:
        subprocess.run([sys.executable, "crawler.py", "--apollo-reset"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Reset failed: {e}")
    except FileNotFoundError:
        print("❌ crawler.py not found!")


def show_apollo_status():
    """Show Apollo pagination status without running full database check"""
    print("\n" + "=" * 60)
    print("APOLLO PAGINATION STATUS")
    print("=" * 60)
    
    if not DB_PATH.exists():
        print("❌ Database file not found!")
        return
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT last_page, total_processed, updated_at 
            FROM crawler_metadata 
            WHERE source_name = 'apollo_people'
        """)
        result = cur.fetchone()
        
        if result:
            last_page, total_processed, updated_at = result
            print(f"\n📊 Apollo Crawler Status:")
            print(f"   Last page fetched: {last_page}")
            print(f"   Total contacts processed: {total_processed}")
            print(f"   Next page to fetch: {last_page + 1}")
            print(f"   Last updated: {updated_at[:19] if updated_at else 'N/A'}")
        else:
            print("\n📊 Apollo has not been run yet or pagination state is empty.")
            print("   First run will start from page 1.")
            
        # Count Apollo-sourced companies
        cur.execute("SELECT COUNT(*) FROM companies WHERE source_name = 'apollo_people'")
        apollo_companies = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM contacts WHERE type = 'personal'")
        apollo_contacts = cur.fetchone()[0]
        
        print(f"\n📈 Apollo Stats in Database:")
        print(f"   Companies from Apollo: {apollo_companies}")
        print(f"   Personal contacts (likely from Apollo): {apollo_contacts}")
        
    except Exception as e:
        print(f"❌ Error reading Apollo status: {e}")
    finally:
        conn.close()


def show_help():
    """Show crawler.py help"""
    print("\n" + "=" * 60)
    print("CRAWLER HELP")
    print("=" * 60)
    try:
        subprocess.run([sys.executable, "crawler.py", "--help"], check=False)
    except FileNotFoundError:
        print("❌ crawler.py not found!")


def main():
    while True:
        choice = show_menu()

        if choice == "1":
            run_crawler()
        elif choice == "2":
            run_crawler_local_only()
        elif choice == "3":
            run_crawler_yaml_only()
        elif choice == "4":
            run_crawler_apollo_only()
        elif choice == "5":
            run_mailer()
        elif choice == "6":
            run_mailer_google_admin()
        elif choice == "7":
            test_email_ipv4()
        elif choice == "8":  # NEW
            test_email_normal_smtp()
        elif choice == "9":
            import_json_only()
        elif choice == "10":
            check_database()
        elif choice == "11":
            reset_contacts()
        elif choice == "12":
            reset_apollo_state()
        elif choice == "13":
            show_apollo_status()
        elif choice == "14":
            show_help()
        elif choice == "15":  # Changed from 14
            print("\nExiting. Goodbye!")
            break
        else:
            print("\nInvalid choice. Please try again.")

        input("\nPress Enter to continue...")


if __name__ == "__main__":
    main()