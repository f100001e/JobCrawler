#!/usr/bin/env python3
# run.py - Unified launcher for the job crawler system
import sys
import subprocess
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent


def show_menu():
    print("\n" + "=" * 60)
    print("JOB CRAWLER SYSTEM")
    print("=" * 60)
    print("\nWhat would you like to do?")
    print("1. Run crawler (find companies & get emails)")
    print("2. Run mailer (send emails)")
    print("3. Import JSON contacts only")
    print("4. Check database status")
    print("5. Reset contacted status")
    print("6. Exit")

    choice = input("\nEnter choice (1-6): ").strip()
    return choice


def run_crawler():
    print("\n" + "=" * 60)
    print("RUNNING CRAWLER")
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


def import_json_only():
    print("\n" + "=" * 60)
    print("IMPORT JSON CONTACTS ONLY")
    print("=" * 60)

    import_script = BASE_DIR / "import_json.py"

    if not import_script.exists():
        print("❌ import_json.py not found!")
        print("Please create import_json.py with import logic")
        return

    subprocess.run([sys.executable, "import_json.py"])


def check_database():
    print("\n" + "=" * 60)
    print("DATABASE STATUS")
    print("=" * 60)

    import sqlite3

    db_path = BASE_DIR / "metacrawler.db"
    if not db_path.exists():
        print("❌ Database file not found!")
        return

    conn = sqlite3.connect(db_path)
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

    print(f"\n📊 Database Statistics:")
    print(f"   Companies: {companies}")
    print(f"   Total contacts: {contacts}")
    print(f"   Pending (contacted=0): {pending}")
    print(f"   Sent (contacted=1): {sent}")
    print(f"   Failed (contacted=-1): {failed}")

    # Show JSON files
    json_files = list(BASE_DIR.glob("contacts*.json"))
    print(f"\n📄 JSON Files: {len(json_files)}")
    for json_file in sorted(json_files, key=lambda p: p.stat().st_mtime, reverse=True)[:3]:
        mtime = datetime.fromtimestamp(json_file.stat().st_mtime)
        print(f"   - {json_file.name} ({mtime})")

    conn.close()


def reset_contacts():
    print("\n" + "=" * 60)
    print("RESET CONTACT STATUS")
    print("=" * 60)

    import sqlite3

    db_path = BASE_DIR / "metacrawler.db"
    if not db_path.exists():
        print("❌ Database file not found!")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Show current status
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
    print("3. Cancel")

    choice = input("\nEnter choice (1-3): ").strip()

    if choice == "1":
        cur.execute("UPDATE contacts SET contacted = 0, contacted_at = NULL, last_error = NULL")
        print(f"✅ Reset ALL contacts to pending")
    elif choice == "2":
        cur.execute("UPDATE contacts SET contacted = 0, contacted_at = NULL, last_error = NULL WHERE contacted = -1")
        print(f"✅ Reset failed contacts to pending")
    else:
        print("Cancelled.")
        conn.close()
        return

    conn.commit()
    conn.close()


def main():
    while True:
        choice = show_menu()

        if choice == "1":
            run_crawler()
        elif choice == "2":
            run_mailer()
        elif choice == "3":
            import_json_only()
        elif choice == "4":
            check_database()
        elif choice == "5":
            reset_contacts()
        elif choice == "6":
            print("\nGoodbye!")
            break
        else:
            print("\nInvalid choice. Please try again.")

        input("\nPress Enter to continue...")


if __name__ == "__main__":
    main()