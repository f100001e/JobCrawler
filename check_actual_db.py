# check_actual_db.py
import sqlite3
import json
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "metacrawler.db"


def check_raw_database():
    """Check what's ACTUALLY in the database, no assumptions"""
    print("=" * 80)
    print("RAW DATABASE INSPECTION")
    print("=" * 80)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 1. List ALL tables
    print("\n📋 ALL TABLES IN DATABASE:")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cur.fetchall()]
    print(f"Tables: {tables}")

    # 2. Check EACH table's contents
    for table in tables:
        print(f"\n{'=' * 40}")
        print(f"TABLE: {table}")
        print(f"{'=' * 40}")

        # Get row count
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"Total rows: {count}")

            if count > 0:
                # Get column names
                cur.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in cur.fetchall()]
                print(f"Columns: {columns}")

                # Show first 5 rows
                cur.execute(f"SELECT * FROM {table} LIMIT 5")
                rows = cur.fetchall()

                print(f"\nFirst {len(rows)} rows:")
                for i, row in enumerate(rows):
                    print(f"\nRow {i + 1}:")
                    # Show each column value
                    for col_name, value in zip(columns, row):
                        if value:  # Only show non-empty values
                            print(f"  {col_name}: {value}")
        except Exception as e:
            print(f"Error reading table {table}: {e}")

    # 3. Check for ANY JSON files with contacts
    print("\n" + "=" * 80)
    print("CHECKING FOR JSON DATA FILES")
    print("=" * 80)

    json_files = list(BASE_DIR.glob("*.json"))
    print(f"Found {len(json_files)} JSON files:")

    for json_file in json_files:
        print(f"\n📄 {json_file.name} ({json_file.stat().st_size} bytes)")
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if isinstance(data, list):
                print(f"  Type: List with {len(data)} items")
                if data:
                    first_item = data[0]
                    print(f"  First item keys: {list(first_item.keys())}")

                    # Check if it has contacts
                    if 'contacts' in first_item:
                        contacts = first_item.get('contacts', [])
                        print(f"  Has contacts: {len(contacts)} in first company")

                        if contacts:
                            print(f"  First contact: {contacts[0]}")
            elif isinstance(data, dict):
                print(f"  Type: Dict with keys: {list(data.keys())}")
            else:
                print(f"  Type: {type(data)}")

        except Exception as e:
            print(f"  Error reading: {e}")

    # 4. Let's try to manually add some test data
    print("\n" + "=" * 80)
    print("CREATING TEST DATA")
    print("=" * 80)

    test_contacts = [
        {"email": "test.hr@example.com", "name": "HR Test", "domain": "example.com"},
        {"email": "jobs@startup.io", "name": "Recruiting", "domain": "startup.io"},
        {"email": "engineering@tech.co", "name": "Dev Team", "domain": "tech.co"}
    ]

    # First, ensure companies table has these domains
    for contact in test_contacts:
        domain = contact['domain']

        # Check if company exists
        cur.execute("SELECT id FROM companies WHERE domain = ?", (domain,))
        company = cur.fetchone()

        if not company:
            print(f"Adding company: {domain}")
            cur.execute(
                "INSERT INTO companies (domain, organization, category) VALUES (?, ?, ?)",
                (domain, domain.title(), 'engineering')
            )
            company_id = cur.lastrowid
        else:
            company_id = company[0]

        # Add contact
        cur.execute("""
            INSERT OR IGNORE INTO contacts 
            (company_id, email, name, contacted)
            VALUES (?, ?, ?, 0)
        """, (company_id, contact['email'], contact['name']))

    conn.commit()

    # Verify
    print("\n✅ Added test contacts. Now checking...")
    cur.execute("SELECT COUNT(*) FROM contacts")
    print(f"Total contacts now: {cur.fetchone()[0]}")

    cur.execute("SELECT email, name FROM contacts")
    print("\nAll contacts in database:")
    for email, name in cur.fetchall():
        print(f"  - {email} ({name})")

    conn.close()

    print("\n" + "=" * 80)
    print("NEXT STEPS:")
    print("1. Run: python simple_mailer.py (should now find contacts)")
    print("2. Run: python mailer.py (with DRY_RUN=true in .env)")
    print("=" * 80)


if __name__ == "__main__":
    check_raw_database()