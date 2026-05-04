import json
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "metacrawler.db"

def import_latest_json():
    """Import contacts from latest JSON file"""
    json_files = list(BASE_DIR.glob("contacts*.json"))

    if not json_files:
        print("❌ No contacts*.json files found!")
        return

    json_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    latest_file = json_files[0]

    print(f"📥 Importing from: {latest_file.name}")

    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    conn = sqlite3.connect(DB_PATH)
    imported = 0

    for item in data:
        domain = (item.get("domain") or "").strip()
        if not domain:
            continue

        # Insert company
        conn.execute(
            "INSERT OR IGNORE INTO companies (domain, organization, category) VALUES(?, ?, ?)",
            (domain, item.get("organization") or domain, 'engineering')
        )

        # Get company ID
        cur = conn.execute("SELECT id FROM companies WHERE domain = ?", (domain,))
        result = cur.fetchone()

        if result:
            company_id = result[0]

            # Insert contacts
            for c in item.get("contacts", []):
                email = (c.get("email") or "").strip()
                if not email:
                    continue

                conn.execute(
                    "INSERT OR IGNORE INTO contacts (company_id, email, name, confidence, type, contacted) VALUES (?, ?, ?, ?, ?, 0)",
                    (company_id, email, c.get("name", ""), c.get("confidence", 0), c.get("type", "generic"))
                )
                imported += 1

    conn.commit()
    conn.close()
    print(f"✅ Imported {imported} contacts from JSON")

if __name__ == "__main__":
    import_latest_json()