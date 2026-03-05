#!/usr/bin/env python3
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent
print(f"📁 Looking in: {BASE_DIR}")
print("\n📄 All .json files:")
for f in sorted(BASE_DIR.glob("*.json")):
    print(f"   {f.name}")

print("\n🎯 hunter_contacts_20260211_1357.json exists?")
target = BASE_DIR / "hunter_contacts_20260211_1357.json"
print(f"   {target.exists()}")

print("\n🔍 All hunter*.json files:")
for f in sorted(BASE_DIR.glob("hunter*.json")):
    print(f"   {f.name}")