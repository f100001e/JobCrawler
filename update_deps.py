#!/usr/bin/env python3
"""
Auto-update dependencies.json from current pip packages
Run this after any pip install/uninstall
"""
import json
import subprocess
import sys


def update_dependencies():
    """Update dependencies.json with current pip packages"""
    try:
        # Get installed packages as JSON
        print("📦 Fetching current dependencies...")
        result = subprocess.run(
            [sys.executable, "-m", "pip", "list", "--format=json"],
            capture_output=True,
            text=True,
            check=True
        )

        packages = json.loads(result.stdout)

        # Save to dependencies.json
        with open("dependencies.json", "w") as f:
            json.dump(packages, f, indent=2)

        print(f"✅ Updated dependencies.json with {len(packages)} packages")
        return True

    except subprocess.CalledProcessError:
        print("❌ Failed to run pip command")
        return False
    except json.JSONDecodeError:
        print("❌ Failed to parse pip output")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False


if __name__ == "__main__":
    success = update_dependencies()
    sys.exit(0 if success else 1)