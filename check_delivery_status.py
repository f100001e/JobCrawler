import requests
import os
from dotenv import load_dotenv
import time

load_dotenv()

api_key = os.getenv('API_KEY')
domain = "mg.franklangelliott-friendly-mailer.com"
message_id = "20260122061757.978ef8e33de76e83@mg.franklangelliott-friendly-mailer.com"

print(f"Checking delivery for message: {message_id}")
print("=" * 60)

# Check events for this message
response = requests.get(
    f"https://api.mailgun.net/v3/{domain}/events",
    auth=("api", api_key),
    params={
        "message-id": message_id,
        "event": ["delivered", "failed", "rejected", "bounced"],
        "ascending": "yes"
    }
)

if response.status_code == 200:
    events = response.json().get('items', [])

    if not events:
        print("No delivery events yet (might still be processing)")
        print("Check again in a few minutes")
    else:
        print(f"Found {len(events)} event(s):")
        for event in events:
            print(f"\nEvent: {event.get('event', 'unknown')}")
            print(f"Time: {event.get('timestamp', '')}")

            if event.get('event') == 'delivered':
                print("✅ DELIVERED to recipient's mailbox")
                print(f"   Message: {event.get('delivery-status', {}).get('message', '')}")
            elif event.get('event') == 'failed':
                print("❌ DELIVERY FAILED")
                print(f"   Reason: {event.get('delivery-status', {}).get('description', '')}")
                print(f"   Code: {event.get('delivery-status', {}).get('code', '')}")
            elif event.get('event') == 'rejected':
                print("🚫 REJECTED by Mailgun")
                print(f"   Reason: {event.get('reject', {}).get('reason', '')}")
            elif event.get('event') == 'bounced':
                print("↪️ BOUNCED by recipient server")
                print(f"   Error: {event.get('delivery-status', {}).get('message', '')}")
else:
    print(f"Error checking events: {response.status_code}")
    print(response.text[:200])

# Also try to get the specific event by ID
print("\n" + "=" * 60)
print("Checking specific event ID: Qpj1SxjuS8iSE49RMcSd5A")
print("=" * 60)

event_id = "Qpj1SxjuS8iSE49RMcSd5A"
response = requests.get(
    f"https://api.mailgun.net/v3/{domain}/events/{event_id}",
    auth=("api", api_key)
)

if response.status_code == 200:
    event = response.json().get('event', {})
    print(f"Event: {event.get('event', 'unknown')}")
    print(f"Status: {event.get('delivery-status', {}).get('status', 'unknown')}")
    print(f"Message: {event.get('delivery-status', {}).get('message', '')}")
else:
    print(f"Could not fetch specific event: {response.status_code}")