# scripts/gc_debug_day.py
import os, getpass
from garminconnect import Garmin

DATE = os.getenv("DATE", "2025-10-10")

email = os.getenv("GARMIN_EMAIL") or input("Garmin email: ").strip()
password = os.getenv("GARMIN_PASSWORD") or getpass.getpass("Garmin password: ")

g = Garmin(email, password)
g.login()  # može tražiti 2FA u konzoli

print("SLEEP RAW:")
print(g.get_sleep_data(DATE))

print("\nHRV RAW:")
try:
    print(g.get_hrv_data(DATE))
except Exception as e:
    print("HRV error:", e)

print("\nDAILY RAW:")
print(g.get_stats(DATE))

g.logout()
