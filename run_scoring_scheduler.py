import schedule
import time
import subprocess
from datetime import datetime

def run_dq_script():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Running dq_scoring.py...")
    try:
        # Run dq_scoring.py as a subprocess
        subprocess.run(["python", "dq_scoring.py"], check=True)
        print("✅ dq_scoring.py completed.\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running dq_scoring.py: {e}\n")

# Schedule to run every 1 minute
schedule.every(1).minutes.do(run_dq_script)

print("🕒 Scheduler started: dq_scoring.py will run every 1 minute.\nPress Ctrl+C to stop.\n")
while True:
    schedule.run_pending()
    time.sleep(1)
