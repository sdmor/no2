import subprocess
from datetime import datetime, timedelta
import pytz

# Set timezone to EST
est = pytz.timezone("US/Eastern")

# Calculate dates
now_est = datetime.now(est)
yesterday = (now_est - timedelta(days=1)).strftime("%Y-%m-%d")
day_before_yesterday = (now_est - timedelta(days=2)).strftime("%Y-%m-%d")
ten_days_ago = (now_est - timedelta(days=10)).strftime("%Y-%m-%d")

# List of date ranges
date_ranges = [
    (day_before_yesterday, yesterday),
    (ten_days_ago, ten_days_ago),
]

# Run main.py for each date range
for start_date, end_date in date_ranges:
    subprocess.run(["python3", "main.py", start_date, end_date])
