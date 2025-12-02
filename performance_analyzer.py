import subprocess
import time
import pandas as pd
import matplotlib.pyplot as plt

subprocess.run(["docker", "exec", "p4-server-1", "python3", "client.py", "DbToHdfs"], check=False, capture_output = True, text = True)

subprocess.run(["docker", "exec", "p4-server-1", "hdfs", "dfs", "-rm", "-r", "hdfs://boss:9000/partitions"], check = False, capture_output = True, text = True)

county_codes_list = ["55001", "55003", "55027", "55059", "55133"]

df = pd.DataFrame({'operation': [], 'time': []})

for county_code in county_codes_list:
    start_time = time.monotonic()
    subprocess.run(["docker", "exec", "p4-server-1", "python3", "client.py", "CalcAvgLoan", "-c", county_code], check = False, capture_output = True, text = True)
    end_time = time.monotonic()
    new_row = pd.DataFrame([{'operation': "create", 'time': end_time-start_time}])
    df = pd.concat([df, new_row], ignore_index = True)

    start_time = time.monotonic()
    subprocess.run(["docker", "exec", "p4-server-1", "python3", "client.py", "CalcAvgLoan", "-c", county_code], check = False, capture_output = True, text = True)
    end_time = time.monotonic()
    new_row = pd.DataFrame([{'operation': "reuse", 'time': end_time-start_time}])
    df = pd.concat([df, new_row], ignore_index = True)

df.to_csv("/app/outputs/performance_results.csv", index=False)

# Calculate average 'create' and 'reuse' times
average_times = df.groupby('operation')['time'].mean()

# Generate a bar chart
operations = average_times.index
times = average_times.values

plt.figure(figsize=(8, 6))
plt.bar(operations, times, color=['blue', 'green'])
plt.xlabel('Operation Type')
plt.ylabel('Average Time (seconds)')
plt.title('Average Performance Times for Create vs. Reuse Operations')
plt.grid(axis='y', linestyle='--')
plt.savefig('/app/outputs/performance_analysis.png')
print("Performance analysis chart saved as performance_analysis.png")

