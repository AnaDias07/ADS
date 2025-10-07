# client/plot_latency.py
import csv
import matplotlib
matplotlib.use("Agg")  # headless backend for containers
import matplotlib.pyplot as plt

xs, ys = [], []
with open("/app/latency.csv") as f:
    r = csv.DictReader(f)
    for row in r:
        xs.append(int(row["i"]))
        ys.append(float(row["latency_ms"]))

plt.figure()
plt.plot(xs, ys, marker="o")
plt.xlabel("Request #")
plt.ylabel("Latency (ms)")
plt.title("Execution latency per request")
plt.grid(True)
plt.tight_layout()
plt.savefig("/app/latency.png")
print("Wrote /app/latency.png")
