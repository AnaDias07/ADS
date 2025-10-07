import rpyc, csv, time, os, sys
from pathlib import Path

SERVER_HOST = os.getenv("SERVER_HOST", "server")
SERVER_PORT = int(os.getenv("SERVER_PORT", "18861"))
OUT_CSV = os.getenv("OUT_CSV", "/app/latency.csv")

# Example workload: (fileRef, keyword)
REQUESTS = [
    ("bee_movie_script.txt", "bee"),
    ("bee_movie_script.txt", "honey"),
    ("bee_movie_script.txt", "the"),
    ("bee_movie_script.txt", "you"),
    ("bee_movie_script.txt", "Barry"),
    ("bee_movie_script.txt", "bee"),
    ("bee_movie_script.txt", "honey"),
]

def main():
    conn = rpyc.connect(SERVER_HOST, SERVER_PORT)
    proxy = conn.root
    Path(OUT_CSV).parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["i", "file", "keyword", "count", "latency_ms"])
        for i, (file_ref, kw) in enumerate(REQUESTS, start=1):
            t0 = time.perf_counter()
            count = proxy.count(file_ref, kw)
            t1 = time.perf_counter()
            latency_ms = (t1 - t0) * 1000.0
            print(f"{i:02d} {file_ref!s:<14} {kw!s:<10} -> {count} (latency {latency_ms:.2f} ms)")
            w.writerow([i, file_ref, kw, count, f"{latency_ms:.3f}"])
    print(f"Saved latencies to {OUT_CSV}")

if __name__ == "__main__":
    sys.exit(main())
