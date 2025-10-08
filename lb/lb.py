# lb.py — Fault-Tolerant Load Balancer for Phase 4
import asyncio, os, itertools, time

# ---------------- Configuration ----------------
BACKENDS = [
    ("server1", 18861),
    ("server2", 18862),
    ("server3", 18863),
]

ALGO = os.getenv("LB_ALGO", "rr")  # "rr" or "lc"
LB_PORT = int(os.getenv("LB_PORT", "9000"))

# ---------------- Global State -----------------
_rr = itertools.cycle(range(len(BACKENDS)))  # round-robin pointer
_inflight = [0] * len(BACKENDS)              # active connections per backend
HEALTH = [True] * len(BACKENDS)              # backend health flags
UNAVAILABLE_UNTIL = [0.0] * len(BACKENDS)    # retry-after timestamps

# ---------------- Health Management ------------
def _refresh_health():
    now = time.time()
    for i in range(len(BACKENDS)):
        if not HEALTH[i] and now >= UNAVAILABLE_UNTIL[i]:
            HEALTH[i] = True  # backend eligible again

def available_backends():
    _refresh_health()
    return [i for i, ok in enumerate(HEALTH) if ok]

async def connect_backend(i):
    host, port = BACKENDS[i]
    try:
        return await asyncio.open_connection(host, port)
    except Exception as e:
        HEALTH[i] = False
        UNAVAILABLE_UNTIL[i] = time.time() + 10  # quarantine 10 s
        print(f"[LB] backend {host}:{port} failed ({e}); marking down 10s", flush=True)
        raise

# ---------------- Backend Selection ------------
def _ordered_ready_for_algo():
    """Return list of backend indices to try, ordered per ALGO and RR pointer."""
    ready = available_backends()
    if not ready:
        return []

    if ALGO == "lc":
        # find current minima of inflight
        min_val = min(_inflight[i] for i in ready)
        tied = [i for i in ready if _inflight[i] == min_val]
        # rotate ties using RR pointer
        for _ in range(len(BACKENDS)):
            j = next(_rr)
            if j in tied:
                start = j
                break
        else:
            return tied
        pos = tied.index(start)
        return tied[pos:] + tied[:pos]
    else:
        # round-robin order among ready
        for _ in range(len(BACKENDS)):
            j = next(_rr)
            if j in ready:
                start = j
                break
        else:
            return ready
        pos = ready.index(start)
        return ready[pos:] + ready[:pos]

# ---------------- Data Piping ------------------
async def pump(r, w, counter):
    try:
        while True:
            data = await r.read(65536)
            if not data:
                break
            counter[0] += len(data)
            w.write(data)
            await w.drain()
    finally:
        try:
            w.close()
        except:
            pass

# ---------------- Client Handler ---------------
async def handle_client(creader, cwriter):
    peer = cwriter.get_extra_info("peername")
    t0 = time.time()
    c2s = [0]; s2c = [0]
    i = -1
    host = "n/a"; port = -1
    try:
        # read first byte before connecting (lazy connect)
        first = await creader.read(1)
        if not first:
            print(f"[LB] {peer} closed before sending data", flush=True)
            return

        # try backends until one succeeds
        connected = False
        for i in _ordered_ready_for_algo():
            host, port = BACKENDS[i]
            try:
                sreader, swriter = await connect_backend(i)
                connected = True
                break
            except Exception:
                continue

        if not connected:
            print(f"[LB] {peer} no healthy backends; closing", flush=True)
            return

        _inflight[i] += 1
        print(f"[LB] {peer} -> {host}:{port} (algo={ALGO}, inflight={_inflight})", flush=True)

        # forward first byte, then pipe both ways
        swriter.write(first)
        await swriter.drain()
        c2s[0] += 1

        await asyncio.gather(
            pump(creader, swriter, c2s),  # client -> server
            pump(sreader, cwriter, s2c),  # server -> client
        )

    except (ConnectionResetError, BrokenPipeError) as e:
        print(f"[LB] {peer} teardown with {host}:{port}: {e}", flush=True)
    except Exception as e:
        print(f"[LB] {peer} error to {host}:{port}: {e}", flush=True)
        try:
            cwriter.close()
        except:
            pass
    finally:
        if 0 <= i < len(_inflight):
            _inflight[i] -= 1
        dur = (time.time() - t0) * 1000
        print(f"[LB] {peer} <- {host}:{port} done ({dur:.1f} ms) "
              f"bytes c->s={c2s[0]} s->c={s2c[0]} inflight={_inflight}", flush=True)

# ---------------- Entry Point ------------------
async def main():
    srv = await asyncio.start_server(handle_client, "0.0.0.0", LB_PORT)
    print(f"[LB] listening on :{LB_PORT} backends={BACKENDS} algo={ALGO}", flush=True)
    async with srv:
        await srv.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
