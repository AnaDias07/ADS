import rpyc, os, redis, re
from rpyc.utils.server import ThreadedServer

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DATA_DIR   = os.getenv("DATA_DIR", "/data")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def count_in_text(text: str, keyword: str) -> int:
    # whole-word, case-insensitive; adjust if you need substring matches
    pattern = r'\b' + re.escape(keyword) + r'\b'
    return len(re.findall(pattern, text, flags=re.IGNORECASE))

class WordCountService(rpyc.Service):
    def exposed_count(self, file_ref: str, keyword: str) -> int:
        key = f"{file_ref}::{keyword.lower()}"
        cached = r.get(key)
        if cached is not None:
            return int(cached)

        path = os.path.join(DATA_DIR, file_ref)
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            text = f.read()

        count = count_in_text(text, keyword)
        r.set(key, count)
        return count

if __name__ == "__main__":
    port = int(os.getenv("RPC_PORT", "18861"))
    t = ThreadedServer(WordCountService, port=port, protocol_config={"allow_public_attrs": True})
    print(f"RPyC server listening on {port}, Redis at {REDIS_HOST}:{REDIS_PORT}, data dir {DATA_DIR}")
    t.start()
