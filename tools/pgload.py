"""A load generator for the Postgres port, as tools/loadgen.go is for HTTP: the same file of statements
and the same JSON line out, so serve_bench.py runs its steps through either door.

    python3 tools/pgload.py --dsns postgresql://u@127.0.0.1:5433/lake --bodies mix.sql -c 200 --secs 10

Each client is a connection of its own (asyncpg), the connections round-robin over `--dsns`, each
statement one of the file's lines at random (`label<TAB>statement`; `{k}` becomes a random key in
[0, keys)). asyncpg sends each statement as Postgres's extended protocol does, and keeps a connection's
statements prepared, as an application's driver would. The clients are spread over `--procs` processes
(one Python process tops out near 10,000 statements a second)."""
import argparse, asyncio, json, multiprocessing, random, time


def lines(path):
    out = []
    for line in open(path):
        if line.strip():
            label, _, statement = line.rstrip("\n").partition("\t") if "\t" in line else ("", "", line.rstrip("\n"))
            out.append((label, statement))
    return out


async def client(dsn, mix, keys, deadline, lat, errors, seed):
    import asyncpg
    rng = random.Random(seed)
    con = await asyncpg.connect(dsn, statement_cache_size=1024)
    try:
        while time.monotonic() < deadline:
            label, statement = rng.choice(mix)
            began = time.monotonic()
            try:
                await con.fetch(statement.replace("{k}", str(rng.randrange(keys))))
                lat.setdefault(label, []).append((time.monotonic() - began) * 1000)
            except Exception as e:  # (counted by its kind, as loadgen counts a status)
                errors[type(e).__name__] = errors.get(type(e).__name__, 0) + 1
    finally:
        await con.close()


def part(args):
    """One process's clients; their latencies by label, their errors, and when the last answer came."""
    dsns, mix, keys, clients, secs, first = args
    lat, errors = {}, {}
    async def run():
        deadline = time.monotonic() + secs
        await asyncio.gather(*(client(dsns[(first + i) % len(dsns)], mix, keys, deadline, lat, errors, first + i) for i in range(clients)))
    began = time.monotonic()
    asyncio.run(run())
    return lat, errors, time.monotonic() - began


def quantile(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsns", required=True, help="comma-separated: connections round-robin over them")
    ap.add_argument("--bodies", required=True)
    ap.add_argument("--keys", type=int, default=1)
    ap.add_argument("-c", type=int, default=8, help="clients, a connection each")
    ap.add_argument("--secs", type=float, default=5)
    ap.add_argument("--procs", type=int, default=max(1, multiprocessing.cpu_count() // 2))
    a = ap.parse_args()
    dsns, mix = a.dsns.split(","), lines(a.bodies)
    procs = min(a.procs, a.c)
    shares = [a.c // procs + (i < a.c % procs) for i in range(procs)]
    firsts = [sum(shares[:i]) for i in range(procs)]
    with multiprocessing.Pool(procs) as pool:
        parts = pool.map(part, [(dsns, mix, a.keys, n, a.secs, f) for n, f in zip(shares, firsts)])
    lat, errors = {}, {}
    for l, e, _ in parts:
        for k, v in l.items():
            lat.setdefault(k, []).extend(v)
        for k, v in e.items():
            errors[k] = errors.get(k, 0) + v
    elapsed = max(p[2] for p in parts)  # (to the last answer: a statement sent before the deadline is counted when it comes back)
    every = [x for v in lat.values() for x in v]
    labels = list(dict.fromkeys(label for label, _ in mix))
    print(json.dumps({"qps": int(len(every) / elapsed), "secs": elapsed, "errors": sum(errors.values()), "errors_by_kind": errors,
                      "p50_ms": quantile(every, .5), "p99_ms": quantile(every, .99),
                      "by": [{"label": k, "n": len(lat.get(k, [])), "p50_ms": quantile(lat.get(k, []), .5), "p99_ms": quantile(lat.get(k, []), .99)} for k in labels]}))


if __name__ == "__main__":
    main()
