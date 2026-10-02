#!/usr/bin/env python3
"""Pondra deployed the ways ADR-041 gives, each tried as a user would run it:

- **image**: the container image serves, as a user that isn't root, from a volume it owns; its
  queries are sized by the container's memory, not the host's; `docker stop` hands the lake on
  cleanly (exit 0); a new container on the same volume has the rows and the key that seals secrets.
- **compose**: `deploy/compose/compose.yaml`'s three nodes are one cluster: one leader, a write on
  one node read on another, Postgres and Kafka on their ports; the leader killed, another leads and
  takes writes; the killed one back as a follower; a leader stopped hands on at once; every node
  then has every row.
- **python**: the image with Python runs a `LANGUAGE python` function.
- **chart**: `deploy/helm/pondra` renders for every way it's meant to run (no cluster needed), one
  node with nothing set and three on a bucket, and refuses what can't work.
- **helm**: the chart on Kubernetes (kind in CI): three nodes and a reader on a bucket, guarded by
  the chart's own tokens; the leader's pod deleted, a rolling restart and an upgrade lose nothing
  and keep the keys; the chart with nothing set is one node that keeps its lake.
- **service**: `pondra service` (systemd, launchd, Windows's service manager), as root: installed,
  it serves with the variables it was installed with; killed, it's started again; installed again,
  it moves to its new options; uninstalled, it's gone.

  deploy_check.py [image] [python] [compose] [chart] [helm] [service] [--image pondra:dev]

The images are built from a context `tools/image.py` makes (`--bin target/release/pondra` for this
machine's build); image, python and compose need Docker with compose; chart needs helm; helm needs
kubectl and helm pointed at a cluster that has the image (`kind load docker-image pondra:dev`);
service needs root (it uses sudo) or, on Windows, an Administrator.
"""
import argparse, base64, json, re, os, socket, struct, subprocess, sys, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
COMPOSE = os.path.join(ROOT, "deploy", "compose", "compose.yaml")
checks, info = {}, {}


def check(name, ok, detail=None):
    checks[name] = bool(ok)
    print(f"{'ok  ' if ok else 'FAIL'} {name}" + (f": {detail}" if detail is not None and not ok else ""), flush=True)
    return ok


def run(*cmd, env=None, check=True, timeout=300, both=False):
    r = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, **(env or {})}, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f"{' '.join(cmd)}: {(r.stdout + r.stderr)[-1500:]}")
    return (r.stdout + r.stderr).strip() if both else r.stdout.strip()


def http(port, path, body=None, timeout=30, token=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body.encode() if body is not None else None, method="POST" if body is not None else "GET",
                                 headers={"Authorization": f"Bearer {token}"} if token else {})
    return urllib.request.urlopen(req, timeout=timeout).read().decode()


def sql(port, q, timeout=60, token=None):
    return json.loads(http(port, "/sql", q, timeout, token))


def stats(port):
    try:
        return json.loads(http(port, "/stats", timeout=3))
    except Exception:
        return None


def until(what, secs, every=0.25):
    """`what()` once it is truthy, or None after `secs`."""
    deadline = time.time() + secs
    while time.time() < deadline:
        try:
            got = what()
            if got:
                return got
        except Exception:
            pass
        time.sleep(every)
    return None


def count(port, table="t", token=None):
    try:
        return sql(port, f"SELECT count(*) AS n FROM {table}", token=token)[0]["n"]
    except Exception:
        return None


def postgres_answers(port):
    """A Postgres startup message gets Postgres's answer (an authentication request, 'R')."""
    body = struct.pack("!I", 196608) + b"user\0pondra\0database\0lake\0\0"
    with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
        s.sendall(struct.pack("!I", len(body) + 4) + body)
        return s.recv(1) == b"R"


def metric(port, name):
    for line in http(port, "/metrics").splitlines():
        if line.startswith(name + " ") or line.startswith(name + "{"):
            return float(line.rsplit(" ", 1)[1])


def image(a):
    """The image alone: a container on a volume, stopped, and a new one on the same volume."""
    tag, vol, name = a.image, f"pondra-check-{os.getpid()}", f"pondra-check-{os.getpid()}"
    limit = 1 << 30

    def start():
        run("docker", "run", "-d", "--name", name, "--memory", str(limit), "-p", "127.0.0.1::8080", "-v", f"{vol}:/data", tag)
        port = int(run("docker", "port", name, "8080/tcp").splitlines()[0].rsplit(":", 1)[1])
        return port if until(lambda: stats(port), 60) else None

    def look(path):  # (the image has no shell: a small one looks at the volume)
        return run("docker", "run", "--rm", "-v", f"{vol}:/data", "busybox:1.37", "sh", "-c", path)

    try:
        port = start()
        if not check("the image serves on its own (serve lake --addr 0.0.0.0:8080)", port, run("docker", "logs", name, check=False)[-1500:]):
            return
        user = run("docker", "inspect", "-f", "{{.Config.User}}", tag)
        check("it runs as a user that isn't root", user not in ("", "0", "root", "0:0"), user)
        sql(port, "CREATE TABLE t (id BIGINT, name TEXT)")
        sql(port, "INSERT INTO t SELECT x, 'n' || x FROM generate_series(1, 1000) AS g(x)")
        check("SQL writes and reads in it", count(port) == 1000, count(port))
        sql(port, "CREATE SECRET kept (TYPE s3, KEY_ID 'id', SECRET 'shh', SCOPE 's3://pondra-check-bucket')")
        owners = look("stat -c '%u %a %n' /data /data/lake /data/.pondra /data/.pondra/secret.key /data/cache")
        info["owners"] = owners
        check("everything it keeps is under /data, the image user's own; the key readable by it alone",
              all(l.split()[0] == "65532" for l in owners.splitlines()) and "600 /data/.pondra/secret.key" in owners, owners)
        query_memory = metric(port, "pondra_memory_limit_bytes")
        info["query memory in a 1 GB container (bytes)"] = query_memory
        check("its queries are sized by the container's memory, not the host's", query_memory and query_memory <= limit / 2, query_memory)
        key = look("sha256sum /data/.pondra/secret.key")
        t0 = time.time()
        run("docker", "stop", "-t", "30", name)
        took, code = time.time() - t0, run("docker", "inspect", "-f", "{{.State.ExitCode}}", name)
        info["docker stop (s)"] = round(took, 2)
        check("docker stop ends it cleanly, at once (SIGTERM: exit 0)", code == "0" and took < 15, f"exit {code} after {took:.1f} s")
        run("docker", "rm", name)
        port = start()
        check("a new container on the same volume has the rows", port and count(port) == 1000, port and count(port))
        secrets = port and sql(port, "SELECT name FROM secrets()")
        check("…and the key that seals its secrets", key == look("sha256sum /data/.pondra/secret.key") and secrets and {"name": "kept"} in secrets, secrets)
    finally:
        run("docker", "rm", "-f", name, check=False)
        run("docker", "volume", "rm", "-f", vol, check=False)


def compose(a):
    """deploy/compose/compose.yaml as it is: three nodes on one lake."""
    project, env = f"pondra-check-{os.getpid()}", {"PONDRA_IMAGE": a.image}
    dc = lambda *a, **k: run("docker", "compose", "-f", COMPOSE, "-p", project, *a, env=env, **k)
    http_port = {"node1": 8080, "node2": 8081, "node3": 8082}
    try:
        dc("up", "-d", "--quiet-pull")
        up = until(lambda: all(stats(p) for p in http_port.values()) and [stats(p) for p in http_port.values()], 90)
        if not check("three nodes start", up, dc("logs", "--tail", "20", check=False)):
            return
        settled = until(lambda: (s := [stats(p) for p in http_port.values()]) and len({(x["leader"], x["term"]) for x in s}) == 1
                        and sorted(x["role"] for x in s) == ["follower", "follower", "leader"] and len(s[0]["nodes"]) == 3 and s, 60)
        info["at start"] = [(x["role"], x["leader"], x["term"]) for x in settled or up]
        check("they are one cluster: one leader, the others following it", settled, info["at start"])
        sql(8081, "CREATE TABLE t (id BIGINT, at TIMESTAMP)")
        sql(8081, "INSERT INTO t SELECT x, now() FROM generate_series(1, 30000) AS g(x)")
        check("a write on one node is read on another", until(lambda: count(8082) == 30000, 30), count(8082))
        check("Postgres answers on each node's port", all(postgres_answers(p) for p in (5432, 5433, 5434)))
        check("Kafka listens on each node's port", all(socket.create_connection(("127.0.0.1", p), timeout=5).close() is None for p in (9092, 9093, 9094)))

        def leader(among):
            s = {n: stats(http_port[n]) for n in among}
            leads = [n for n, x in s.items() if x and x["role"] == "leader"]
            return leads[0] if len(leads) == 1 and all(x and x["leader"] == f"{leads[0]}:8080" for x in s.values()) else None

        first = leader(http_port)
        t0 = time.time()
        dc("kill", first)
        rest = [n for n in http_port if n != first]
        second = until(lambda: leader(rest), 60)
        info["killed leader -> a new one (s)"] = round(time.time() - t0, 1)
        check("the leader killed, another leads", second, {n: stats(http_port[n]) for n in rest})
        other = next(n for n in rest if n != second)
        wrote = until(lambda: sql(http_port[other], "INSERT INTO t VALUES (30001, now())"), 60, every=1)
        check("…and takes writes, from any node", wrote, count(http_port[other]))
        dc("start", first)
        back = until(lambda: (s := stats(http_port[first])) and s["role"] == "follower" and s["leader"] == f"{second}:8080", 60)
        check("the killed node comes back as a follower", back, stats(http_port[first]))
        t0 = time.time()
        dc("stop", second)
        took = time.time() - t0
        third = until(lambda: leader([n for n in http_port if n != second]), 30)
        info["leader stopped -> a new one (s)"] = round(time.time() - t0, 1)
        check("a leader stopped (SIGTERM) hands on, at once", third and took < 15, f"{took:.1f} s to stop, then {info['leader stopped -> a new one (s)']} s")
        dc("start", second)
        same = until(lambda: all(count(p) == 30001 for p in http_port.values()), 60)
        check("every node has every row", same, {n: count(p) for n, p in http_port.items()})
    finally:
        if not all(checks.values()):
            info["logs"] = dc("logs", "--tail", "40", check=False)[-6000:]
        dc("down", "-v", "--timeout", "30", check=False)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Forward:
    """`kubectl port-forward` to a pod or service, for as long as it's needed."""
    def __init__(self, ns, target, port):
        # A connection the pod refuses (its server not listening yet, though the pod is Ready) ends
        # kubectl's port-forward ("lost connection to pod"), so it is started again until a probe
        # leaves it running: moto's pod, Ready before moto listened, failed a PR's run that way.
        for _ in range(30):
            self.local = free_port()
            self.p = subprocess.Popen(["kubectl", "-n", ns, "port-forward", target, f"{self.local}:{port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if until(lambda: socket.create_connection(("127.0.0.1", self.local), timeout=1).close() or True, 30):
                time.sleep(1)
                if self.p.poll() is None:
                    return
            self.p.kill()
            self.p.wait()
        raise RuntimeError(f"kubectl port-forward {target} {port} never stayed up")

    def __enter__(self):
        return self.local

    def __exit__(self, *_):
        self.p.terminate()
        self.p.wait()


def helm(a):
    """deploy/helm/pondra on the Kubernetes cluster kubectl points at (kind in CI), the image in it
    already (`kind load docker-image`): a lake in a bucket (moto, in the cluster), three nodes and a
    reader; `helm test`; SQL with the chart's own token; the leader's pod deleted; a rolling restart;
    an upgrade keeping the keys; and the chart with nothing set (one node, its lake on its volume)."""
    ns, chart = f"pondra-check-{os.getpid()}", os.path.join(ROOT, "deploy", "helm", "pondra")
    k = lambda *a, **kw: run("kubectl", "-n", ns, *a, **kw)
    repo, _, version = a.image.rpartition(":")
    image = ["--set", f"image.repository={repo},image.tag={version},image.pullPolicy=Never,persistence.size=1Gi"]
    kept = lambda: k("get", "secret", "p-pondra-key", "p-pondra-tokens", "-o", "jsonpath={.items[*].data}")

    def stats(pod):
        try:
            return json.loads(k("get", "--raw", f"/api/v1/namespaces/{ns}/pods/{pod}:8080/proxy/stats"))
        except Exception:
            return None

    def leader():
        pods = k("get", "pods", "-l", "app.kubernetes.io/component=node", "-o", "jsonpath={.items[*].metadata.name}").split()
        s = {p: stats(p) for p in pods}
        leads = [p for p, x in s.items() if x and x["role"] == "leader"]
        return leads[0] if len(pods) == 3 and len(leads) == 1 and all(x and x["leader"].startswith(leads[0] + ".") for x in s.values()) else None

    try:
        run("kubectl", "create", "namespace", ns)
        # (moto takes a host like s3.<ns>.svc for a bucket named "s3": the nodes ask by path)
        k("run", "s3", "--image=motoserver/moto:5.1.4", "--port=5000", "--expose", "--env=S3_IGNORE_SUBDOMAIN_BUCKETNAME=true")
        k("wait", "--for=condition=Ready", "pod/s3", "--timeout=180s")
        with Forward(ns, "pod/s3", 5000) as port:
            urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}/lake", method="PUT"), timeout=30).read()
        k("create", "secret", "generic", "bucket", "--from-literal=AWS_ACCESS_KEY_ID=test", "--from-literal=AWS_SECRET_ACCESS_KEY=test",
          "--from-literal=AWS_REGION=us-east-1", f"--from-literal=AWS_ENDPOINT=http://s3.{ns}.svc:5000", "--from-literal=AWS_ALLOW_HTTP=true")
        values = ["--set", "lake=s3://lake/main,bucket.existingSecret=bucket,readers.replicas=1", *image]
        r = subprocess.run(["helm", "-n", ns, "install", "p", chart, *values, "--wait", "--timeout", "6m"], capture_output=True, text=True)
        if not check("the chart installs: three nodes and a reader, ready", r.returncode == 0, (r.stdout + r.stderr)[-1500:] + k("get", "pods", "-o", "wide", check=False)):
            return
        first = until(leader, 60)
        check("the nodes are one cluster: one leader, the others following it", first)
        r = subprocess.run(["helm", "-n", ns, "test", "p", "--logs"], capture_output=True, text=True)
        check("helm test: every node answers, one leader", r.returncode == 0, (r.stdout + r.stderr)[-1500:])
        token = base64.b64decode(k("get", "secret", "p-pondra-tokens", "-o", "jsonpath={.data.PONDRA_ADMIN_TOKEN}")).decode()
        with Forward(ns, "svc/p-pondra", 8080) as port:
            try:
                sql(port, "SELECT 1")
                refused = False
            except urllib.error.HTTPError as e:
                refused = e.code == 401
            check("the chart's tokens guard it: no token, no answer", refused)
            sql(port, "CREATE TABLE t (id BIGINT, at TIMESTAMP)", token=token)
            sql(port, "INSERT INTO t SELECT x, now() FROM generate_series(1, 20000) AS g(x)", token=token)
            check("SQL through the service, with the chart's admin token", count(port, token=token) == 20000, count(port, token=token))
        with Forward(ns, "svc/p-pondra-read", 8080) as port:
            check("…and through the readers' service", until(lambda: all(count(port, token=token) == 20000 for _ in range(4)), 30), count(port, token=token))
        before = kept()
        uid = k("get", "pod", first, "-o", "jsonpath={.metadata.uid}")
        k("delete", "pod", first, "--grace-period=0", "--force", "--wait=false")
        t0 = time.time()
        # Its pod comes back under its name, which is its address (ADR-005): it leads its term again
        # when nobody claimed the next one meanwhile (a node finding the latest term its own resumes
        # it), or follows whoever did.
        second = until(lambda: k("get", "pod", first, "-o", "jsonpath={.metadata.uid}") != uid and leader(), 120)
        info["leader's pod deleted -> one leader again (s)"] = round(time.time() - t0, 1)
        info["…the leader then"] = "the same pod, its term resumed" if second == first else second
        check("the leader's pod deleted: back under its name, one leader, the others following it", second, {p: stats(p) for p in ("p-pondra-0", "p-pondra-1", "p-pondra-2")})
        with Forward(ns, "svc/p-pondra", 8080) as port:
            wrote = until(lambda: sql(port, "INSERT INTO t VALUES (20001, now())", token=token), 60, every=1)
            check("…and the cluster takes writes", wrote and count(port, token=token) == 20001, count(port, token=token))
        k("rollout", "restart", "statefulset/p-pondra")
        k("rollout", "status", "statefulset/p-pondra", "--timeout=300s", timeout=320)
        third = until(leader, 60)
        with Forward(ns, "svc/p-pondra", 8080) as port:
            check("a rolling restart (one node at a time, each handing on): one leader, every row", third and until(lambda: count(port, token=token) == 20001, 60), count(port, token=token))
        r = subprocess.run(["helm", "-n", ns, "upgrade", "p", chart, *values, "--set", "podAnnotations.round=2", "--wait", "--timeout", "6m"], capture_output=True, text=True)
        check("an upgrade keeps the key that seals secrets, and the tokens", r.returncode == 0 and kept() == before, (r.stdout + r.stderr)[-800:])
        run("helm", "-n", ns, "install", "solo", chart, *image, "--wait", "--timeout", "4m")
        token = base64.b64decode(k("get", "secret", "solo-pondra-tokens", "-o", "jsonpath={.data.PONDRA_ADMIN_TOKEN}")).decode()
        with Forward(ns, "svc/solo-pondra", 8080) as port:
            sql(port, "CREATE TABLE t AS SELECT 1 AS one", token=token)
            check("the chart with nothing set: one node, its lake on its volume", count(port, token=token) == 1)
        k("delete", "pod", "solo-pondra-0")
        k("wait", "--for=condition=Ready", "pod/solo-pondra-0", "--timeout=180s")
        with Forward(ns, "svc/solo-pondra", 8080) as port:
            check("…which keeps it through a restart", count(port, token=token) == 1, count(port, token=token))
    finally:
        if not all(checks.values()):
            info["pods"] = k("get", "pods", "-o", "wide", check=False)
            info["logs"] = k("logs", "-l", "app.kubernetes.io/name=pondra", "--tail=30", "--prefix", check=False)[-6000:]
        run("kubectl", "delete", "namespace", ns, "--wait=false", check=False)


def chart(a):
    """deploy/helm/pondra rendered without a cluster: every combination of its values makes
    manifests, and what can't work is refused before anything is made."""
    path = os.path.join(ROOT, "deploy", "helm", "pondra")
    render = lambda *sets: subprocess.run(["helm", "template", "p", path, *[x for v in sets for x in ("--set", v)]], capture_output=True, text=True)
    made = {v: render(*v.split(" ")) for v in ["replicas=1", "lake=s3://b/l", "lake=gs://b/l,readers.replicas=2,readers.autoscaling.enabled=true",
                                                 "sharedStorage.existingClaim=nfs,kafka.enabled=true,flight.enabled=true,ack=replicated",
                                                 "lake=az://c/l,tls.existingSecret=t,tls.mutual=true,python.enabled=true,ingress.enabled=true,ingress.hosts[0]=x.example.com,podMonitor.enabled=true",
                                                 "lake=s3://b/l,auth.existingSecret=a,secretKey.existingSecret=k,persistence.enabled=false,podAnnotations.round=2,podLabels.tier=1"]}
    check("the chart renders for every way it's meant to run", all(r.returncode == 0 for r in made.values()), {v: r.stderr[-300:] for v, r in made.items() if r.returncode})
    given = made["lake=s3://b/l,auth.existingSecret=a,secretKey.existingSecret=k,persistence.enabled=false,podAnnotations.round=2,podLabels.tier=1"].stdout
    check("pods' labels and annotations given as numbers are strings, as Kubernetes needs", given.count('round: "2"') == 1 and given.count('tier: "1"') == 1, given[:400])
    nodes = lambda r: r.stdout.count("kind: StatefulSet") == 1 and re.search(r"replicas: (\d+)", r.stdout.split("kind: StatefulSet")[1]).group(1)
    check("with nothing set, one node; on a bucket, three", nodes(render()) == "1" and nodes(made["lake=s3://b/l"]) == "3", (nodes(render()), nodes(made["lake=s3://b/l"])))
    refused = {"several nodes on a lake they can't share": ("replicas=3",), "readers without a shared lake": ("replicas=1", "readers.replicas=1"),
               "a folder lake on no volume": ("persistence.enabled=false",), "Python without tokens": ("lake=s3://b/l", "python.enabled=true", "auth.enabled=false")}
    wrong = [what for what, sets in refused.items() if render(*sets).returncode == 0]
    check("…and what can't work is refused, saying why", not wrong, wrong)


def python(a):
    """The image with Python: a `LANGUAGE python` function runs in it (a token is a must there)."""
    name = f"pondra-check-py-{os.getpid()}"
    try:
        run("docker", "run", "-d", "--name", name, "-p", "127.0.0.1::8080", "-e", "PONDRA_ADMIN_TOKEN=check", a.python_image)
        port = int(run("docker", "port", name, "8080/tcp").splitlines()[0].rsplit(":", 1)[1])
        if not check("the Python image serves (--python auto)", until(lambda: stats(port), 60), run("docker", "logs", name, check=False)[-1500:]):
            return
        sql(port, "CREATE FUNCTION twice(x BIGINT) RETURNS BIGINT LANGUAGE python AS $$\nreturn x * 2\n$$", token="check")
        got = sql(port, "SELECT twice(21) AS y", token="check")
        check("a Python function runs in it", got == [{"y": 42}], got)
    finally:
        run("docker", "rm", "-f", name, check=False)


def service(a):
    """`pondra service` on this machine, as root (sudo; an Administrator on Windows): installed, it
    serves and keeps its rows; killed, its manager starts it again; installed again (stopped and
    started with new options), it serves on its new port; uninstalled, it's gone."""
    win, name, token = os.name == "nt", f"pondra-check-{os.getpid()}", "check-token"
    lake = os.path.join(os.path.abspath(a.work), name, "lake")
    os.makedirs(os.path.dirname(lake), exist_ok=True)
    sudo = [*root(), "--preserve-env=PONDRA_ADMIN_TOKEN"] if root() else []
    env = {"PONDRA_ADMIN_TOKEN": token}
    pondra = lambda *args, **kw: run(*sudo, os.path.abspath(a.bin), "service", *args, env=env, **kw)
    first, second = free_port(), free_port()
    try:
        said = pondra("install", "--name", name, "--lake", lake, "--addr", f"127.0.0.1:{first}")
        info["installed"] = said
        if not check("pondra service install: it serves", until(lambda: stats(first), 60), said + pondra("status", "--name", name, check=False)):
            return
        sql(first, "CREATE TABLE t (id BIGINT)", token=token)
        sql(first, "INSERT INTO t SELECT x FROM generate_series(1, 1000) AS g(x)", token=token)
        check("…with the variables it was installed with (a token)", count(first, token=token) == 1000 and count(first) is None)
        status = pondra("status", "--name", name)
        info["status"] = status
        check("pondra service status: running, and the node leads", any(w in status for w in ("running", "Running")) and "leader of term" in status, status)
        killed = kill_node(name, first)
        check("killed, its manager starts it again, with its rows", killed and until(lambda: count(first, token=token) == 1000, 90), killed)
        pondra("install", "--name", name, "--lake", lake, "--addr", f"127.0.0.1:{second}")
        check("installed again with other options: stopped, started on its new port, with its rows", until(lambda: count(second, token=token) == 1000, 60) and not stats(first))
        pondra("uninstall", "--name", name)
        check("pondra service uninstall: stopped and gone", until(lambda: not stats(second), 30) and "no service" in pondra("status", "--name", name, check=False, both=True))
    finally:
        pondra("uninstall", "--name", name, check=False)


def root():
    """What runs a command as root here: nothing on Windows (CI's user is an Administrator) or as root."""
    return [] if os.name == "nt" or os.geteuid() == 0 else ["sudo"]


def kill_node(name, port):
    """Kill the node's process outright (not its manager's own), as a crash would."""
    if os.name == "nt":  # (the node is the supervisor's child: the pondra.exe serving)
        ps = "Get-CimInstance Win32_Process -Filter \"Name='pondra.exe'\" | Where-Object { $_.CommandLine -like '* serve *' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; $_.ProcessId }"
        pid = run("powershell", "-NoProfile", "-Command", ps, check=False)
    elif sys.platform == "darwin":
        printed = run(*root(), "launchctl", "print", f"system/{name}", check=False)
        pid = next((l.split("=")[1].strip() for l in printed.splitlines() if l.strip().startswith("pid =")), "")
        pid and run(*root(), "kill", "-9", pid)
    else:
        pid = run("systemctl", "show", name, "-p", "MainPID", "--value", check=False)
        pid not in ("", "0") and run(*root(), "kill", "-9", pid)
    return pid.strip() not in ("", "0") and until(lambda: not stats(port), 10) is not None and pid.strip()


PARTS = {"image": image, "compose": compose, "chart": chart, "helm": helm, "python": python, "service": service}

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("parts", nargs="*", help=f"any of {', '.join(PARTS)} (default: image compose)")
    ap.add_argument("--image", default="pondra:dev", help="the image to try")
    ap.add_argument("--python-image", default="pondra:dev-python", help="the image with Python (python)")
    ap.add_argument("--bin", default=os.path.join(ROOT, "target", "release", "pondra"), help="the binary `pondra service` installs (service)")
    ap.add_argument("--work", default=os.path.join(ROOT, "target", "deploy-check"), help="where service's lake goes (its user must be able to write there)")
    a = ap.parse_args()
    if unknown := set(a.parts) - set(PARTS):
        ap.error(f"no such part: {', '.join(unknown)}")
    for part in a.parts or ["image", "compose"]:
        print(f"-- {part}", flush=True)
        try:
            PARTS[part](a)
        except Exception as e:
            check(f"{part} ran to its end", False, str(e)[-2000:])
    ok = bool(checks) and all(checks.values())
    print(json.dumps({"deploy": checks, "ok": ok, "info": info}, indent=1, default=str))
    sys.exit(0 if ok else 1)
