import json
import time
import urllib.request

TRINO = "http://localhost:8080"


class TrinoError(RuntimeError):
    pass


def query(sql, session=None, catalog=None, schema=None, user="bench"):
    headers = {"X-Trino-User": user, "X-Trino-Source": "oti-bench"}
    if catalog:
        headers["X-Trino-Catalog"] = catalog
    if schema:
        headers["X-Trino-Schema"] = schema
    if session:
        headers["X-Trino-Session"] = ",".join(f"{k}={v}" for k, v in session.items())

    req = urllib.request.Request(f"{TRINO}/v1/statement", data=sql.encode(), headers=headers)
    t0 = time.perf_counter()
    resp = json.load(urllib.request.urlopen(req))
    columns, rows = None, []
    while True:
        if "error" in resp:
            e = resp["error"]
            raise TrinoError(f"{e.get('errorName')}: {e.get('message')}\nSQL: {sql[:300]}")
        if resp.get("columns") and columns is None:
            columns = [c["name"] for c in resp["columns"]]
        rows.extend(resp.get("data", []))
        nxt = resp.get("nextUri")
        if not nxt:
            break
        resp = json.load(urllib.request.urlopen(urllib.request.Request(nxt, headers=headers)))
    wall = time.perf_counter() - t0
    st = resp.get("stats", {})
    return {
        "query_id": resp.get("id"),
        "columns": columns or [],
        "rows": rows,
        "client_s": wall,
        "elapsed_s": st.get("elapsedTimeMillis", 0) / 1000,
        "cpu_s": st.get("cpuTimeMillis", 0) / 1000,
        "physical_input_bytes": st.get("physicalInputBytes", 0),
        "processed_rows": st.get("processedRows", 0),
        "peak_memory_bytes": st.get("peakMemoryBytes", 0),
    }


def scalar(sql, **kw):
    return query(sql, **kw)["rows"][0][0]


def query_timing(query_id):
    r = query(
        "SELECT queued_time_ms, analysis_time_ms, planning_time_ms, "
        "date_diff('millisecond', created, \"end\") "
        f"FROM system.runtime.queries WHERE query_id = '{query_id}'"
    )["rows"][0]
    return dict(zip(["queued_ms", "analysis_ms", "planning_ms", "total_ms"], r))


def run_script(path, params=None, **kw):
    text = open(path, encoding="utf-8").read()
    for k, v in (params or {}).items():
        text = text.replace("${" + k + "}", str(v))
    stmts, buf = [], []
    for line in text.splitlines():
        if line.strip().startswith("--"):
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            stmts.append("\n".join(buf).rstrip().rstrip(";"))
            buf = []
    if "".join(buf).strip():
        stmts.append("\n".join(buf))
    out = []
    for s in stmts:
        if not s.strip():
            continue
        r = query(s, **kw)
        print(f"  [{r['elapsed_s']:8.1f}s] {' '.join(s.split())[:90]}", flush=True)
        out.append((s, r))
    return out
