#!/usr/bin/env python3
"""End-to-end tests for segment original-copy verification (SHA-256 re-fetch).

Covers:
  V1 first save records a SHA-256; segments re-fetched from later playlists
     become 'verified'; a segment that slides out first is 'no_opportunity'
     and is never reported as consistent
  V2 same seq+URL TS rewrite -> conflict -> job failed, download forbidden
  V3 conflict and pending-verification states survive a restart
  V4 stop while verifications are in flight: stop is immediate and no ZIP is
     published until every re-fetch settles
  V5 non-blocking: slow verification re-fetches do not block stop/query/other jobs
  V6 segment detail endpoint and web page status wording
"""
import os
import subprocess
import sys
import time

import httpx

SRC = os.environ.get("SRC", "http://127.0.0.1:8081")
PORT = int(os.environ.get("TEST_PORT", "0"))
if PORT == 0:
    import socket
    s = socket.socket(); s.bind(("127.0.0.1", 0)); PORT = s.getsockname()[1]; s.close()
APP = f"http://127.0.0.1:{PORT}"
DATA = f"/tmp/recdata_verify_{os.getpid()}"
os.makedirs(DATA, exist_ok=True)
SEED = open(os.environ.get("SEED_TS", "/tmp/livesrc/seed_real.ts"), "rb").read()

results = []


def check(name, cond, detail=""):
    results.append((name, bool(cond)))
    print(("PASS " if cond else "FAIL "), name, ("" if cond else f"  -> {detail}"))


def api(method, path, **kw):
    return getattr(httpx, method)(f"{APP}{path}", timeout=15, **kw)


def get_job(jid):
    return api("get", f"/api/jobs/{jid}").json()


def wait_state(jid, states, timeout=90):
    t0 = time.time()
    j = get_job(jid)
    while time.time() - t0 < timeout:
        j = get_job(jid)
        if j["state"] in states:
            return j
        time.sleep(0.4)
    return j


def wait_saved(jid, n, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        j = get_job(jid)
        if j["saved"] >= n:
            return j
        time.sleep(0.4)
    return get_job(jid)


def start_job(url):
    r = api("post", "/api/jobs", json={"url": url})
    assert r.status_code == 200, r.text
    return r.json()["id"]


app_proc = None


def start_app():
    global app_proc
    log = open("/tmp/app_verify.log", "ab")
    app_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "src.main:app",
         "--host", "127.0.0.1", "--port", str(PORT)],
        cwd=os.environ.get("APP_DIR", "/workspace/app"),
        env={**os.environ, "DATA_DIR": DATA, "RECORDER_TEST_HOOKS": "1"},
        stdout=log, stderr=subprocess.STDOUT)
    for _ in range(60):
        if app_proc.poll() is not None:
            raise RuntimeError("app exited during startup")
        try:
            api("get", "/api/jobs")
            return
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("app did not start")


def crash_app():
    global app_proc
    try:
        api("post", "/api/_test/crash")
    except Exception:
        pass
    if app_proc:
        app_proc.wait(timeout=10)
    app_proc = None
    for _ in range(40):
        try:
            api("get", "/api/jobs")
        except Exception:
            return
        time.sleep(0.25)
    raise RuntimeError("port still in use after crash")


def seqs_of(jid, status):
    rows = api("get", f"/api/jobs/{jid}/segments").json()
    return [r["seq"] for r in rows if r["verify_status"] == status], rows


try:
    start_app()

    # ---------- V1: verify + no_opportunity semantics ----------
    # First: hold verifications back so the earliest segments slide out of the
    # window without any re-fetch chance (deterministic no_opportunity case).
    api("post", "/api/_test/verify_delay/30")
    jid = start_job(f"{SRC}/live.m3u8")
    time.sleep(12)
    mid = get_job(jid)
    check("V1 录制中存在已保存但尚未复核的片段",
          mid["saved"] >= 5
          and mid["verify"]["unverified"] + mid["verify"]["verifying"]
              + mid["verify"]["no_opportunity"] >= 1, str(mid["verify"]))
    api("post", "/api/_test/verify_delay/0")
    rows = api("get", f"/api/jobs/{jid}/segments").json()
    hashes = {r["sha256"] for r in rows if r["status"] == "done"}
    import hashlib
    check("V1 首次保存即记录 SHA-256（与原件一致）",
          all(r["sha256"] for r in rows if r["status"] == "done")
          and hashlib.sha256(SEED).hexdigest() in hashes,
          str(hashes)[:120])
    check("V1 待复核/复取中片段不得被报为一致",
          all(r["verify_status"] != "verified"
              for r in rows if r["verify_status"] in ("unverified", "verifying", "no_opportunity"))
          and any(r["verify_status"] in ("unverified", "verifying", "no_opportunity")
                  for r in rows),
          str([(r["seq"], r["verify_status"]) for r in rows]))
    check("V1 滑出窗口且未复取的片段标为未获得复核机会（不说成一致）",
          any(r["verify_status"] == "no_opportunity" for r in rows),
          str([(r["seq"], r["verify_status"]) for r in rows]))
    api("post", f"/api/jobs/{jid}/stop")
    j = wait_state(jid, {"done", "failed"}, 60)
    check("V1 正常录制完成", j["state"] == "done", str(j["state"]))
    v = j["verify"]
    check("V1 完成时无冲突、无悬而未决的复核",
          v["conflict"] == 0 and v["verifying"] == 0 and v["unverified"] == 0, str(v))
    check("V1 未获得复核机会状态保留到最终结果", v["no_opportunity"] >= 1, str(v))
    check("V1 窗口内片段经复取复核一致", v["verified"] >= 1, str(v))

    # ---------- V2: same seq+URL rewrite -> conflict ----------
    now = httpx.get(f"{SRC}/health", timeout=5).json()["now_seq"]
    target = now + 3
    jid = start_job(f"{SRC}/live.m3u8?rewrite={target}")
    j = wait_state(jid, {"failed"}, 90)
    check("V2 同序号同地址TS改写判失败", j["state"] == "failed", str(j["state"]))
    check("V2 冲突序号被记录", target in (j["conflicts"] or []), str(j["conflicts"]))
    check("V2 冲突计数=1", j["verify"]["conflict"] == 1, str(j["verify"]))
    check("V2 错误信息含复核冲突", "复核冲突" in (j["error"] or ""), str(j["error"]))
    r = api("get", f"/api/jobs/{jid}/download")
    check("V2 冲突任务禁止下载(409)", r.status_code == 409, f"got {r.status_code}")
    _, rows = seqs_of(jid, "conflict")
    crow = next(r for r in rows if r["seq"] == target)
    check("V2 冲突行保留首次 SHA-256 且状态为 conflict",
          crow["sha256"] and crow["verify_status"] == "conflict", str(crow))
    conflict_jid = jid

    # ---------- V3: states survive restart ----------
    # also create a fresh recording and crash while it still has unverified segs
    jid2 = start_job(f"{SRC}/live.m3u8")
    wait_saved(jid2, 2, 30)
    crash_app()
    start_app()
    jc = get_job(conflict_jid)
    check("V3 冲突状态重启后保留", jc["state"] == "failed"
          and jc["verify"]["conflict"] == 1 and target in jc["conflicts"], str(jc))
    r = api("get", f"/api/jobs/{conflict_jid}/download")
    check("V3 重启后冲突任务仍禁止下载", r.status_code == 409, f"got {r.status_code}")
    j2 = get_job(jid2)
    rows2 = api("get", f"/api/jobs/{jid2}/segments").json()
    check("V3 待复核状态重启后保留（无残留 verifying）",
          j2["state"] == "recording"
          and all(r["verify_status"] != "verifying" for r in rows2)
          and any(r["verify_status"] in ("unverified", "no_opportunity", "verified")
                  for r in rows2 if r["status"] == "done"),
          str([(r["seq"], r["verify_status"]) for r in rows2]))
    api("post", f"/api/jobs/{jid2}/stop")
    j2 = wait_state(jid2, {"done", "failed"}, 60)
    check("V3 重启续录后可正常完成", j2["state"] == "done", str(j2["state"]))

    # ---------- V4: stop during in-flight verification gates the ZIP ----------
    api("post", "/api/_test/verify_delay/3")
    jid = start_job(f"{SRC}/live.m3u8")
    time.sleep(6)
    pre = get_job(jid)
    check("V4 预备：存在复取进行中片段", pre["verify"]["verifying"] >= 1, str(pre["verify"]))
    t0 = time.time()
    r = api("post", f"/api/jobs/{jid}/stop")
    dt = time.time() - t0
    check("V4 停止即时受理不被复取阻塞(<1s)", r.status_code == 200 and dt < 1.0, f"{dt:.2f}s")
    time.sleep(0.5)
    mid = get_job(jid)
    check("V4 复核未结束不发布ZIP（仍 finalizing）", mid["state"] == "finalizing"
          and mid["download_url"] is None, str((mid["state"], mid["download_url"])))
    j = wait_state(jid, {"done", "failed"}, 90)
    check("V4 全部复核落定后才完成", j["state"] == "done"
          and j["verify"]["verifying"] == 0 and j["verify"]["unverified"] == 0, str(j["verify"]))
    r = api("get", f"/api/jobs/{jid}/download")
    check("V4 完成后 ZIP 可下载", r.status_code == 200, f"got {r.status_code}")
    api("post", "/api/_test/verify_delay/0")

    # ---------- V5: slow verification does not block other work ----------
    api("post", "/api/_test/verify_delay/3")
    slow = start_job(f"{SRC}/live.m3u8?delay_ms=500")
    other = start_job(f"{SRC}/live.m3u8")
    time.sleep(6)
    t0 = time.time()
    api("post", f"/api/jobs/{slow}/stop")
    dt_stop = time.time() - t0
    t0 = time.time()
    api("get", "/api/jobs")
    dt_q = time.time() - t0
    check("V5 慢复核不阻塞停止受理(<1s)", dt_stop < 1.0, f"{dt_stop:.2f}s")
    check("V5 慢复核不阻塞查询(<1s)", dt_q < 1.0, f"{dt_q:.2f}s")
    jo = wait_saved(other, 2, 30)
    check("V5 另一路录制照常推进", jo["saved"] >= 2, str(jo["saved"]))
    api("post", f"/api/jobs/{other}/stop")
    js = wait_state(slow, {"done", "failed"}, 120)
    jo = wait_state(other, {"done", "failed"}, 120)
    check("V5 两路最终均完成", js["state"] == "done" and jo["state"] == "done",
          f"{js['state']}/{jo['state']}")
    api("post", "/api/_test/verify_delay/0")

    # ---------- V6: API + web page wording ----------
    rows = api("get", f"/api/jobs/{jid}/segments").json()
    keys = {"seq", "status", "sha256", "verify_status", "verify_attempts"}
    check("V6 片段明细接口字段完整", rows and keys.issubset(rows[0].keys()), str(rows[0].keys()))
    page = api("get", "/").text
    for word in ["已复核", "待复核", "未获得复核机会", "内容冲突"]:
        check(f"V6 网页区分复核状态（{word}）", word in page, word)
    check("V6 网页提示不得把未复核说成一致", "不得视为一致" in page, "missing wording")

finally:
    crash_app()

print("\n==== SUMMARY ====")
failed = [n for n, ok in results if not ok]
print(f"{len(results) - len(failed)}/{len(results)} passed")
if failed:
    print("FAILED:", *failed, sep="\n  - ")
    sys.exit(1)
print("ALL PASS")
