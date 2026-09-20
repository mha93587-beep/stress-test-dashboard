"""
🔥 Stress Test Dashboard — Streamlit Frontend with SQLite Persistence
Pure Python asyncio-based load tester with SQLite state persistence.
Survives browser refresh, tab close, and multi-client access.
"""

import os
import sys
import time
import random
import asyncio
import threading
import sqlite3
import json
import html
from datetime import datetime
from collections import deque, defaultdict
from typing import List, Tuple, Optional
import aiohttp
import streamlit as st


# ============================================
# DATABASE & PROCESS WORKER SETUP
# ============================================
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stress_test.db")

# Process-level worker tracking surviving Streamlit script reruns
if not hasattr(sys, "_stress_test_workers"):
    sys._stress_test_workers = {}
WORKERS = sys._stress_test_workers
WORKERS_LOCK = threading.Lock()


def get_db():
    """Returns a new SQLite connection with WAL mode enabled for high concurrency."""
    conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def init_db():
    """Initializes the database schema if not present."""
    with get_db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_url TEXT NOT NULL,
            status TEXT NOT NULL,
            timeout_sec INTEGER NOT NULL,
            stages_text TEXT NOT NULL,
            peak_users INTEGER DEFAULT 0,
            total_duration INTEGER DEFAULT 0,
            active_users INTEGER DEFAULT 0,
            total_requests INTEGER DEFAULT 0,
            successful INTEGER DEFAULT 0,
            failed INTEGER DEFAULT 0,
            status_5xx INTEGER DEFAULT 0,
            timeouts INTEGER DEFAULT 0,
            conn_errors INTEGER DEFAULT 0,
            rps REAL DEFAULT 0.0,
            error_rate REAL DEFAULT 0.0,
            avg_ms INTEGER DEFAULT 0,
            p50_ms INTEGER DEFAULT 0,
            p95_ms INTEGER DEFAULT 0,
            p99_ms INTEGER DEFAULT 0,
            max_ms INTEGER DEFAULT 0,
            status_codes_json TEXT DEFAULT '{}',
            elapsed_sec INTEGER DEFAULT 0,
            stop_requested INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            finished_at TIMESTAMP
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            message TEXT NOT NULL,
            FOREIGN KEY (job_id) REFERENCES jobs(id)
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_job ON logs(job_id, id);")
        conn.commit()


# Initialize DB on load
init_db()


# ============================================
# DATABASE HELPER FUNCTIONS
# ============================================
def create_job(target_url: str, stages_text: str, timeout_sec: int, peak_users: int, total_duration: int) -> int:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        cursor = conn.execute("""
            INSERT INTO jobs (
                target_url, status, timeout_sec, stages_text,
                peak_users, total_duration, started_at
            ) VALUES (?, 'RUNNING', ?, ?, ?, ?, ?)
        """, (target_url, timeout_sec, stages_text, peak_users, total_duration, now_str))
        job_id = cursor.lastrowid
        conn.commit()
    return job_id


def add_log(job_id: int, message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    with get_db() as conn:
        conn.execute(
            "INSERT INTO logs (job_id, timestamp, message) VALUES (?, ?, ?)",
            (job_id, timestamp, message)
        )
        conn.commit()


def update_job_metrics(job_id: int, metrics: dict):
    with get_db() as conn:
        conn.execute("""
            UPDATE jobs SET
                active_users = ?,
                total_requests = ?,
                successful = ?,
                failed = ?,
                status_5xx = ?,
                timeouts = ?,
                conn_errors = ?,
                rps = ?,
                error_rate = ?,
                avg_ms = ?,
                p50_ms = ?,
                p95_ms = ?,
                p99_ms = ?,
                max_ms = ?,
                status_codes_json = ?,
                elapsed_sec = ?
            WHERE id = ?
        """, (
            metrics.get("active_users", 0),
            metrics.get("total_requests", 0),
            metrics.get("successful", 0),
            metrics.get("failed", 0),
            metrics.get("status_5xx", 0),
            metrics.get("timeouts", 0),
            metrics.get("conn_errors", metrics.get("connection_errors", 0)),
            metrics.get("rps", 0.0),
            metrics.get("error_rate", 0.0),
            metrics.get("avg_response_ms", metrics.get("avg_ms", 0)),
            metrics.get("p50_ms", 0),
            metrics.get("p95_ms", 0),
            metrics.get("p99_ms", 0),
            metrics.get("max_ms", 0),
            json.dumps(metrics.get("status_codes", {})),
            metrics.get("elapsed", metrics.get("elapsed_sec", 0)),
            job_id
        ))
        conn.commit()


def mark_job_status(job_id: int, status: str):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with get_db() as conn:
        conn.execute("""
            UPDATE jobs SET status = ?, finished_at = ? WHERE id = ?
        """, (status, now_str, job_id))
        conn.commit()


def request_stop_job(job_id: int):
    with get_db() as conn:
        conn.execute("UPDATE jobs SET stop_requested = 1 WHERE id = ?", (job_id,))
        conn.commit()


def is_stop_requested(job_id: int) -> bool:
    with get_db() as conn:
        row = conn.execute("SELECT stop_requested FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return bool(row and row["stop_requested"] == 1)


def get_job(job_id: int) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return dict(row) if row else None


def get_active_job() -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE status = 'RUNNING' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        
        job = dict(row)
        job_id = job["id"]

        # Check if the worker thread is actually alive
        with WORKERS_LOCK:
            t = WORKERS.get(job_id)
            is_alive = t is not None and t.is_alive()

        # If recorded as RUNNING but thread is dead (e.g. server restarted or crashed), update status
        if not is_alive:
            mark_job_status(job_id, "STOPPED")
            job["status"] = "STOPPED"
        return job


def get_latest_job() -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 1").fetchone()
        return dict(row) if row else None


def get_all_jobs(limit: int = 15) -> List[dict]:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_recent_logs(job_id: int, limit: int = 250) -> List[str]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT timestamp, message FROM logs WHERE job_id = ? ORDER BY id DESC LIMIT ?",
            (job_id, limit)
        ).fetchall()
        # Return in ascending chronological order
        return [f"[{r['timestamp']}] {r['message']}" for r in reversed(rows)]


# ============================================
# METRICS TRACKER
# ============================================
class LiveMetrics:
    def __init__(self):
        self.total_requests = 0
        self.successful = 0
        self.failed = 0
        self.status_5xx = 0
        self.timeouts = 0
        self.connection_errors = 0
        self.response_times = deque(maxlen=5000)
        self.status_codes = defaultdict(int)
        self.lock = asyncio.Lock()

    async def record(self, status: int, duration: float, error: str = None):
        async with self.lock:
            self.total_requests += 1
            self.response_times.append(duration)
            self.status_codes[status] += 1

            if error == "timeout":
                self.timeouts += 1
                self.failed += 1
            elif error == "connection":
                self.connection_errors += 1
                self.failed += 1
            elif status == 0 or status >= 500:
                self.failed += 1
                if status >= 500:
                    self.status_5xx += 1
            else:
                self.successful += 1

    def percentile(self, p: float) -> float:
        if not self.response_times:
            return 0.0
        sorted_times = sorted(self.response_times)
        idx = int(len(sorted_times) * p / 100)
        return sorted_times[min(idx, len(sorted_times) - 1)]

    def to_dict(self, active_users: int, rps: float, elapsed: float) -> dict:
        return {
            "total_requests": self.total_requests,
            "successful": self.successful,
            "failed": self.failed,
            "status_5xx": self.status_5xx,
            "timeouts": self.timeouts,
            "conn_errors": self.connection_errors,
            "connection_errors": self.connection_errors,
            "active_users": active_users,
            "rps": round(rps, 1),
            "error_rate": round(
                self.failed / self.total_requests * 100 if self.total_requests else 0, 2
            ),
            "avg_response_ms": round(
                sum(self.response_times) / len(self.response_times) * 1000
                if self.response_times else 0
            ),
            "p50_ms": round(self.percentile(50) * 1000),
            "p95_ms": round(self.percentile(95) * 1000),
            "p99_ms": round(self.percentile(99) * 1000),
            "max_ms": round(max(self.response_times) * 1000 if self.response_times else 0),
            "status_codes": dict(self.status_codes),
            "elapsed": round(elapsed),
            "elapsed_sec": round(elapsed),
        }


# ============================================
# STAGES PARSER & HELPER
# ============================================
DEFAULT_STAGES_TEXT = """30, 1000
60, 1000
60, 5000
60, 5000
60, 10000
60, 10000
120, 25000
60, 25000
120, 50000
60, 50000
180, 100000
600, 100000
60, 50000
60, 10000
60, 0"""


def parse_stages(text: str) -> List[Tuple[int, int]]:
    stages = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(",")
        if len(parts) == 2:
            try:
                duration = int(parts[0].strip())
                users = int(parts[1].strip())
                stages.append((duration, users))
            except ValueError:
                continue
    return stages


def current_target(stages: List[Tuple[int, int]], elapsed: float) -> int:
    cumulative = 0
    prev = 0
    for duration, users in stages:
        cumulative += duration
        if elapsed < cumulative:
            time_in = elapsed - (cumulative - duration)
            progress = time_in / duration
            return int(prev + (users - prev) * progress)
        prev = users
    return 0


# ============================================
# ASYNC ENGINE (ISOLATED WORKER)
# ============================================
async def virtual_user_loop(session, target_url: str, timeout_sec: int, metrics: LiveMetrics, stop_event: asyncio.Event):
    while not stop_event.is_set():
        start = time.perf_counter()
        status = 0
        error = None
        try:
            async with session.get(
                target_url,
                timeout=aiohttp.ClientTimeout(total=timeout_sec),
                ssl=False,
            ) as resp:
                await resp.read()
                status = resp.status
        except asyncio.TimeoutError:
            error = "timeout"
        except (aiohttp.ClientConnectionError, aiohttp.ClientError):
            error = "connection"
        except asyncio.CancelledError:
            return
        except Exception:
            error = "connection"

        duration = time.perf_counter() - start
        await metrics.record(status, duration, error)

        try:
            await asyncio.sleep(random.uniform(1.0, 3.0))
        except asyncio.CancelledError:
            return


async def run_stress_test_async(job_id: int, target_url: str, stages: List[Tuple[int, int]], timeout_sec: int):
    """Main async load test engine running in a detached thread."""
    metrics = LiveMetrics()
    stop_event = asyncio.Event()
    total_duration = sum(d for d, _ in stages)
    active_tasks = []
    start_time = time.time()

    add_log(job_id, f"🚀 Job #{job_id} started: Target={target_url}, Duration={total_duration}s, Peak Users={max(u for _, u in stages):,}")

    connector = aiohttp.TCPConnector(
        limit=0,
        limit_per_host=0,
        ttl_dns_cache=300,
        force_close=False,
        enable_cleanup_closed=True,
    )

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
            last_metric_update = 0.0

            while True:
                # Check stop request from SQLite
                if is_stop_requested(job_id):
                    add_log(job_id, "🛑 Stop signal received. Gracefully terminating virtual users...")
                    break

                elapsed = time.time() - start_time
                if elapsed > total_duration:
                    add_log(job_id, "🏁 All load stages completed successfully.")
                    break

                target = current_target(stages, elapsed)

                # Spawn users
                while len(active_tasks) < target:
                    t = asyncio.create_task(
                        virtual_user_loop(session, target_url, timeout_sec, metrics, stop_event)
                    )
                    active_tasks.append(t)
                    if len(active_tasks) % 200 == 0:
                        await asyncio.sleep(0)

                # Despawn excess users
                while len(active_tasks) > target:
                    t = active_tasks.pop()
                    t.cancel()

                # Update SQLite metrics every 2 seconds
                if elapsed - last_metric_update >= 2.0:
                    last_metric_update = elapsed
                    rps = metrics.total_requests / max(elapsed, 1.0)
                    metrics_data = metrics.to_dict(len(active_tasks), rps, elapsed)
                    update_job_metrics(job_id, metrics_data)

                    err_rate = (
                        metrics.failed / metrics.total_requests * 100
                        if metrics.total_requests else 0.0
                    )
                    log_line = (
                        f"⏱ {elapsed:>5.0f}s | "
                        f"👥 Users: {len(active_tasks):>6,} | "
                        f"📨 Req: {metrics.total_requests:>8,} | "
                        f"⚡ RPS: {rps:>7.1f} | "
                        f"❌ Err: {err_rate:>5.2f}%"
                    )
                    add_log(job_id, log_line)

                await asyncio.sleep(0.5)

    except Exception as e:
        add_log(job_id, f"⚠️ Engine exception: {type(e).__name__}: {str(e)}")
    finally:
        # Cleanup
        stop_event.set()
        for t in active_tasks:
            t.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)

        elapsed = time.time() - start_time
        rps = metrics.total_requests / max(elapsed, 1.0)
        final_metrics = metrics.to_dict(0, rps, elapsed)
        update_job_metrics(job_id, final_metrics)

        # Final Summary Log
        final_report = (
            f"📊 FINAL REPORT\n"
            f"   Total Requests : {metrics.total_requests:,}\n"
            f"   ✅ Successful  : {metrics.successful:,}\n"
            f"   ❌ Failed      : {metrics.failed:,} (5xx: {metrics.status_5xx:,}, Timeouts: {metrics.timeouts:,}, ConnErr: {metrics.connection_errors:,})\n"
            f"   ⚡ Avg RPS     : {rps:.1f}\n"
            f"   ⏱ Avg Time    : {final_metrics['avg_response_ms']} ms | P95: {final_metrics['p95_ms']} ms | Max: {final_metrics['max_ms']} ms"
        )
        add_log(job_id, final_report)

        final_status = "STOPPED" if is_stop_requested(job_id) else "COMPLETED"
        mark_job_status(job_id, final_status)
        add_log(job_id, f"🏁 Job #{job_id} marked as {final_status}.")


def background_worker(job_id: int, target_url: str, stages: List[Tuple[int, int]], timeout_sec: int):
    """Entry point for the background OS thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_stress_test_async(job_id, target_url, stages, timeout_sec))
    except Exception as e:
        add_log(job_id, f"⚠️ Fatal worker error: {type(e).__name__}: {str(e)}")
        mark_job_status(job_id, "FAILED")
    finally:
        loop.close()
        with WORKERS_LOCK:
            WORKERS.pop(job_id, None)


# ============================================
# STREAMLIT UI SETUP
# ============================================
st.set_page_config(
    page_title="🔥 Stress Test Dashboard",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #0a0c1b, #1b1a38, #16182c);
    }
    .metric-card {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 10px;
        padding: 16px;
        text-align: center;
    }
    .log-box {
        background: #090d16;
        border: 1px solid #1f293d;
        border-radius: 8px;
        padding: 16px;
        font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
        font-size: 13px;
        color: #58a6ff;
        max-height: 480px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-wrap: break-word;
        line-height: 1.5;
    }
    .status-running {
        color: #3fb950;
        font-weight: 700;
        font-size: 1.1em;
    }
    .status-stopped {
        color: #f85149;
        font-weight: 700;
        font-size: 1.1em;
    }
    .status-completed {
        color: #58a6ff;
        font-weight: 700;
        font-size: 1.1em;
    }
    .status-idle {
        color: #8b949e;
        font-size: 1.1em;
    }
</style>
""", unsafe_allow_html=True)


# ============================================
# SIDEBAR CONTROLS
# ============================================
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.markdown("---")

    target_url = st.text_input(
        "🎯 Target URL",
        value="https://example.com",
        placeholder="https://your-server.com",
        help="Stress test target website URL",
    )

    timeout_sec = st.slider(
        "⏱ Request Timeout (sec)",
        min_value=5,
        max_value=120,
        value=30,
        step=5,
    )

    st.markdown("---")
    st.markdown("### 📊 Load Stages")
    st.caption("Format: `duration_sec, target_users` (one per line)")

    stages_text = st.text_area(
        "Stages Configuration",
        value=DEFAULT_STAGES_TEXT,
        height=260,
        label_visibility="collapsed",
    )

    parsed_stages = parse_stages(stages_text)

    if parsed_stages:
        total_dur = sum(d for d, _ in parsed_stages)
        peak = max(u for _, u in parsed_stages)
        st.success(f"✅ {len(parsed_stages)} stages | {total_dur}s total | Peak: {peak:,} users")
    else:
        st.error("❌ No valid stages defined")

    st.markdown("---")
    auto_refresh = st.checkbox("🔄 Auto-refresh live data (2s)", value=True)

    # Job History Selector
    all_jobs = get_all_jobs(limit=15)
    st.markdown("---")
    st.markdown("### 📜 Job History")
    job_options = {}
    for j in all_jobs:
        status_icon = "🟢" if j["status"] == "RUNNING" else "✅" if j["status"] == "COMPLETED" else "🛑"
        job_options[j["id"]] = f"Job #{j['id']} {status_icon} ({j['target_url'][:25]})"

    selected_job_id = None
    if job_options:
        selected_job_id = st.selectbox(
            "Select Job to view",
            options=list(job_options.keys()),
            format_func=lambda x: job_options[x],
            index=0,
        )

    st.markdown("---")
    st.caption("⚠️ Only run tests against systems you own or have permission to test.")


# ============================================
# DETERMINE ACTIVE JOB
# ============================================
# Check if there is an active running job in SQLite
current_running_job = get_active_job()

# If user selected a job from history dropdown, show that job; otherwise show current active or latest job
if current_running_job:
    display_job = current_running_job
elif selected_job_id:
    display_job = get_job(selected_job_id)
else:
    display_job = get_latest_job()

is_running = bool(current_running_job)


# ============================================
# MAIN DASHBOARD UI
# ============================================
st.markdown("# 🔥 Stress Test Dashboard")
st.markdown("*High-performance load testing engine with SQLite state persistence*")
st.markdown("---")

# Controls row
c_btn1, c_btn2, c_btn3, c_status = st.columns([1.2, 1.2, 1, 3.5])

with c_btn1:
    start_disabled = is_running or not parsed_stages or not target_url.strip()
    if st.button("🚀 Start Test", type="primary", disabled=start_disabled, use_container_width=True):
        total_dur = sum(d for d, _ in parsed_stages)
        peak = max(u for _, u in parsed_stages)
        new_job_id = create_job(
            target_url.strip(), stages_text, timeout_sec, peak, total_dur
        )
        
        # Start worker thread
        t = threading.Thread(
            target=background_worker,
            args=(new_job_id, target_url.strip(), parsed_stages, timeout_sec),
            daemon=True,
        )
        with WORKERS_LOCK:
            WORKERS[new_job_id] = t
        t.start()

        st.toast(f"🚀 Job #{new_job_id} launched!")
        st.rerun()

with c_btn2:
    stop_disabled = not is_running
    if st.button("🛑 Stop Test", disabled=stop_disabled, use_container_width=True):
        if current_running_job:
            request_stop_job(current_running_job["id"])
            st.toast(f"🛑 Stopping Job #{current_running_job['id']}...")
            time.sleep(0.5)
            st.rerun()

with c_btn3:
    if st.button("🔄 Refresh", use_container_width=True):
        st.rerun()

with c_status:
    if display_job:
        status_text = display_job["status"]
        job_num = display_job["id"]
        target = display_job["target_url"]
        if status_text == "RUNNING":
            st.markdown(f'<p class="status-running">● TEST RUNNING — Job #{job_num} ({target})</p>', unsafe_allow_html=True)
        elif status_text == "COMPLETED":
            st.markdown(f'<p class="status-completed">✔ COMPLETED — Job #{job_num} ({target})</p>', unsafe_allow_html=True)
        else:
            st.markdown(f'<p class="status-stopped">■ {status_text} — Job #{job_num} ({target})</p>', unsafe_allow_html=True)
    else:
        st.markdown('<p class="status-idle">○ IDLE — Configure target URL & click Start Test</p>', unsafe_allow_html=True)

st.markdown("---")

# ============================================
# METRICS DISPLAY
# ============================================
if display_job:
    j = display_job
    status_codes = {}
    if j.get("status_codes_json"):
        try:
            status_codes = json.loads(j["status_codes_json"])
        except Exception:
            status_codes = {}

    # Metrics row 1
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("👥 Active Users", f"{j['active_users']:,}")
    with m2:
        st.metric("📨 Total Requests", f"{j['total_requests']:,}")
    with m3:
        st.metric("⚡ RPS", f"{j['rps']:,.1f}")
    with m4:
        st.metric("⏱ Elapsed Time", f"{j['elapsed_sec']}s / {j['total_duration']}s")

    # Metrics row 2
    m5, m6, m7, m8 = st.columns(4)
    with m5:
        st.metric("✅ Successful", f"{j['successful']:,}")
    with m6:
        st.metric("❌ Failed", f"{j['failed']:,}")
    with m7:
        st.metric("📊 Error Rate", f"{j['error_rate']:.2f}%")
    with m8:
        st.metric("🔥 5xx Errors", f"{j['status_5xx']:,}")

    st.markdown("---")

    # Response times row
    st.markdown("### ⏱ Response Times")
    r1, r2, r3, r4 = st.columns(4)
    with r1:
        st.metric("Average", f"{j['avg_ms']} ms")
    with r2:
        st.metric("P50 (Median)", f"{j['p50_ms']} ms")
    with r3:
        st.metric("P95", f"{j['p95_ms']} ms")
    with r4:
        st.metric("P99 / Max", f"{j['p99_ms']} / {j['max_ms']} ms")

    # Status codes row
    if status_codes:
        st.markdown("### 📋 Status Codes Breakdown")
        sc_cols = st.columns(min(len(status_codes), 6))
        for idx, (code, count) in enumerate(sorted(status_codes.items())):
            with sc_cols[idx % len(sc_cols)]:
                tag = "✅" if code.isdigit() and 200 <= int(code) < 400 else "⚠️" if code.isdigit() and int(code) < 500 else "🔴"
                st.metric(f"{tag} HTTP {code}", f"{count:,}")

    st.markdown("---")

    # Logs section
    st.markdown(f"### 📜 Live Logs (Job #{j['id']})")
    logs = get_recent_logs(j["id"], limit=250)
    logs_content = "\n".join(logs) if logs else "No logs recorded yet for this job."
    escaped_logs = html.escape(logs_content)
    st.markdown(f'<div class="log-box">{escaped_logs}</div>', unsafe_allow_html=True)

else:
    st.info("👈 Enter a target URL in the sidebar and click **Start Test** to begin!")


# ============================================
# AUTO-REFRESH WHEN TEST IS RUNNING
# ============================================
if is_running and auto_refresh:
    time.sleep(2.0)
    st.rerun()
