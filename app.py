"""
🔥 Stress Test Dashboard — Streamlit Frontend
Pure Python asyncio-based load tester with live metrics.
Streamlit Cloud compatible.
"""

import streamlit as st
import asyncio
import aiohttp
import time
import random
import threading
from dataclasses import dataclass, field
from typing import List
from collections import defaultdict
from datetime import datetime


# ============================================
# PAGE CONFIG
# ============================================
st.set_page_config(
    page_title="🔥 Stress Test Dashboard",
    page_icon="🔥",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ============================================
# CUSTOM CSS
# ============================================
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
    }
    .metric-card {
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        backdrop-filter: blur(10px);
    }
    .metric-value {
        font-size: 2.2em;
        font-weight: 800;
        color: #00d4ff;
    }
    .metric-label {
        font-size: 0.85em;
        color: #aaa;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    .log-box {
        background: #0d1117;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 16px;
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 13px;
        color: #c9d1d9;
        max-height: 500px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-wrap: break-word;
    }
    .status-running {
        color: #3fb950;
        font-weight: bold;
    }
    .status-stopped {
        color: #f85149;
        font-weight: bold;
    }
    .status-idle {
        color: #8b949e;
    }
    div[data-testid="stSidebar"] {
        background: rgba(15, 12, 41, 0.95);
    }
</style>
""", unsafe_allow_html=True)


# ============================================
# SESSION STATE INITIALIZATION
# ============================================
if "test_running" not in st.session_state:
    st.session_state.test_running = False
if "test_stop_requested" not in st.session_state:
    st.session_state.test_stop_requested = False
if "logs" not in st.session_state:
    st.session_state.logs = []
if "metrics" not in st.session_state:
    st.session_state.metrics = {
        "total_requests": 0,
        "successful": 0,
        "failed": 0,
        "status_5xx": 0,
        "timeouts": 0,
        "connection_errors": 0,
        "active_users": 0,
        "rps": 0.0,
        "error_rate": 0.0,
        "avg_response_ms": 0,
        "p50_ms": 0,
        "p95_ms": 0,
        "p99_ms": 0,
        "max_ms": 0,
        "status_codes": {},
        "elapsed": 0,
    }
if "test_thread" not in st.session_state:
    st.session_state.test_thread = None
if "test_completed" not in st.session_state:
    st.session_state.test_completed = False


# ============================================
# DEFAULT STAGES
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


# ============================================
# METRICS DATA CLASS
# ============================================
@dataclass
class LiveMetrics:
    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    status_5xx: int = 0
    timeouts: int = 0
    connection_errors: int = 0
    response_times: List[float] = field(default_factory=list)
    status_codes: dict = field(default_factory=lambda: defaultdict(int))

    def record(self, status: int, duration: float, error: str = None):
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
        sorted_times = sorted(self.response_times[-5000:])  # last 5000 for memory
        idx = int(len(sorted_times) * p / 100)
        return sorted_times[min(idx, len(sorted_times) - 1)]

    def to_dict(self, active_users: int, rps: float, elapsed: float) -> dict:
        return {
            "total_requests": self.total_requests,
            "successful": self.successful,
            "failed": self.failed,
            "status_5xx": self.status_5xx,
            "timeouts": self.timeouts,
            "connection_errors": self.connection_errors,
            "active_users": active_users,
            "rps": round(rps, 1),
            "error_rate": round(
                self.failed / self.total_requests * 100
                if self.total_requests else 0, 2
            ),
            "avg_response_ms": round(
                sum(self.response_times[-5000:]) / len(self.response_times[-5000:]) * 1000
                if self.response_times else 0
            ),
            "p50_ms": round(self.percentile(50) * 1000),
            "p95_ms": round(self.percentile(95) * 1000),
            "p99_ms": round(self.percentile(99) * 1000),
            "max_ms": round(max(self.response_times[-5000:]) * 1000 if self.response_times else 0),
            "status_codes": dict(self.status_codes),
            "elapsed": round(elapsed),
        }


# ============================================
# PARSE STAGES
# ============================================
def parse_stages(text: str):
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


# ============================================
# ASYNC STRESS TEST ENGINE
# ============================================
async def virtual_user(session, target_url, timeout_sec, metrics, stop_event):
    """Single virtual user loop."""
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
        metrics.record(status, duration, error)

        try:
            await asyncio.sleep(random.uniform(1, 3))
        except asyncio.CancelledError:
            return


def current_target(stages, elapsed):
    """Calculate target user count at given elapsed time."""
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


async def run_stress_test(target_url, stages, timeout_sec):
    """Main async engine that runs the load test."""
    metrics = LiveMetrics()
    stop_event = asyncio.Event()
    total_duration = sum(d for d, _ in stages)
    active_tasks = []
    user_count = 0
    start_time = time.time()

    log_msg = (
        f"🔥 STRESS TEST STARTED\n"
        f"   Target    : {target_url}\n"
        f"   Peak Users: {max(u for _, u in stages):,}\n"
        f"   Duration  : {total_duration} sec\n"
        f"   Stages    : {len(stages)}"
    )
    st.session_state.logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {log_msg}")

    connector = aiohttp.TCPConnector(
        limit=0,
        limit_per_host=0,
        ttl_dns_cache=300,
        force_close=False,
        enable_cleanup_closed=True,
    )

    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            last_log = 0

            while True:
                # Check if stop requested
                if st.session_state.test_stop_requested:
                    st.session_state.logs.append(
                        f"[{datetime.now().strftime('%H:%M:%S')}] 🛑 Test stopped by user"
                    )
                    break

                elapsed = time.time() - start_time
                if elapsed > total_duration:
                    st.session_state.logs.append(
                        f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Test completed — all stages finished"
                    )
                    break

                target = current_target(stages, elapsed)

                # Spawn new users
                while len(active_tasks) < target:
                    user_count += 1
                    task = asyncio.create_task(
                        virtual_user(session, target_url, timeout_sec, metrics, stop_event)
                    )
                    active_tasks.append(task)
                    if len(active_tasks) % 200 == 0:
                        await asyncio.sleep(0)

                # Remove excess users
                while len(active_tasks) > target:
                    task = active_tasks.pop()
                    task.cancel()

                # Update metrics in session state every 3 seconds
                if elapsed - last_log >= 3:
                    last_log = elapsed
                    rps = metrics.total_requests / max(elapsed, 1)
                    st.session_state.metrics = metrics.to_dict(
                        len(active_tasks), rps, elapsed
                    )

                    err_rate = (
                        metrics.failed / metrics.total_requests * 100
                        if metrics.total_requests else 0
                    )
                    log_line = (
                        f"[{datetime.now().strftime('%H:%M:%S')}] "
                        f"⏱ {elapsed:>6.0f}s | "
                        f"👥 Users: {len(active_tasks):>6,} | "
                        f"📨 Req: {metrics.total_requests:>8,} | "
                        f"⚡ RPS: {rps:>7.0f} | "
                        f"❌ Err: {err_rate:>5.2f}%"
                    )
                    st.session_state.logs.append(log_line)

                await asyncio.sleep(0.5)

    except Exception as e:
        st.session_state.logs.append(
            f"[{datetime.now().strftime('%H:%M:%S')}] ⚠️ Engine error: {type(e).__name__}: {e}"
        )
    finally:
        # Cancel all active tasks
        stop_event.set()
        for task in active_tasks:
            task.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)

        # Final metrics
        elapsed = time.time() - start_time
        rps = metrics.total_requests / max(elapsed, 1)
        st.session_state.metrics = metrics.to_dict(0, rps, elapsed)

        # Final report log
        final = (
            f"\n{'='*50}\n"
            f"📊 FINAL REPORT\n"
            f"{'='*50}\n"
            f"Total Requests     : {metrics.total_requests:,}\n"
            f"✅ Successful      : {metrics.successful:,}\n"
            f"❌ Failed          : {metrics.failed:,}\n"
            f"   ├─ 5xx Errors   : {metrics.status_5xx:,}\n"
            f"   ├─ Timeouts     : {metrics.timeouts:,}\n"
            f"   └─ Conn Errors  : {metrics.connection_errors:,}\n"
        )
        if metrics.total_requests:
            err = metrics.failed / metrics.total_requests * 100
            final += f"Error Rate         : {err:.2f}%\n"
        if metrics.response_times:
            avg = sum(metrics.response_times[-5000:]) / len(metrics.response_times[-5000:]) * 1000
            final += (
                f"\n⏱️  Response Times:\n"
                f"   Avg             : {avg:.0f} ms\n"
                f"   P50             : {metrics.percentile(50)*1000:.0f} ms\n"
                f"   P95             : {metrics.percentile(95)*1000:.0f} ms\n"
                f"   P99             : {metrics.percentile(99)*1000:.0f} ms\n"
                f"   Max             : {max(metrics.response_times[-5000:])*1000:.0f} ms\n"
            )
        final += f"\n📋 Status Codes:\n"
        for code, count in sorted(metrics.status_codes.items()):
            final += f"   {code:>5} : {count:,}\n"
        final += "=" * 50

        st.session_state.logs.append(f"[{datetime.now().strftime('%H:%M:%S')}] {final}")

        st.session_state.test_running = False
        st.session_state.test_completed = True


def run_test_thread(target_url, stages, timeout_sec):
    """Thread wrapper to run asyncio event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_stress_test(target_url, stages, timeout_sec))
    finally:
        loop.close()


# ============================================
# SIDEBAR — CONFIGURATION
# ============================================
with st.sidebar:
    st.markdown("## ⚙️ Configuration")
    st.markdown("---")

    target_url = st.text_input(
        "🎯 Target URL",
        value="https://example.com",
        placeholder="https://your-server.com",
        help="Enter the URL of the server you want to stress test",
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
        height=300,
        label_visibility="collapsed",
    )

    stages = parse_stages(stages_text)

    if stages:
        total_dur = sum(d for d, _ in stages)
        peak = max(u for _, u in stages)
        st.success(
            f"✅ {len(stages)} stages | {total_dur}s total | Peak: {peak:,} users"
        )
    else:
        st.error("❌ No valid stages defined")

    st.markdown("---")
    st.markdown(
        "⚠️ **Disclaimer**: Only test against servers you **OWN** "
        "or have **written permission** to test."
    )


# ============================================
# MAIN UI
# ============================================
st.markdown("# 🔥 Stress Test Dashboard")
st.markdown("*Pure Python asyncio-based load tester with live metrics*")
st.markdown("---")

# CONTROL BUTTONS
col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 4])

with col_btn1:
    start_disabled = st.session_state.test_running or not stages or not target_url.strip()
    if st.button(
        "🚀 Start Test",
        type="primary",
        disabled=start_disabled,
        use_container_width=True,
    ):
        st.session_state.test_running = True
        st.session_state.test_stop_requested = False
        st.session_state.test_completed = False
        st.session_state.logs = []
        st.session_state.metrics = {
            "total_requests": 0, "successful": 0, "failed": 0,
            "status_5xx": 0, "timeouts": 0, "connection_errors": 0,
            "active_users": 0, "rps": 0.0, "error_rate": 0.0,
            "avg_response_ms": 0, "p50_ms": 0, "p95_ms": 0,
            "p99_ms": 0, "max_ms": 0, "status_codes": {}, "elapsed": 0,
        }

        thread = threading.Thread(
            target=run_test_thread,
            args=(target_url.strip(), stages, timeout_sec),
            daemon=True,
        )
        thread.start()
        st.session_state.test_thread = thread
        st.rerun()

with col_btn2:
    if st.button(
        "🛑 Stop Test",
        disabled=not st.session_state.test_running,
        use_container_width=True,
    ):
        st.session_state.test_stop_requested = True
        st.rerun()

# STATUS
if st.session_state.test_running:
    st.markdown('<p class="status-running">● TEST RUNNING</p>', unsafe_allow_html=True)
elif st.session_state.test_completed:
    st.markdown('<p class="status-stopped">● TEST COMPLETED</p>', unsafe_allow_html=True)
else:
    st.markdown('<p class="status-idle">● IDLE — Configure and start a test</p>', unsafe_allow_html=True)

st.markdown("---")

# METRICS DASHBOARD
m = st.session_state.metrics

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("👥 Active Users", f"{m['active_users']:,}")
with col2:
    st.metric("📨 Total Requests", f"{m['total_requests']:,}")
with col3:
    st.metric("⚡ RPS", f"{m['rps']:,.1f}")
with col4:
    st.metric("⏱ Elapsed", f"{m['elapsed']}s")

col5, col6, col7, col8 = st.columns(4)
with col5:
    st.metric("✅ Successful", f"{m['successful']:,}")
with col6:
    st.metric("❌ Failed", f"{m['failed']:,}")
with col7:
    st.metric("📊 Error Rate", f"{m['error_rate']}%")
with col8:
    st.metric("🔥 5xx Errors", f"{m['status_5xx']:,}")

st.markdown("---")

# RESPONSE TIMES
st.markdown("### ⏱ Response Times")
rt1, rt2, rt3, rt4 = st.columns(4)
with rt1:
    st.metric("Avg", f"{m['avg_response_ms']} ms")
with rt2:
    st.metric("P50", f"{m['p50_ms']} ms")
with rt3:
    st.metric("P95", f"{m['p95_ms']} ms")
with rt4:
    st.metric("P99 / Max", f"{m['p99_ms']} / {m['max_ms']} ms")

# STATUS CODES
if m.get("status_codes"):
    st.markdown("### 📋 Status Codes")
    sc_cols = st.columns(min(len(m["status_codes"]), 6))
    for i, (code, count) in enumerate(sorted(m["status_codes"].items())):
        with sc_cols[i % len(sc_cols)]:
            label = "✅" if 200 <= int(code) < 400 else "⚠️" if int(code) < 500 else "🔴"
            st.metric(f"{label} {code}", f"{count:,}")

st.markdown("---")

# LOGS
st.markdown("### 📜 Live Logs")
logs_text = "\n".join(st.session_state.logs[-200:]) if st.session_state.logs else "No logs yet. Start a test to see live output."
st.markdown(f'<div class="log-box">{logs_text}</div>', unsafe_allow_html=True)

# AUTO-REFRESH while test is running
if st.session_state.test_running:
    time.sleep(3)
    st.rerun()
