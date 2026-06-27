#!/usr/bin/env python3
"""
Chrome 内存监控工具 - Python + CDP 版
======================================
通过 Chrome DevTools Protocol 实时采集浏览器的 JS 堆内存、
DOM节点、事件监听器等详细信息。

采集策略（双层，各自独立定时器）：
- 分配采样（Allocation Sampling）：HeapProfiler.startSampling → 持续录制 → stopSampling 保存 → restart 继续录制
- 堆快照（Heap Snapshot）：HeapProfiler.takeHeapSnapshot 按单独频率定期保存

使用方式：
  start-monitor-py.bat      # 自动安装依赖并启动
  或：
  pip install websockets
  python chrome-memory-monitor.py

"""

import asyncio
import json
import os
import re
import signal
import subprocess
import sys
import time
import csv
from datetime import datetime
from pathlib import Path

# ==================== 配置（可根据需要修改）====================
TARGET_URL = ""                  # 监控目标 URL（留空则不自动导航，用户在浏览器手动输入地址）
CDP_PORT = 9222
METRIC_INTERVAL = 10             # 指标数据采集间隔（秒）

# ===== 分配采样（Allocation Profile）配置 =====
SAMPLING_ENABLED = True                  # 开启/关闭分配采样
SAMPLING_INTERVAL_NORMAL = 60            # 正常模式：分配采样保存间隔（秒）
SAMPLING_INTERVAL_HIGH = 30              # 高频模式：分配采样保存间隔（秒），内存 >80% 时
SAMPLING_INTERVAL_CRIT = 5               # 紧急模式：分配采样保存间隔（秒），内存 >95% 时

# ===== 堆快照（Heap Snapshot）配置 =====
SNAPSHOT_ENABLED = True                  # 开启/关闭堆快照
SNAPSHOT_INTERVAL_NORMAL = 60            # 正常模式：堆快照间隔（分钟），默认60分钟
SNAPSHOT_INTERVAL_HIGH = 10              # 高频模式：堆快照间隔（分钟），内存 >80% 时，默认10分钟
SNAPSHOT_INTERVAL_CRIT = 5               # 紧急模式：堆快照间隔（分钟），内存 >95% 时，默认5分钟

# 阈值基于浏览器 JS 堆上限（jsHeapSizeLimit）的百分比
HIGH_PERCENT = 0.80           # 高频采样阈值：JS 堆占用超上限 80% 时加快采样
CRIT_PERCENT = 0.95           # 紧急快照阈值：JS 堆占用超上限 95% 时立即保存快照
METRIC_INTERVAL_FAST = 3        # 高频指标采集间隔（秒），超过 HIGH_PERCENT 后使用此间隔

MAX_SAMPLING_FILES = 100       # 最大保留分配采样文件数（.heapprofile），超过自动删除最旧的
MAX_SNAPSHOT_FILES = 20        # 最大保留堆快照文件数（.heapsnapshot），超过自动删除最旧的
SAMPLING_INTERVAL_US = 1000000  # 分配采样内部间隔（微秒），1000000 = 1秒

SCRIPT_DIR = Path(__file__).parent
LOG_DIR = SCRIPT_DIR / "chrome-memory-logs"
SNAP_DIR = LOG_DIR / "snapshots"
DATE_STR = datetime.now().strftime("%Y-%m-%d")
CSV_FILE = LOG_DIR / f"memory-{DATE_STR}.csv"
LOG_FILE = LOG_DIR / f"monitor-{DATE_STR}.txt"

# ==================== 全局状态 ====================
class MonitorState:
    def __init__(self):
        self.used_mb = 0.0
        self.total_mb = 0.0
        self.limit_mb = 0.0
        self.process_memory_mb = 0.0   # 进程级内存（任务管理器数值）
        self.process_pid = None        # Chrome 渲染进程 PID
        self.dom_nodes = 0
        self.listeners = 0
        self.documents = 0
        self.frames = 0
        self.layout_count = 0
        self.style_count = 0
        self.running = True
        self.snapshot_count = 0
        self.sampling_count = 0
        self.elapsed = 0
        self.high_freq = False
        self.ws = None
        self.history = []        # [(used_mb, ts), ...]
        self.process_history = []  # [process_memory_mb, ...]
        self.high_mem_mb = 99999  # 首次采集后根据 jsHeapSizeLimit 动态计算
        self.crit_mem_mb = 99999
        self._thresh_set = False
        self._msg_id = 0
        self._snap_chunks = []   # 堆快照数据块
        self._pending = {}       # msg_id -> asyncio.Event

state = MonitorState()
LOG_LINES = []        # 日志缓冲区（仪表盘下方展示最近 N 条）


# ==================== 日志 & 文件 ====================
MAX_LOG_LINES = 15    # 仪表盘下方最多显示的日志行数

def log(msg, color=""):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    # 写入缓冲区，用于仪表盘下方实时展示
    LOG_LINES.append(line)
    if len(LOG_LINES) > MAX_LOG_LINES:
        LOG_LINES.pop(0)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def ensure_dirs():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    SNAP_DIR.mkdir(parents=True, exist_ok=True)

def init_csv():
    if not CSV_FILE.exists():
        with open(CSV_FILE, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow([
                "timestamp", "elapsed_sec",
                "used_js_heap_mb", "total_js_heap_mb", "heap_limit_mb",
                "process_memory_mb",  # 任务管理器数值
                "dom_nodes", "js_event_listeners", "documents", "frames"
            ])

def append_csv(u, t, l, pm, d, ls, docs, frames):
    with open(CSV_FILE, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            state.elapsed,
            f"{u:.2f}", f"{t:.2f}", f"{l:.0f}",
            f"{pm:.1f}",  # 进程内存
            d, ls, docs, frames
        ])

def clean_old_snapshots(keep=MAX_SNAPSHOT_FILES):
    """删除超过数量的旧快照"""
    files = sorted(SNAP_DIR.glob("*.heapsnapshot"), key=os.path.getmtime)
    while len(files) > keep:
        files[0].unlink(missing_ok=True)
        files = files[1:]


# ==================== 进程内存采集（任务管理器数值）====================
def find_chrome_pid():
    """通过 netstat 找到监听调试端口的 Chrome 进程 PID"""
    try:
        r = subprocess.run(
            ["netstat", "-ano"],
            capture_output=True, text=True, timeout=10
        )
        for line in r.stdout.split("\n"):
            if f":{CDP_PORT}" in line and "LISTENING" in line:
                parts = line.strip().split()
                pid = parts[-1]
                return int(pid)
    except:
        pass
    return None

def get_process_memory(pid):
    """查询指定 PID 的进程工作集内存（MB）"""
    try:
        r = subprocess.run(
            ["tasklist", "/fi", f"pid eq {pid}", "/fo", "csv", "/nh"],
            capture_output=True, text=True, timeout=5
        )
        for line in r.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.strip().split('","')
            if len(parts) >= 5:
                mem_text = parts[4].replace('"', '').replace(',', '').replace(' K', '').strip()
                return int(mem_text) / 1024
    except:
        pass
    return 0

def get_all_chrome_memory():
    """获取所有 Chrome 进程总内存（MB），等效任务管理器汇总"""
    try:
        r = subprocess.run(
            ["tasklist", "/fi", "imagename eq chrome.exe", "/fo", "csv", "/nh"],
            capture_output=True, text=True, timeout=5
        )
        total_kb = 0
        for line in r.stdout.strip().split("\n"):
            if not line.strip() or "chrome.exe" not in line.lower():
                continue
            parts = line.strip().split('","')
            if len(parts) >= 5:
                mem_text = parts[4].replace('"', '').replace(',', '').replace(' K', '').strip()
                if mem_text.isdigit():
                    total_kb += int(mem_text)
        return total_kb / 1024
    except:
        return 0


# ==================== Chrome 管理 ====================
def find_chrome():
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expanduser(r"~\AppData\Local\Google\Chrome\Application\chrome.exe"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return p
    try:
        r = subprocess.run(
            ["reg", "query", r"HKLM\SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\chrome.exe", "/ve"],
            capture_output=True, text=True, timeout=5
        )
        m = re.search(r'REG_SZ\s+(.+)', r.stdout)
        if m:
            return m.group(1).strip()
    except:
        pass
    raise FileNotFoundError("未找到 Chrome，请手动安装")

def is_chrome_debug_ready():
    """检查 Chrome 调试端口是否已就绪"""
    import urllib.request
    try:
        urllib.request.urlopen(f"http://localhost:{CDP_PORT}/json/version", timeout=2)
        return True
    except:
        return False

def kill_all_chrome():
    """彻底杀死所有 Chrome 进程并等待完全退出"""
    try:
        subprocess.run(["taskkill", "/f", "/im", "chrome.exe"],
                       capture_output=True, timeout=5)
    except:
        pass
    # 等待所有进程完全退出
    for _ in range(20):
        try:
            r = subprocess.run(["tasklist", "/nh", "/fo", "csv"],
                               capture_output=True, text=True, timeout=3)
            if "chrome.exe" not in r.stdout.lower():
                break
        except:
            pass
        time.sleep(0.5)

def start_chrome():
    # 优先复用：检查端口是否已就绪（可能已有 Chrome 以 --remote-debugging-port 启动）
    if is_chrome_debug_ready():
        log("✅ 检测到 Chrome 调试端口已就绪，直接复用", "green")
        return

    path = find_chrome()
    log(f"启动 Chrome: {path}")

    # 使用独立用户数据目录，避免与已有 Chrome 实例冲突
    temp_dir = SCRIPT_DIR / ".chrome-debug-profile"
    temp_dir.mkdir(parents=True, exist_ok=True)

    args = [
        path,
        f"--remote-debugging-port={CDP_PORT}",
        f"--user-data-dir={temp_dir}",
        "--enable-precise-memory-info",
        "--no-first-run",
        "--no-default-browser-check",
        "about:blank",
    ]
    log(f"Chrome 启动参数: {' '.join(args)}")
    subprocess.Popen(args, shell=False)
    log("等待 Chrome 初始化（约 6 秒）...")
    # 等待端口就绪
    for i in range(15):
        if is_chrome_debug_ready():
            log(f"Chrome 已就绪，耗时约 {i+1} 秒")
            return
        time.sleep(1)

    log("❌ Chrome 启动超时，请手动用以下命令启动 Chrome 后重新运行脚本：", "red")
    log(f'   "{path}" --remote-debugging-port={CDP_PORT}', "red")


# ==================== CDP 通信 ====================
import websockets

async def get_pages():
    """获取 Chromium 页面的 WebSocket URL"""
    import urllib.request
    try:
        resp = urllib.request.urlopen(
            f"http://localhost:{CDP_PORT}/json", timeout=5)
        return json.loads(resp.read())
    except Exception as e:
        log(f"CDP 连接失败: {e}")
        return []


# 消息分发
async def cdp_listener():
    """持续监听 WebSocket 消息并分发"""
    ws = state.ws
    try:
        while state.running and ws:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                msg = json.loads(raw)
                mid = msg.get("id")
                method = msg.get("method")

                # 响应（有 id）
                if mid is not None and mid in state._pending:
                    evt = state._pending.pop(mid)
                    evt.result = msg
                    evt.set()

                # 堆快照数据块
                elif method == "HeapProfiler.addHeapSnapshotChunk":
                    chunk = msg["params"]["chunk"]
                    state._snap_chunks.append(chunk)

                # 堆快照进度
                elif method == "HeapProfiler.reportHeapSnapshotProgress":
                    pass

            except asyncio.TimeoutError:
                continue
            except websockets.ConnectionClosed:
                log("WebSocket 已断开", "yellow")
                break
            except Exception as e:
                log(f"CDP 接收异常: {e}", "yellow")
    except Exception as e:
        log(f"CDP Listener 异常: {e}", "red")


async def send_cdp(method, params=None, timeout=15):
    """发送 CDP 命令并等待响应"""
    if not state.ws:
        return None

    state._msg_id += 1
    mid = state._msg_id
    evt = asyncio.Event()
    evt.result = None
    state._pending[mid] = evt

    req = {"id": mid, "method": method}
    if params:
        req["params"] = params

    try:
        await state.ws.send(json.dumps(req))
    except websockets.ConnectionClosed:
        log("WebSocket 已关闭，无法发送命令", "yellow")
        return None

    try:
        await asyncio.wait_for(evt.wait(), timeout=timeout)
        return evt.result
    except asyncio.TimeoutError:
        state._pending.pop(mid, None)
        log(f"CDP 调用超时: {method}", "yellow")
        return {"error": "timeout"}


async def collect_metrics():
    """采集页面内存和性能指标（带容错：首个超时则跳过本周期剩余调用）"""
    mem = {}
    dom = {}
    perf = {}

    # 1. JS 堆内存 + DOM 节点数（最核心指标）
    try:
        resp = await send_cdp("Runtime.evaluate", {
            "expression": (
                "(function(){"
                "try{var m=performance.memory;"
                "return JSON.stringify({"
                "u:m.usedJSHeapSize,t:m.totalJSHeapSize,l:m.jsHeapSizeLimit,"
                "d:document.querySelectorAll('*').length"
                "});}catch(e){return '{}';}"
                "})()"
            ),
            "returnByValue": True,
            "silent": True,
        }, timeout=15)
        if resp and "result" in resp and "result" in resp["result"]:
            val = resp["result"]["result"].get("value")
            if val and val != "{}":
                try:
                    mem = json.loads(val)
                except:
                    pass
    except asyncio.TimeoutError:
        # 最核心指标超时 → V8 正忙，后续调用大概率也超时，直接跳过本周期
        log("⚠️ Runtime.evaluate 超时（V8 忙碌），跳过本周期剩余采集", "yellow")
        return {**mem, **dom, **perf}
    except Exception as e:
        log(f"⚠️ Runtime.evaluate 异常: {e}，跳过本周期", "yellow")
        return {**mem, **dom, **perf}

    # 2. DOM 计数器：事件监听器、文档数
    try:
        resp = await send_cdp("Memory.getDOMCounters", timeout=15)
        if resp and "result" in resp:
            dom = resp["result"]
    except asyncio.TimeoutError:
        log("⚠️ Memory.getDOMCounters 超时，使用上一次值", "yellow")
    except Exception:
        pass

    # 3. 性能指标（非核心，超时静默）
    try:
        resp = await send_cdp("Performance.getMetrics", timeout=15)
        if resp and "result" in resp and "metrics" in resp["result"]:
            perf = {m["name"]: m["value"] for m in resp["result"]["metrics"]}
    except asyncio.TimeoutError:
        pass
    except Exception:
        pass

    return {**mem, **dom, **perf}


async def _resume_sampling():
    """恢复分配采样（仅在 SAMPLING_ENABLED 时执行）"""
    if not SAMPLING_ENABLED:
        return
    await send_cdp("HeapProfiler.startSampling", {
        "samplingInterval": SAMPLING_INTERVAL_US
    })
    log("▶️ 已恢复分配采样", "cyan")


async def take_snapshot():
    """保存堆快照到文件（先停分配采样避免 HeapProfiler 域冲突）
    
    首次快照可能超时（V8 需要冷启动：强制 GC + 构建堆图），
    超时后自动重试一次（V8 已热身，大概率成功）。
    """
    # 内存高压时跳过（V8 全力 GC，takeHeapSnapshot 会超时或卡死浏览器）
    ratio = state.used_mb / max(state.limit_mb, 1)
    if ratio >= CRIT_PERCENT:
        log("⚠️ 内存使用率过高，跳过堆快照（分配采样已保存）", "yellow")
        return None

    # 堆快照 & 分配采样共用 HeapProfiler 域，只在采样启用时才需要暂停/恢复
    if SAMPLING_ENABLED:
        await send_cdp("HeapProfiler.stopSampling")
        log("⏸️ 已暂停分配采样（准备堆快照）", "yellow")

    # 最多尝试 2 次：首次可能因 V8 冷启动超时，重试时 V8 已热身
    max_attempts = 2
    snapshot_timeout = 120  # 大堆首次快照需要较长时间

    for attempt in range(1, max_attempts + 1):
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            fp = SNAP_DIR / f"heap-{ts}.heapsnapshot"

            # 清空之前的数据
            state._snap_chunks.clear()

            if attempt == 1:
                log("📸 开始采集堆快照...")
            else:
                log(f"📸 重试采集堆快照（第 {attempt} 次，V8 已热身）...")

            # 触发快照（数据通过 addHeapSnapshotChunk 事件返回）
            resp = await send_cdp("HeapProfiler.takeHeapSnapshot", {"reportProgress": True}, timeout=snapshot_timeout)
            if not resp or resp.get("error"):
                if resp and resp.get("error") == "timeout" and attempt < max_attempts:
                    log(f"⚠️ 堆快照超时（V8 首次冷启动，将自动重试）", "yellow")
                    continue
                log(f"❌ 快照命令失败: {resp}", "red")
                await _resume_sampling()
                return None

            # 等待所有 chunks 到达
            await asyncio.sleep(1)

            # 再等几轮确认接收完毕
            for _ in range(5):
                old_count = len(state._snap_chunks)
                await asyncio.sleep(1)
                if len(state._snap_chunks) == old_count and old_count > 0:
                    break

            if not state._snap_chunks:
                if attempt < max_attempts:
                    log("⚠️ 未收到快照数据，将自动重试", "yellow")
                    continue
                log("⚠️ 未收到快照数据", "yellow")
                await _resume_sampling()
                return None

            log(f"💾 正在流式写入快照文件...")
            with open(fp, "w", encoding="utf-8") as f:
                for chunk in state._snap_chunks:
                    f.write(chunk)
                    
            mb = os.path.getsize(fp) / 1024 / 1024
            state.snapshot_count += 1
            log(f"✅ 快照已保存: {fp.name} ({mb:.2f} MB){"  (重试成功)" if attempt > 1 else ""}")
            
            clean_old_snapshots()

            await _resume_sampling()
            return fp

        except asyncio.TimeoutError:
            if attempt < max_attempts:
                log("⚠️ 堆快照超时（V8 首次冷启动），将自动重试", "yellow")
                continue
            log("❌ 堆快照超时，已放弃", "red")
            try:
                await _resume_sampling()
            except:
                pass
            return None
        except Exception as e:
            if attempt < max_attempts:
                log(f"⚠️ 堆快照异常: {e}，将自动重试", "yellow")
                continue
            log(f"❌ 堆快照异常: {e}", "red")
            try:
                await _resume_sampling()
            except:
                pass
            return None

    # 所有尝试都失败
    log("❌ 堆快照多次尝试均失败", "red")
    try:
        await _resume_sampling()
    except:
        pass
    return None


async def take_sampling():
    """停止录制并保存 allocation profile（类似 DevTools 点击"停止录制"）"""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    fp = SNAP_DIR / f"sampling-{ts}.heapprofile"

    log("📊 停止录制分配采样...", "cyan")

    resp = await send_cdp("HeapProfiler.stopSampling")

    if not resp or resp.get("error"):
        log(f"⚠️ 采样失败: {resp}，尝试重新启动", "yellow")
        await send_cdp("HeapProfiler.startSampling", {
            "samplingInterval": SAMPLING_INTERVAL_US
        })
        return None

    profile = resp.get("result", {}).get("profile")
    if not profile:
        log("⚠️ 未收到采样数据", "yellow")
        await send_cdp("HeapProfiler.startSampling", {
            "samplingInterval": SAMPLING_INTERVAL_US
        })
        return None

    with open(fp, "w", encoding="utf-8") as f:
        json.dump(profile, f)

    state.sampling_count += 1
    log(f"✅ 采样已保存: {fp.name}", "green")

    # 清理旧采样文件（保留最多的数量）
    _clean_old_files("*.heapprofile", MAX_SAMPLING_FILES)

    # 重新开始采样
    await send_cdp("HeapProfiler.startSampling", {
        "samplingInterval": SAMPLING_INTERVAL_US
    })
    log("▶️ 继续录制下一段分配采样...", "cyan")

    return fp


def _clean_old_files(pattern, keep):
    """删除超过数量的旧文件"""
    files = sorted(SNAP_DIR.glob(pattern), key=os.path.getmtime)
    while len(files) > keep:
        files[0].unlink(missing_ok=True)
        files = files[1:]


# ==================== 仪表盘 ====================
import unicodedata
import re

# 正确匹配 ANSI 转义序列（\x1b 是 ESC 字符）
ANSI_RE = re.compile('\x1b' + r'\[[0-9;]*m')

def _display_len(s: str) -> int:
    """计算字符串在终端的显示宽度（中文/全角算2，ASCII算1），忽略ANSI转义序列"""
    clean = ANSI_RE.sub('', s)
    w = 0
    for ch in clean:
        ea = unicodedata.east_asian_width(ch)
        # 'W'(宽)和'F'(全角)算2列；'A'(模糊)在终端通常按单宽渲染
        w += 2 if ea in ('W', 'F') else 1
    return w

def _pad_to(s: str, width: int) -> str:
    """将字符串填充到指定显示宽度（中文算2列）"""
    return s + ' ' * max(0, width - _display_len(s))

# 方框内宽 & 横线字符
BOX_W = 78
BOX_X = "═"

def draw_bar(val, mx, width=20):
    """绘制彩色进度条"""
    r = min(val / max(mx, 1), 1.0)
    f = int(r * width)
    bar = "█" * f + "░" * (width - f)
    if r > 0.8:
        return f"\033[91m{bar}\033[0m"
    elif r > 0.5:
        return f"\033[93m{bar}\033[0m"
    return f"\033[92m{bar}\033[0m"

SPARK_CHARS = "▁▂▃▄▅▆▇█"

def sparkline(values, width=50, fixed_max=None):
    """将数值列表渲染为 sparkline 字符曲线，支持固定最大值统一比例尺"""
    if not values:
        return ""
    mx = fixed_max if fixed_max is not None else max(values)
    if mx <= 0:
        return SPARK_CHARS[0] * min(len(values), width)
    steps = len(SPARK_CHARS) - 1
    result = []
    for v in values[-width:]:
        idx = int((v / mx) * steps)
        result.append(SPARK_CHARS[min(idx, steps)])
    return "".join(result)

def color_val(val, warn, crit):
    if val >= crit:
        return f"\033[91m{val:>10.2f}\033[0m"
    elif val >= warn:
        return f"\033[93m{val:>10.2f}\033[0m"
    return f"\033[92m{val:>10.2f}\033[0m"

def render():
    """渲染仪表盘（左侧边框，去掉右侧边框避免对齐问题）"""
    os.system("cls")
    limit_gb = state.limit_mb / 1024
    ratio = state.used_mb / max(state.limit_mb, 1)

    # 状态标签
    if state.used_mb > state.crit_mem_mb:
        status_text = "● 内存告警"
        status_color = "\033[91m"
    elif state.high_freq:
        status_text = "● 高频采样"
        status_color = "\033[93m"
    else:
        status_text = "● 正常"
        status_color = "\033[92m"

    freq_text = f"\033[93m高频({METRIC_INTERVAL_FAST}秒)\033[0m" if state.high_freq else f"\033[92m正常({METRIC_INTERVAL}秒)\033[0m"
    url_short = (TARGET_URL[:55] + "..") if TARGET_URL and len(TARGET_URL) > 55 else (TARGET_URL or "手动导航")

    lines = []
    C = "\033[96m"   # 青色边框
    R = "\033[0m"

    def add_sep():
        """添加分隔线"""
        lines.append(f"{C}╠{'═' * BOX_W}╣{R}")

    def add_line(text=""):
        """添加带左边框的内容行"""
        if text:
            lines.append(f"{C}║  {text}{R}")
        else:
            lines.append(f"{C}║{R}")

    # 1. 顶边
    lines.append(f"{C}╔{'═' * BOX_W}╗{R}")

    # 2. 标题（无右边框，与其他内容行一致）
    add_line(f"Chrome 内存监控工具 (CDP)")

    # 3. 分隔线
    add_sep()

    # 4. 目标 + 运行信息 + PID（无右边框）
    add_line(f"目标: \033[93m{url_short}\033[0m")
    pid_info = f"PID: {state.process_pid}" if state.process_pid else "PID: 未知"
    add_line(f"运行: {state.elapsed:>8} 秒  |  {pid_info}  |  状态: {status_color}{status_text}\033[0m")

    # 5. 分隔线
    add_sep()

    # 6. 内存信息
    add_line(f"\033[96m📊 内存信息\033[0m")
    cv = color_val(state.used_mb, state.high_mem_mb, state.crit_mem_mb)
    add_line(f"   JS 堆:  {cv} MB  |  进程: {state.process_memory_mb:>8.1f} MB\033[0m")
    add_line(f"   JS堆上限:  {limit_gb:>8.2f} GB")
    bar = draw_bar(state.used_mb, max(state.crit_mem_mb, 1))
    add_line(f"   JS堆使用率: {ratio*100:>6.2f}%  {bar}")
    add_line(f"   阈值: 高频 > {HIGH_PERCENT*100:.0f}% · 紧急 > {CRIT_PERCENT*100:.0f}% · 限 {state.limit_mb:.0f}MB")

    # 7. 分隔线
    add_sep()

    # 8. DOM 信息（左对齐）
    add_line(f"\033[96m🌐 DOM 信息\033[0m")
    for label, val in [("DOM节点:", state.dom_nodes),
                       ("事件监听器:", state.listeners),
                       ("文档数:", state.documents),
                       ("帧数:", state.frames)]:
        add_line(f"   {label} {val:>10,}")

    # 9. 分隔线
    add_sep()

    # 10. 渲染指标（左对齐）
    add_line(f"\033[96m⚡ 渲染指标\033[0m")
    for label, val in [("布局重排:", state.layout_count),
                       ("样式重计算:", state.style_count)]:
        add_line(f"   {label} {val:>10,}")

    # 11. 分隔线
    add_sep()

    # 计算当前模式下的采集间隔
    if state.used_mb > state.crit_mem_mb:
        cur_sampling_int = SAMPLING_INTERVAL_CRIT
        cur_snap_min = SNAPSHOT_INTERVAL_CRIT
    elif state.high_freq:
        cur_sampling_int = SAMPLING_INTERVAL_HIGH
        cur_snap_min = SNAPSHOT_INTERVAL_HIGH
    else:
        cur_sampling_int = SAMPLING_INTERVAL_NORMAL
        cur_snap_min = SNAPSHOT_INTERVAL_NORMAL

    # 12. 采集状态（分配采样/堆快照各自统计）
    add_line(f"\033[96m📊 采集状态\033[0m")
    if SAMPLING_ENABLED:
        add_line(f"   \033[96m分配采样\033[0m {status_color}{status_text}\033[0m  ·  已存 {state.sampling_count} 次  ·  每 {cur_sampling_int}s")
    else:
        add_line(f"   \033[96m分配采样\033[0m \033[90m已关闭\033[0m")
    if SNAPSHOT_ENABLED:
        add_line(f"   \033[96m堆快照\033[0m {status_color}{status_text}\033[0m  ·  已存 {state.snapshot_count} 次  ·  每 {cur_snap_min}分钟")
    else:
        add_line(f"   \033[96m堆快照\033[0m \033[90m已关闭\033[0m")

    # 13. 分隔线
    add_sep()

    # 14. 内存趋势（统一比例尺，标注 min/avg/max）
    SPARK_W = 62
    add_line(f"\033[96m📈 内存趋势\033[0m")
    if state.history and state.process_history:
        js_vals = [v for v, _ in state.history]
        proc_vals = state.process_history
        # 统一比例尺：两条曲线用同一个最大值，确保图形高度可对比
        all_vals = js_vals + proc_vals
        global_max = max(all_vals)

        def desc(vals, label):
            spark = sparkline(vals, SPARK_W, fixed_max=global_max)
            mn = min(vals)
            mx = max(vals)
            ag = sum(vals) / len(vals)
            return f"{label} {spark}  \033[90mmin:{mn:.0f}  avg:{ag:.0f}  max:{mx:.0f}MB\033[0m"

        add_line(desc(js_vals, "\033[96mJS堆\033[0m"))
        add_line("")
        add_line(desc(proc_vals, "\033[96m进程\033[0m"))
    elif state.history:
        js_vals = [v for v, _ in state.history]
        js_spark = sparkline(js_vals, SPARK_W)
        mn = min(js_vals)
        mx = max(js_vals)
        ag = sum(js_vals) / len(js_vals)
        add_line(f"\033[96mJS堆\033[0m {js_spark}  \033[90mmin:{mn:.0f}  avg:{ag:.0f}  max:{mx:.0f}MB\033[0m")
        add_line(f"\033[90m进程: 等待数据...\033[0m")
    else:
        add_line(f"\033[90mJS堆: 等待数据...\033[0m")
        add_line(f"\033[90m进程: 等待数据...\033[0m")

    # 15. 底边
    lines.append(f"{C}╚{'═' * BOX_W}╝{R}")

    # 16. 提示行（方框外）
    lines.append(f"\033[90m  提示: Ctrl+C 停止  日志: {CSV_FILE.name}\033[0m")

    print("\n".join(lines))

    # ── 仪表盘下方：实时日志缓冲区 ──
    if LOG_LINES:
        print(f"\033[90m{'─' * 55}\033[0m")
        for log_line in LOG_LINES:
            print(f"  \033[37m{log_line}\033[0m")


# ==================== 崩溃检测 ====================
async def crash_watch():
    """独立协程：检测 Chrome 进程是否存活"""
    while state.running:
        await asyncio.sleep(5)
        try:
            r = subprocess.run(
                ["tasklist", "/fi", "imagename eq chrome.exe", "/fo", "csv", "/nh"],
                capture_output=True, text=True, timeout=5
            )
            if "chrome.exe" not in r.stdout.lower():
                log("⚠️ Chrome 进程已退出！", "red")
                if state.ws and state.ws.close_code is None:
                    if SAMPLING_ENABLED:
                        await take_sampling()
                    if SNAPSHOT_ENABLED:
                        await take_snapshot()
                state.running = False
                break
        except:
            pass


# ==================== 主监控循环 ====================
async def monitor_loop():
    """定时采集 + 仪表盘渲染 + 分配采样 + 堆快照（独立定时器）"""
    start = time.time()
    sampling_timer = 0.0   # 分配采样保存计时
    snap_timer = 0.0       # 堆快照计时

    # 启用所需域
    await send_cdp("Performance.enable")
    await send_cdp("Memory.enable")

    # 启动分配采样（需开启时）
    if SAMPLING_ENABLED:
        await send_cdp("HeapProfiler.startSampling", {
            "samplingInterval": SAMPLING_INTERVAL_US
        })
        log(f"✅ 分配采样已启动（正常{SAMPLING_INTERVAL_NORMAL}s / 高频{SAMPLING_INTERVAL_HIGH}s / 紧急{SAMPLING_INTERVAL_CRIT}s）", "green")
    if SNAPSHOT_ENABLED:
        log(f"✅ 堆快照已启动（正常{SNAPSHOT_INTERVAL_NORMAL}分 / 高频{SNAPSHOT_INTERVAL_HIGH}分 / 紧急{SNAPSHOT_INTERVAL_CRIT}分）", "green")

    fail = 0

    # 找到 Chrome 进程 PID（仅一次）
    if not state.process_pid:
        state.process_pid = find_chrome_pid()
        if state.process_pid:
            log(f"Chrome 进程 PID: {state.process_pid}", "cyan")
        else:
            log("⚠️ 未找到 Chrome 进程 PID", "yellow")

    while state.running:
        try:
            info = await collect_metrics()
            if not info:
                fail += 1
                if fail >= 3:
                    log("⚠️ 连续 3 次采集失败，Chrome 可能已崩溃", "red")
                    if SAMPLING_ENABLED:
                        await take_sampling()
                    if SNAPSHOT_ENABLED:
                        await take_snapshot()
                    break
                await asyncio.sleep(3)
                continue

            fail = 0
            state.elapsed = int(time.time() - start)

            # 更新状态（0 表示获取失败，保留上一次有效值）
            new_used = (info.get("u") or info.get("usedJSHeapSize") or 0) / (1024*1024)
            new_total = (info.get("t") or info.get("totalJSHeapSize") or 0) / (1024*1024)
            new_limit = (info.get("l") or info.get("jsHeapSizeLimit") or 0) / (1024*1024)
            if new_used > 0:
                state.used_mb = new_used
            if new_total > 0:
                state.total_mb = new_total
            if new_limit > 0:
                state.limit_mb = new_limit

            # 首次采集到 jsHeapSizeLimit 后动态计算阈值
            if not state._thresh_set and state.limit_mb > 0:
                state.high_mem_mb = int(state.limit_mb * HIGH_PERCENT)
                state.crit_mem_mb = int(state.limit_mb * CRIT_PERCENT)
                state._thresh_set = True
                log(f"📐 JS 堆上限: {state.limit_mb:.0f} MB  |  高频阈值: {state.high_mem_mb} MB ({HIGH_PERCENT*100:.0f}%)  |  紧急阈值: {state.crit_mem_mb} MB ({CRIT_PERCENT*100:.0f}%)", "cyan")

            new_nodes = info.get("d", 0) or info.get("nodes", 0)
            if new_nodes > 0:
                state.dom_nodes = new_nodes
            new_listeners = info.get("jsEventListeners", 0)
            if new_listeners > 0:
                state.listeners = new_listeners
            new_docs = info.get("documents", 0)
            if new_docs > 0:
                state.documents = new_docs
            new_frames = info.get("frames", 0) or info.get("Frames", 0)
            if new_frames > 0:
                state.frames = new_frames
            new_layout = info.get("layoutCount", 0) or info.get("LayoutCount", 0)
            if new_layout > 0:
                state.layout_count = new_layout
            new_style = info.get("recalcStyleCount", 0) or info.get("RecalcStyleCount", 0)
            if new_style > 0:
                state.style_count = new_style

            # 采集进程级内存（tasklist 数据，每 3 次采集一次以节省开销）
            if state.elapsed % 3 == 0:
                state.process_memory_mb = get_all_chrome_memory()

            state.history.append((state.used_mb, time.time()))
            if len(state.history) > 30:
                state.history = state.history[-30:]
            state.process_history.append(state.process_memory_mb)
            if len(state.process_history) > 30:
                state.process_history = state.process_history[-30:]

            append_csv(state.used_mb, state.total_mb, state.limit_mb, state.process_memory_mb,
                       state.dom_nodes, state.listeners, state.documents, state.frames)

            # 阈值判断
            is_crit = state.used_mb >= state.crit_mem_mb
            if is_crit:
                state.high_freq = True
                log(f"🔴 内存超限量 {state.crit_mem_mb:.0f}MB（{CRIT_PERCENT*100:.0f}%）", "red")
                # 紧急模式：只保存分配采样（堆快照在内存高压下几乎无法完成，避免阻塞）
                if SAMPLING_ENABLED:
                    await take_sampling()
                # 紧急模式跳过堆快照（V8 正在全力 GC，takeHeapSnapshot 会超时或卡死）
                log("🔴 紧急模式跳过堆快照（内存高压下快照不可行），已保存分配采样", "red")
            elif state.used_mb >= state.high_mem_mb:
                if not state.high_freq:
                    log(f"🟡 内存超限量 {state.high_mem_mb:.0f}MB（{HIGH_PERCENT*100:.0f}%），切换到高频采样", "yellow")
                state.high_freq = True
            elif state.used_mb < state.limit_mb * 0.64:  # 降回 80%*80% = 64% 以下才恢复正常
                if state.high_freq:
                    log(f"🟢 内存恢复正常（{HIGH_PERCENT*100:.0f}% 以下）", "green")
                state.high_freq = False

            # 根据当前模式决定各采集器间隔（采样秒，快照分→秒）
            if is_crit:
                sampling_sec = SAMPLING_INTERVAL_CRIT
                snapshot_sec = SNAPSHOT_INTERVAL_CRIT * 60
            elif state.high_freq:
                sampling_sec = SAMPLING_INTERVAL_HIGH
                snapshot_sec = SNAPSHOT_INTERVAL_HIGH * 60
            else:
                sampling_sec = SAMPLING_INTERVAL_NORMAL
                snapshot_sec = SNAPSHOT_INTERVAL_NORMAL * 60

            # 数据采集间隔（sleep 用）
            interval = METRIC_INTERVAL_FAST if state.high_freq else METRIC_INTERVAL
            sampling_timer += interval
            snap_timer += interval

            # 分配采样（优先执行，避免冲突时丢数据）
            if SAMPLING_ENABLED and sampling_timer >= sampling_sec:
                await take_sampling()
                sampling_timer = 0

            # 堆快照（后执行，take_snapshot 内部会自动暂停/恢复采样）
            if SNAPSHOT_ENABLED and snap_timer >= snapshot_sec:
                await take_snapshot()
                snap_timer = 0

            # 渲染
            render()

            await asyncio.sleep(interval)

        except websockets.ConnectionClosed:
            log("⚠️ WebSocket 断开，Chrome 可能已崩溃", "red")
            state.running = False
            break
        except Exception as e:
            log(f"⚠️ 异常: {e}", "yellow")
            fail += 1
            if fail >= 3:
                break
            await asyncio.sleep(3)


# ==================== 入口 ====================
async def main():
    print("\033[96m" + "=" * 60)
    print("  Chrome 内存监控工具 (Python + CDP) ")
    print("=" * 60 + "\033[0m\n")

    ensure_dirs()
    init_csv()

    # 启动 Chrome
    start_chrome()

    # 获取 WebSocket URL
    log("连接 Chrome DevTools Protocol...")
    pages = await get_pages()
    if not pages:
        log("❌ 无法连接到 Chrome", "red")
        return

    # 找到有页面的 tab
    target = None
    for p in pages:
        if p.get("type") == "page":
            target = p
            break
    if not target:
        log("❌ 未找到可监控的页面", "red")
        return

    ws_url = target["webSocketDebuggerUrl"]
    log(f"页面: {target.get('title', 'N/A')[:50]}")
    log(f"WebSocket: {ws_url}")

    try:
        async with websockets.connect(ws_url, max_size=2**31) as ws:
            state.ws = ws
            log("\n✅ CDP 已连接，启动监控...\n", "green")

            # 先启动消息监听器（send_cdp 依赖它接收响应）
            listener = asyncio.create_task(cdp_listener())
            await asyncio.sleep(0.3)

            # 如果配置了 TARGET_URL，新开 Tab 并跳转（不影响其他已有标签页）
            if TARGET_URL:
                log(f"新开标签页并导航到: {TARGET_URL}", "cyan")
                new_target = await send_cdp("Target.createTarget", {
                    "url": TARGET_URL
                })
                if new_target and "result" in new_target:
                    new_target_id = new_target["result"].get("targetId")
                    if new_target_id:
                        await asyncio.sleep(1)
                        # 关闭旧的空白页，只保留新开的页面
                        await send_cdp("Target.closeTarget", {"targetId": target["id"]})
                        # 切换监控目标到新页面
                        new_pages = await get_pages()
                        for p in new_pages:
                            if p.get("targetId") == new_target_id:
                                new_ws_url = p.get("webSocketDebuggerUrl")
                                if new_ws_url:
                                    # 重新连接到新页面的 WebSocket
                                    await ws.close()
                                    state.ws = None
                                    listener.cancel()
                                    async with websockets.connect(new_ws_url, max_size=2**31) as new_ws:
                                        state.ws = new_ws
                                        listener = asyncio.create_task(cdp_listener())
                                        await asyncio.sleep(0.3)
                                        watcher = asyncio.create_task(crash_watch())
                                        await monitor_loop()
                                        watcher.cancel()
                                        listener.cancel()
                                    # 正常退出流程
                                    log("监控已断开，Chrome 保持运行", "green")
                                    return
                        log("⚠️ 新标签页导航完成，继续监控当前页面", "yellow")
                else:
                    log("⚠️ 新开标签页失败，尝试在当前页面导航", "yellow")
                    escaped_url = TARGET_URL.replace("\\", "\\\\").replace("'", "\\'")
                    await send_cdp("Runtime.evaluate", {
                        "expression": f"window.location.href = '{escaped_url}'",
                        "silent": True
                    })
                    await asyncio.sleep(2)
            else:
                log("TARGET_URL 未配置，直接监控当前页面（请在浏览器手动输入地址）", "yellow")

            # 启动崩溃检测
            watcher = asyncio.create_task(crash_watch())
            # 启动主循环
            await monitor_loop()

            listener.cancel()
            watcher.cancel()

    except Exception as e:
        log(f"❌ 连接失败: {e}", "red")

    # 断开连接但不关闭 Chrome，保留用户数据目录以便复用
    log("监控已断开，Chrome 保持运行", "green")

    print("\n\033[96m" + "=" * 60)
    print(f"  监控已结束（Chrome 未关闭）")
    print(f"  日志: {CSV_FILE}")
    print(f"  采样+快照: {SNAP_DIR} ({state.snapshot_count} 个)")
    print("=" * 60 + "\033[0m")


def sigint(_, __):
    state.running = False

if __name__ == "__main__":
    signal.signal(signal.SIGINT, sigint)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        state.running = False
        print("\n监控已停止（Chrome 保持运行）")
