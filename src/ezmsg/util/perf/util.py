import os
import sys
import gc
import time
import statistics as stats
import contextlib
import subprocess
from dataclasses import dataclass
from typing import Iterable

try:
    import psutil  # optional but helpful
except Exception:
    psutil = None

_IS_WIN = os.name == "nt"
_IS_MAC = sys.platform == "darwin"
_IS_LINUX = sys.platform.startswith("linux")

# ---------- Utilities ----------


def _set_env_threads(single_thread: bool = True):
    """
    Normalize math/threading libs so they don't spawn surprise worker threads.
    """
    if single_thread:
        os.environ.setdefault("OMP_NUM_THREADS", "1")
        os.environ.setdefault("MKL_NUM_THREADS", "1")
        os.environ.setdefault("VECLIB_MAXIMUM_THREADS", "1")
        os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
        os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
    # Keep PYTHONHASHSEED stable for deterministic dict/set iteration costs
    os.environ.setdefault("PYTHONHASHSEED", "0")


# ---------- Priority & Affinity ----------


@contextlib.contextmanager
def _process_priority():
    """
    Elevate process priority in a cross-platform best-effort way.
    """
    if psutil is None:
        yield
        return

    p = psutil.Process()
    orig_nice = None
    if _IS_WIN:
        try:
            import ctypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            ABOVE_NORMAL_PRIORITY_CLASS = 0x00008000
            HIGH_PRIORITY_CLASS = 0x00000080
            # Try High, fall back to Above Normal
            if not kernel32.SetPriorityClass(
                kernel32.GetCurrentProcess(), HIGH_PRIORITY_CLASS
            ):
                kernel32.SetPriorityClass(
                    kernel32.GetCurrentProcess(), ABOVE_NORMAL_PRIORITY_CLASS
                )
        except Exception:
            pass
    else:
        try:
            orig_nice = p.nice()
            # Negative nice may need privileges; try smaller magnitude first
            for nice_val in (-10, -5, 0):
                try:
                    p.nice(nice_val)
                    break
                except Exception:
                    continue
        except Exception:
            pass
    try:
        yield
    finally:
        # restore nice if we changed it
        if psutil is not None and not _IS_WIN and orig_nice is not None:
            try:
                p.nice(orig_nice)
            except Exception:
                pass


@contextlib.contextmanager
def _cpu_affinity(prefer_isolation: bool = True):
    """
    Set CPU affinity to a small, stable set of CPUs (where supported).
    macOS does not support affinity via psutil; we no-op there.
    """
    if psutil is None or _IS_MAC:
        yield
        return

    p = psutil.Process()
    original = None
    try:
        if hasattr(p, "cpu_affinity"):
            original = p.cpu_affinity()
            cpus = original
            if prefer_isolation and len(cpus) > 2:
                # Pick two middle CPUs to avoid 0 which often handles interrupts
                mid = len(cpus) // 2
                cpus = [cpus[mid - 1], cpus[mid]]
            p.cpu_affinity(cpus)
        yield
    finally:
        try:
            if original is not None and hasattr(p, "cpu_affinity"):
                p.cpu_affinity(original)
        except Exception:
            pass


# ---------- Platform-specific helpers ----------


@contextlib.contextmanager
def _mac_caffeinate():
    """
    Keep macOS awake during the run via a background caffeinate process.
    """
    if not _IS_MAC:
        yield
        return
    proc = None
    try:
        proc = subprocess.Popen(["caffeinate", "-dimsu"])
    except Exception:
        proc = None
    try:
        yield
    finally:
        if proc is not None:
            try:
                proc.terminate()
            except Exception:
                pass


@contextlib.contextmanager
def _win_timer_resolution(ms: int = 1):
    """
    On Windows, request a finer system timer to stabilize sleeps and scheduling slices.
    """
    if not _IS_WIN:
        yield
        return
    import ctypes

    winmm = ctypes.WinDLL("winmm")
    timeBeginPeriod = winmm.timeBeginPeriod
    timeEndPeriod = winmm.timeEndPeriod
    try:
        timeBeginPeriod(ms)
    except Exception:
        pass
    try:
        yield
    finally:
        try:
            timeEndPeriod(ms)
        except Exception:
            pass


# ---------- Warm-up & GC ----------


def warmup(seconds: float = 60.0, fn=None, *args, **kwargs):
    """
    Optional warm-up to reach steady clocks/caches.
    If fn is provided, call it in a loop for the given time.
    """
    if seconds <= 0:
        return
    end = time.perf_counter()
    target = end + seconds
    if fn is None:
        # Busy wait / sleep mix to heat up without heavy CPU
        while time.perf_counter() < target:
            x = 0
            for _ in range(10000):
                x += 1
            time.sleep(0)
    else:
        while time.perf_counter() < target:
            fn(*args, **kwargs)


@contextlib.contextmanager
def gc_pause():
    """
    Disable GC inside timing windows; re-enable and collect after.
    """
    was_enabled = gc.isenabled()
    try:
        gc.disable()
        yield
    finally:
        if was_enabled:
            gc.enable()
        gc.collect()


# ---------- Robust statistics ----------


def median_of_means(samples: Iterable[float], k: int = 5) -> float:
    """
    Robust estimate: split samples into k buckets (round-robin), average each, take median of bucket means.
    """
    samples = list(samples)
    if not samples:
        return float("nan")
    k = max(1, min(k, len(samples)))
    buckets = [[] for _ in range(k)]
    for i, v in enumerate(samples):
        buckets[i % k].append(v)
    means = [sum(b) / len(b) for b in buckets if b]
    means.sort()
    return means[len(means) // 2]


def coef_var(samples: Iterable[float]) -> float:
    vals = list(samples)
    if len(vals) < 2:
        return 0.0
    m = sum(vals) / len(vals)
    if m == 0:
        return 0.0
    sd = stats.pstdev(vals)
    return sd / m


# ---------- Public context manager ----------


@dataclass
class PerfOptions:
    single_thread_math: bool = True
    prefer_isolated_cpus: bool = True
    warmup_seconds: float = 0.0
    adjust_priority: bool = True
    tweak_timer_windows: bool = True
    keep_mac_awake: bool = True


@contextlib.contextmanager
def stable_perf(opts: PerfOptions = PerfOptions()):
    """
    Wrap your perf runs with this context manager for a stabler environment.
    """
    _set_env_threads(opts.single_thread_math)

    cm_stack = contextlib.ExitStack()
    try:
        if opts.adjust_priority:
            cm_stack.enter_context(_process_priority())
        if opts.tweak_timer_windows:
            cm_stack.enter_context(_win_timer_resolution(1))
        if opts.prefer_isolated_cpus:
            cm_stack.enter_context(_cpu_affinity(True))
        if opts.keep_mac_awake:
            cm_stack.enter_context(_mac_caffeinate())

        if opts.warmup_seconds > 0:
            warmup(opts.warmup_seconds)

        with gc_pause():
            yield
    finally:
        cm_stack.close()
