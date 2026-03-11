"""
F1 Terminal — Live Timing Engine

Connects to the F1 live timing SignalR stream via FastF1's SignalRClient,
parses incremental timing updates, and maintains a live timing board state.
"""

import ast
import json
import logging
import queue
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional

from fastf1.livetiming.client import SignalRClient

log = logging.getLogger(__name__)

# ─── Compound display ────────────────────────────────────────────────────────

COMPOUND_ICON = {
    "SOFT": ("S", "red"),
    "MEDIUM": ("M", "yellow"),
    "HARD": ("H", "white"),
    "INTERMEDIATE": ("I", "green"),
    "WET": ("W", "blue"),
    "UNKNOWN": ("?", "dim"),
}


# ─── Subclassed client that also feeds a queue ───────────────────────────────

class _QueuedClient(SignalRClient):
    """SignalRClient that, in addition to writing to file, puts parsed
    (topic, data) tuples into a thread-safe queue for the UI to consume."""

    def __init__(self, data_queue: queue.Queue, filename: str, **kwargs):
        super().__init__(filename=filename, **kwargs)
        self._q = data_queue

    def _on_message(self, msg):
        # Let the parent write to file
        super()._on_message(msg)
        # Also parse and queue
        try:
            from signalrcore.hub.base_hub_connection import CompletionMessage
            if isinstance(msg, CompletionMessage):
                for topic, raw in msg.result.items():
                    try:
                        data = json.loads(raw) if isinstance(raw, str) else raw
                        self._q.put_nowait((topic, data))
                    except Exception:
                        pass
            elif isinstance(msg, list):
                # list format: [topic, data, timestamp] or nested
                self._parse_list_msg(msg)
        except Exception:
            pass

    def _parse_list_msg(self, msg: list):
        try:
            if len(msg) >= 2:
                topic = msg[0]
                data = msg[1]
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except Exception:
                        return
                if isinstance(topic, str) and isinstance(data, dict):
                    self._q.put_nowait((topic, data))
        except Exception:
            pass


# ─── Timing Board State ───────────────────────────────────────────────────────

class LiveTimingState:
    """Thread-safe state for the live timing board."""

    def __init__(self):
        self._lock = threading.Lock()
        self.drivers: dict = {}        # num -> {Tla, FullName, TeamName, TeamColour}
        self.timing: dict = {}         # num -> incremental timing snapshot
        self.tyres: dict = {}          # num -> {Compound, New, TotalLaps}
        self.session_status: str = "Unknown"
        self.track_status: str = ""
        self.current_lap: int = 0
        self.total_laps: int = 0
        self.session_name: str = ""
        self.race_control: list = []   # recent RC messages

    # ── Handlers ──────────────────────────────────────────────────────────────

    def apply(self, topic: str, data: dict):
        with self._lock:
            if topic == "DriverList":
                self._merge_drivers(data)
            elif topic == "TimingData":
                self._merge_timing(data)
            elif topic == "TimingAppData":
                self._merge_tyres(data)
            elif topic == "SessionStatus":
                s = data.get("Status", "")
                if s:
                    self.session_status = s
            elif topic == "TrackStatus":
                self.track_status = _TRACK_STATUS.get(data.get("Status", ""), "")
            elif topic == "LapCount":
                try:
                    self.current_lap = int(data.get("CurrentLap", self.current_lap))
                    self.total_laps = int(data.get("TotalLaps", self.total_laps))
                except (ValueError, TypeError):
                    pass
            elif topic == "SessionInfo":
                meeting = data.get("Meeting", {})
                self.session_name = (
                    meeting.get("Name", "")
                    + " — "
                    + data.get("Name", "")
                )
            elif topic == "RaceControlMessages":
                msgs = data.get("Messages", {})
                if isinstance(msgs, dict):
                    for item in msgs.values():
                        if isinstance(item, dict):
                            self.race_control.append(item.get("Message", ""))
                elif isinstance(msgs, list):
                    for item in msgs:
                        if isinstance(item, dict):
                            self.race_control.append(item.get("Message", ""))
                self.race_control = self.race_control[-20:]  # keep last 20

    def _merge_drivers(self, data: dict):
        for num, info in data.items():
            if isinstance(info, dict) and num.isdigit():
                if num not in self.drivers:
                    self.drivers[num] = {}
                self.drivers[num].update(info)

    def _merge_timing(self, data: dict):
        lines = data.get("Lines", data)
        if not isinstance(lines, dict):
            return
        for num, info in lines.items():
            if isinstance(info, dict):
                if num not in self.timing:
                    self.timing[num] = {}
                _deep_merge(self.timing[num], info)

    def _merge_tyres(self, data: dict):
        lines = data.get("Lines", data)
        if not isinstance(lines, dict):
            return
        for num, info in lines.items():
            if not isinstance(info, dict):
                continue
            stints = info.get("Stints", {})
            if not stints:
                continue
            # Stints is a dict keyed by stint index string
            if isinstance(stints, dict):
                if stints:
                    last_key = max(stints.keys(), key=lambda k: int(k) if k.isdigit() else 0)
                    stint = stints[last_key]
                    if isinstance(stint, dict) and stint:
                        if num not in self.tyres:
                            self.tyres[num] = {}
                        self.tyres[num].update(stint)

    # ── Board snapshot ────────────────────────────────────────────────────────

    def get_board(self) -> list[dict]:
        with self._lock:
            rows = []
            for num, t in self.timing.items():
                drv = self.drivers.get(num, {})
                tyre = self.tyres.get(num, {})
                try:
                    pos_int = int(t.get("Position", 99))
                except (ValueError, TypeError):
                    pos_int = 99

                # Sector values
                sectors = t.get("Sectors", {})
                if isinstance(sectors, dict):
                    s1 = sectors.get("0", {})
                    s2 = sectors.get("1", {})
                    s3 = sectors.get("2", {})
                elif isinstance(sectors, list):
                    s1 = sectors[0] if len(sectors) > 0 else {}
                    s2 = sectors[1] if len(sectors) > 1 else {}
                    s3 = sectors[2] if len(sectors) > 2 else {}
                else:
                    s1 = s2 = s3 = {}

                def sector_val(s):
                    if isinstance(s, dict):
                        return s.get("Value", "")
                    return str(s) if s else ""

                def sector_fastest(s):
                    if isinstance(s, dict):
                        return s.get("OverallFastest", False) or s.get("PersonalFastest", False)
                    return False

                last_lap = t.get("LastLapTime", {})
                if isinstance(last_lap, dict):
                    last_lap_val = last_lap.get("Value", "")
                    last_lap_fastest = last_lap.get("OverallFastest", False)
                    last_lap_personal = last_lap.get("PersonalFastest", False)
                else:
                    last_lap_val = str(last_lap) if last_lap else ""
                    last_lap_fastest = False
                    last_lap_personal = False

                interval = t.get("IntervalToPositionAhead", {})
                if isinstance(interval, dict):
                    interval_val = interval.get("Value", "")
                else:
                    interval_val = str(interval) if interval else ""

                compound = tyre.get("Compound", "")
                compound_icon, compound_color = COMPOUND_ICON.get(
                    compound.upper() if compound else "UNKNOWN",
                    ("?", "dim")
                )

                rows.append({
                    "pos": pos_int,
                    "num": num,
                    "code": drv.get("Tla", num),
                    "name": drv.get("FullName", ""),
                    "team": drv.get("TeamName", ""),
                    "gap": t.get("GapToLeader", ""),
                    "interval": interval_val,
                    "last_lap": last_lap_val,
                    "last_lap_fastest": last_lap_fastest,
                    "last_lap_personal": last_lap_personal,
                    "s1": sector_val(s1), "s1_fast": sector_fastest(s1),
                    "s2": sector_val(s2), "s2_fast": sector_fastest(s2),
                    "s3": sector_val(s3), "s3_fast": sector_fastest(s3),
                    "compound": compound_icon,
                    "compound_color": compound_color,
                    "tyre_laps": str(tyre.get("TotalLaps", "")),
                    "in_pit": bool(t.get("InPit", False)),
                    "pit_out": bool(t.get("PitOut", False)),
                    "retired": bool(t.get("Retired", False)),
                })

            rows.sort(key=lambda r: r["pos"])
            return rows

    def get_rc_messages(self) -> list[str]:
        with self._lock:
            return list(reversed(self.race_control[-10:]))


# ─── Collector (runs client in background thread) ────────────────────────────

class LiveTimingCollector:
    """Runs the SignalR client in a background thread and feeds LiveTimingState."""

    def __init__(self, state: LiveTimingState, no_auth: bool = True):
        self.state = state
        self.no_auth = no_auth
        self._q: queue.Queue = queue.Queue(maxsize=2000)
        self._client_thread: Optional[threading.Thread] = None
        self._processor_thread: Optional[threading.Thread] = None
        self._tmpfile: Optional[str] = None
        self.running = False
        self.connected = False
        self.error: Optional[str] = None

    def start(self):
        if self.running:
            return
        self.running = True
        self.connected = False
        self.error = None

        # Create temp log file
        tf = tempfile.NamedTemporaryFile(
            mode="w", suffix="_f1live.txt", delete=False, encoding="utf-8"
        )
        self._tmpfile = tf.name
        tf.close()

        # Start client thread
        self._client_thread = threading.Thread(target=self._run_client, daemon=True, name="f1-live-client")
        self._client_thread.start()

        # Start message processor thread
        self._processor_thread = threading.Thread(target=self._process_queue, daemon=True, name="f1-live-proc")
        self._processor_thread.start()

    def stop(self):
        self.running = False
        self.connected = False
        # Clean up temp file
        if self._tmpfile:
            try:
                Path(self._tmpfile).unlink(missing_ok=True)
            except Exception:
                pass
            self._tmpfile = None

    def _run_client(self):
        try:
            client = _QueuedClient(
                data_queue=self._q,
                filename=self._tmpfile,
                filemode="w",
                timeout=60,
                no_auth=self.no_auth,
            )
            self.connected = True
            client.start()
        except KeyboardInterrupt:
            pass
        except Exception as e:
            self.error = str(e)
        finally:
            self.connected = False
            self.running = False

    def _process_queue(self):
        while self.running or not self._q.empty():
            try:
                topic, data = self._q.get(timeout=0.5)
                self.state.apply(topic, data)
            except queue.Empty:
                continue
            except Exception:
                continue


# ─── Helpers ─────────────────────────────────────────────────────────────────

_TRACK_STATUS = {
    "1": "Track Clear",
    "2": "Yellow Flag",
    "3": "SC Deployed",
    "4": "SC Deployed",
    "5": "Red Flag",
    "6": "VSC Deployed",
    "7": "VSC Ending",
}


def _deep_merge(base: dict, update: dict):
    """Recursively merge update into base (in-place)."""
    for key, val in update.items():
        if key in base and isinstance(base[key], dict) and isinstance(val, dict):
            _deep_merge(base[key], val)
        else:
            base[key] = val
