#!/usr/bin/env python3
"""
F1 Live Timing Proxy
────────────────────
Connects to livetiming.formula1.com in Python (no CORS restrictions),
then relays the data to your browser via a local WebSocket on localhost:8765.

Usage:
    python proxy.py

Then open f1.html and click Connect — it will find the proxy automatically.
Press Ctrl+C to stop.
"""

import asyncio
import base64
import json
import queue
import sys
import threading
import time
import zlib
from datetime import datetime
from pathlib import Path

import fastf1

import requests
import websockets
from signalrcore.hub_connection_builder import HubConnectionBuilder

PORT = 8765

# Shared between threads
f1_q: queue.Queue = queue.Queue(maxsize=10000)
clients: set = set()
state_cache: dict = {}   # topic -> latest data, sent to new clients on connect
f1_connected = False

# Track map
_track_cache: dict = {}
_track_loading: set = set()
_current_circuit: str = ""
_current_year: int = datetime.now().year


# ─────────────────────────────────────────────────────────────────────────────
# WebSocket server (browser side)
# ─────────────────────────────────────────────────────────────────────────────

async def ws_handler(websocket):
    """Handle one browser connection."""
    clients.add(websocket)
    print(f"  Browser connected  (total: {len(clients)})")

    # Send live state snapshot
    for topic, data in list(state_cache.items()):
        try:
            await websocket.send(json.dumps({"topic": topic, "data": data}))
        except Exception:
            pass
    await websocket.send(json.dumps({"topic": "__proxy__", "data": {"connected": f1_connected}}))

    replay: ReplayEngine | None = None
    replay_task: asyncio.Task | None = None

    async def send(topic, data):
        await websocket.send(json.dumps({"topic": topic, "data": data}))

    try:
        async for raw in websocket:          # exits cleanly on disconnect
            try:
                cmd = json.loads(raw)
                action = cmd.get("cmd", "")

                if action == "replay_load":
                    if replay_task and not replay_task.done():
                        replay_task.cancel()
                    replay = ReplayEngine(send)
                    replay_task = asyncio.create_task(
                        replay.load_and_stream(
                            int(cmd["year"]), int(cmd["round"]), cmd.get("session", "R")
                        )
                    )
                elif action == "replay_play" and replay:
                    replay.playing = True
                    replay.speed = float(cmd.get("speed", 10))
                elif action == "replay_pause" and replay:
                    replay.playing = False
                elif action == "replay_seek" and replay:
                    replay.seek_pct = float(cmd.get("pct", 0))
                elif action == "replay_speed" and replay:
                    replay.speed = float(cmd.get("speed", 10))

            except Exception:
                pass
    finally:
        if replay_task and not replay_task.done():
            replay_task.cancel()
        clients.discard(websocket)
        print(f"  Browser disconnected (total: {len(clients)})")


# ─────────────────────────────────────────────────────────────────────────────
# REPLAY ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class ReplayEngine:
    """Loads a FastF1 session and streams position frames to one browser client."""

    SAMPLE_MS = 200   # sample every 200ms of race time

    def __init__(self, send_fn):
        self._send = send_fn
        self.playing = False
        self.speed = 10.0
        self.seek_pct: float | None = None

    async def load_and_stream(self, year: int, round_num: int, session_type: str):
        await self._send("__replay_status__", {"state": "loading", "msg": "Downloading session data via FastF1…"})
        try:
            loop = asyncio.get_event_loop()
            frames, track, drivers, duration = await loop.run_in_executor(
                None, self._load_blocking, year, round_num, session_type
            )
        except Exception as e:
            await self._send("__replay_status__", {"state": "error", "msg": str(e)})
            return

        await self._send("__track__", track)
        await self._send("__replay_ready__", {
            "duration": duration,
            "drivers": drivers,
            "frameCount": len(frames),
            "circuit": track.get("circuit", ""),
        })
        await self._send("__replay_status__", {"state": "ready", "msg": "Loaded — press Play"})

        # Streaming loop
        idx = 0
        while idx < len(frames):
            # Handle seek
            if self.seek_pct is not None:
                idx = int(self.seek_pct * (len(frames) - 1))
                self.seek_pct = None

            if not self.playing:
                await asyncio.sleep(0.05)
                continue

            frame = frames[idx]
            await self._send("__replay_frame__", frame)
            idx += 1

            if idx < len(frames):
                dt = (frames[idx]["t"] - frame["t"]) / self.speed
                await asyncio.sleep(max(0.01, dt))

        await self._send("__replay_status__", {"state": "finished", "msg": "Replay complete"})

    def _load_blocking(self, year: int, round_num: int, session_type: str):
        """Runs in a thread — loads FastF1 data and returns frames."""
        import numpy as np

        session = fastf1.get_session(year, round_num, session_type)
        session.load(laps=True, telemetry=True, weather=False, messages=False)

        # ── Track outline ─────────────────────────────────────────────────────
        lap = session.laps.pick_fastest()
        tel = lap.get_telemetry()
        x = tel["X"].dropna().round(1).tolist()
        y = tel["Y"].dropna().round(1).tolist()
        rotation = 0
        try:
            rotation = float(session.get_circuit_info().rotation)
        except Exception:
            pass
        track = {
            "x": x, "y": y,
            "xMin": min(x), "xMax": max(x),
            "yMin": min(y), "yMax": max(y),
            "circuit": str(session.event.get("EventName", "")),
            "rotation": rotation,
        }

        # ── Driver info ───────────────────────────────────────────────────────
        drivers = {}
        for num in session.pos_data:
            try:
                drv_laps = session.laps.pick_drivers(num)
                if drv_laps.empty:
                    continue
                row = drv_laps.iloc[0]
                drivers[str(num)] = {
                    "code": str(row.get("Abbreviation", num)),
                    "team": str(row.get("Team", "")),
                    "color": str(row.get("TeamColor", "aaaaaa")),
                }
            except Exception:
                drivers[str(num)] = {"code": str(num), "team": "", "color": "aaaaaa"}

        # ── Build unified position timeline ───────────────────────────────────
        all_pos = {}
        t_min, t_max = float("inf"), float("-inf")

        for num, pos_df in session.pos_data.items():
            if pos_df.empty or "X" not in pos_df.columns:
                continue
            t = pos_df["Time"].dt.total_seconds().values
            all_pos[str(num)] = {
                "t": t,
                "x": pos_df["X"].values,
                "y": pos_df["Y"].values,
                "status": pos_df["Status"].values if "Status" in pos_df.columns
                          else np.full(len(t), "OnTrack"),
            }
            t_min = min(t_min, t[0])
            t_max = max(t_max, t[-1])

        if not all_pos:
            raise ValueError("No position data found for this session")

        duration = float(t_max - t_min)
        sample_s = self.SAMPLE_MS / 1000.0
        t_samples = np.arange(t_min, t_max, sample_s)

        frames = []
        for t in t_samples:
            positions = {}
            for num, p in all_pos.items():
                idx = int(np.searchsorted(p["t"], t))
                idx = min(idx, len(p["t"]) - 1)
                positions[num] = {
                    "X": round(float(p["x"][idx]), 1),
                    "Y": round(float(p["y"][idx]), 1),
                    "Status": str(p["status"][idx]),
                }
            frames.append({
                "t": round(float(t - t_min), 2),
                "pct": round(float((t - t_min) / duration), 4),
                "positions": positions,
            })

        return frames, track, drivers, duration


async def forward_loop():
    """Read F1 messages from the queue and broadcast to all browser clients."""
    global clients
    while True:
        try:
            topic, data = f1_q.get_nowait()
            state_cache[topic] = data

            if clients:
                msg = json.dumps({"topic": topic, "data": data})
                dead = set()
                for ws in list(clients):
                    try:
                        await ws.send(msg)
                    except Exception:
                        dead.add(ws)
                clients -= dead
        except queue.Empty:
            await asyncio.sleep(0.02)   # poll at ~50 Hz


# ─────────────────────────────────────────────────────────────────────────────
# F1 SignalR client (runs in a background thread, auto-reconnects)
# ─────────────────────────────────────────────────────────────────────────────

F1_WS_URL    = "wss://livetiming.formula1.com/signalrcore"
F1_NEG_URL   = "https://livetiming.formula1.com/signalrcore/negotiate"
F1_TOPICS    = [
    "Heartbeat", "AudioStreams", "DriverList", "ExtrapolatedClock",
    "RaceControlMessages", "SessionInfo", "SessionStatus", "TeamRadio",
    "TimingAppData", "TimingStats", "TrackStatus", "WeatherData",
    "Position.z", "CarData.z", "ContentStreams", "SessionData",
    "TimingData", "TopThree", "RcmSeries", "LapCount",
]


def _decode_compressed(raw: str) -> dict | None:
    """Decode a zlib-compressed, base64-encoded F1 data payload."""
    try:
        decoded = base64.b64decode(raw)
        decompressed = zlib.decompress(decoded, -15)   # raw deflate (no header)
        return json.loads(decompressed)
    except Exception:
        return None


def _extract_positions(data: dict) -> dict:
    """Pull out the latest car positions from a decoded Position.z payload."""
    positions = {}
    entries_list = data.get("Position", [])
    if not entries_list:
        return positions
    # Take the most recent entry
    latest = entries_list[-1].get("Entries", {})
    for num, pos in latest.items():
        if isinstance(pos, dict) and "X" in pos:
            positions[num] = pos
    return positions


def _load_track_outline(circuit: str, year: int):
    """Load track X/Y coordinates from FastF1 (runs in a daemon thread)."""
    global _track_cache, _track_loading
    key = f"{year}_{circuit}"
    if key in _track_cache or key in _track_loading:
        return
    _track_loading.add(key)
    try:
        print(f"  Loading track map for {circuit} {year}…")
        schedule = fastf1.get_event_schedule(year, include_testing=False)

        # Match by Location or Country
        mask = schedule["Location"].str.contains(circuit, case=False, na=False)
        if not mask.any():
            mask = schedule["Country"].str.contains(circuit, case=False, na=False)
        if not mask.any():
            mask = schedule["EventName"].str.contains(circuit, case=False, na=False)
        if not mask.any():
            print(f"  No event found for: {circuit}")
            return

        round_num = int(schedule[mask].iloc[0]["RoundNumber"])

        # Try Qualifying first (fastest lap = cleanest track outline), then Race
        for sess_type in ["Q", "R", "FP2", "FP1"]:
            try:
                session = fastf1.get_session(year, round_num, sess_type)
                session.load(laps=True, telemetry=True, weather=False, messages=False)
                lap = session.laps.pick_fastest()
                if lap is None or (hasattr(lap, "empty") and lap.empty):
                    continue
                tel = lap.get_telemetry()
                if "X" not in tel.columns:
                    continue
                x = tel["X"].dropna().round(1).tolist()
                y = tel["Y"].dropna().round(1).tolist()
                if len(x) < 10:
                    continue

                # Rotation from circuit info (optional)
                rotation = 0
                try:
                    rotation = float(session.get_circuit_info().rotation)
                except Exception:
                    pass

                track = {
                    "x": x, "y": y,
                    "xMin": min(x), "xMax": max(x),
                    "yMin": min(y), "yMax": max(y),
                    "circuit": circuit,
                    "rotation": rotation,
                }
                _track_cache[key] = track
                f1_q.put_nowait(("__track__", track))
                print(f"  Track map ready ✅  {len(x)} points  ({sess_type})")
                return
            except Exception as e:
                print(f"  Session {sess_type} failed: {e}")
                continue
    except Exception as e:
        print(f"  Track load error: {e}")
    finally:
        _track_loading.discard(key)


def _on_f1_message(msg):
    """Called by signalrcore for each incoming message."""
    global _current_circuit, _current_year
    try:
        from signalrcore.hub.base_hub_connection import CompletionMessage

        def handle(topic: str, raw):
            global _current_circuit

            # Compressed streams end in '.z' — decode before forwarding
            if topic.endswith(".z") and isinstance(raw, str):
                decoded = _decode_compressed(raw)
                if decoded is None:
                    return
                base_topic = topic[:-2]   # e.g. "Position.z" -> "Position"
                if base_topic == "Position":
                    positions = _extract_positions(decoded)
                    if positions:
                        f1_q.put_nowait(("Position", positions))
                # (CarData.z forwarded as-is if needed in future)
                return

            # Normal JSON topics
            if isinstance(raw, str):
                try:
                    data = json.loads(raw)
                except Exception:
                    return
            else:
                data = raw

            if not isinstance(data, dict):
                return

            # Trigger track loading from SessionInfo
            if topic == "SessionInfo":
                circuit = (
                    data.get("Meeting", {})
                        .get("Circuit", {})
                        .get("ShortName", "")
                )
                if circuit and circuit != _current_circuit:
                    _current_circuit = circuit
                    threading.Thread(
                        target=_load_track_outline,
                        args=(circuit, _current_year),
                        daemon=True,
                        name="track-loader",
                    ).start()

            f1_q.put_nowait((topic, data))

        if isinstance(msg, CompletionMessage):
            for topic, raw in msg.result.items():
                handle(topic, raw)
        elif isinstance(msg, list) and len(msg) >= 2:
            handle(msg[0], msg[1])

    except Exception:
        pass


def run_f1_client():
    """Blocking loop — connects to F1 directly via signalrcore, reconnects on drop."""
    global f1_connected

    while True:
        connection = None
        try:
            print("  Connecting to F1 live timing…")

            # Pre-negotiate to get AWSALBCORS cookie (same as FastF1 does)
            headers = {}
            try:
                r = requests.options(F1_NEG_URL, headers=headers, timeout=10)
                cookie = r.cookies.get("AWSALBCORS", "")
                if cookie:
                    headers["Cookie"] = f"AWSALBCORS={cookie}"
            except Exception as e:
                print(f"  Pre-negotiate warning: {e}")

            # Build connection — omit access_token_factory entirely (no auth)
            options = {"verify_ssl": True, "headers": headers}
            connection = (
                HubConnectionBuilder()
                .with_url(F1_WS_URL, options=options)
                .build()
            )

            connected_event = threading.Event()
            closed_event = threading.Event()

            connection.on_open(lambda: connected_event.set())
            connection.on_close(lambda: closed_event.set())
            connection.on("feed", _on_f1_message)

            connection.start()
            connected_event.wait(timeout=15)

            if not connected_event.is_set():
                raise TimeoutError("Connection timed out after 15 s")

            print("  F1 live timing connected ✅")
            connection.send("Subscribe", [F1_TOPICS])
            f1_connected = True
            _notify_clients_f1_status(True)

            # Block until stream closes
            closed_event.wait()

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"  F1 connection error: {e}")
        finally:
            f1_connected = False
            _notify_clients_f1_status(False)
            try:
                if connection:
                    connection.stop()
            except Exception:
                pass

        print("  Reconnecting in 15 s…")
        time.sleep(15)


def _notify_clients_f1_status(connected: bool):
    """Thread-safe: tell all browser clients about F1 connection state."""
    # We can't await here (we're in a thread), so just enqueue a synthetic message
    f1_q.put_nowait(("__proxy__", {"connected": connected}))


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    # Start F1 client in background thread
    t = threading.Thread(target=run_f1_client, daemon=True, name="f1-signalr")
    t.start()

    print(f"\n  F1 Proxy ready  →  ws://localhost:{PORT}")
    print("  Open f1.html and click Connect\n")

    async with websockets.serve(ws_handler, "localhost", PORT):
        await forward_loop()


if __name__ == "__main__":
    print("=" * 50)
    print("  F1 Live Timing Proxy")
    print("=" * 50)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n  Proxy stopped.")
