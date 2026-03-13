import datetime
import threading

import requests
from meshtastic import BROADCAST_ADDR, mesh_pb2, portnums_pb2

import cfg
import plugins
import plugins.libcommand as LibCommand
import plugins.libdiscordutil as DiscordUtil


# =====================================
# DEFAULT CONFIGURATION
# =====================================

DEFAULTS = {
    "use_nws_alerts": True,
    "nws_latitude": 37.6872,
    "nws_longitude": -97.3301,
    "nws_check_interval": 300,
    "nws_alert_channel": 0,
    "nws_initial_startup_delay": 30,
    "nws_request_timeout": 20,
    "nws_failure_retry_delay": 60,
    "nws_agent_name": "Flyover-Mesh-NWS-Alerts",
    "nws_contact": "913-909-0608 Burke Jones",
}


# =====================================
# RUNTIME STATE
# =====================================

mesh_interface = None
discord_client = None
last_check_time = None
alert_state = {}
alert_state_initialized = False
thread_started = False
startup_delay_applied = False
retry_after_failure = False
failure_retry_used = False
command_registered = False
state_lock = threading.Lock()
wake_event = threading.Event()


def _now_string():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _capture_interface(interface):
    global mesh_interface

    if interface is None:
        return False

    captured_new_interface = False
    with state_lock:
        if mesh_interface is not interface:
            mesh_interface = interface
            captured_new_interface = True

    if captured_new_interface:
        print("[NWS] Interface captured")

    return captured_new_interface


def _capture_client(client):
    global discord_client

    if client is None:
        return

    with state_lock:
        discord_client = client


def _config_bool(key):
    value = cfg.config.get(key, DEFAULTS[key])
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _config_int(key):
    try:
        return int(cfg.config.get(key, DEFAULTS[key]))
    except (TypeError, ValueError):
        return int(DEFAULTS[key])


def _config_float(key):
    try:
        return float(cfg.config.get(key, DEFAULTS[key]))
    except (TypeError, ValueError):
        return float(DEFAULTS[key])


def _config_str(key):
    value = cfg.config.get(key, DEFAULTS[key])
    return str(value)


def _plugin_enabled():
    if not hasattr(cfg, "config") or not isinstance(cfg.config, dict):
        return DEFAULTS["use_nws_alerts"]
    return _config_bool("use_nws_alerts")


def _schedule_check_after_capture():
    global startup_delay_applied

    if not _plugin_enabled():
        return

    should_delay = False
    with state_lock:
        if not startup_delay_applied:
            startup_delay_applied = True
            should_delay = True

    initial_delay = _config_int("nws_initial_startup_delay")

    if should_delay and initial_delay > 0:
        print(f"[NWS] Delaying first alert check for {initial_delay} seconds")
        timer = threading.Timer(initial_delay, wake_event.set)
        timer.daemon = True
        timer.start()
    else:
        print("[NWS] Scheduling immediate alert check")
        wake_event.set()


def cmd_alertcheck(packet, interface, client, args):
    if not _plugin_enabled():
        return "NWS alerts are disabled in config"

    if _capture_interface(interface):
        _schedule_check_after_capture()
    _capture_client(client)

    with state_lock:
        last = last_check_time or "Not yet checked"
        tracked = len(alert_state)
        has_interface = mesh_interface is not None

    status = "RUNNING" if has_interface else "WAITING FOR INTERFACE"
    return f"NWS Monitor: {status}\nTracked alerts: {tracked}\nLast check: {last}"

def format_expiration(expires_raw):
    if not expires_raw:
        return "Unknown"

    try:
        expires_dt = datetime.datetime.fromisoformat(expires_raw.replace("Z", "+00:00"))
    except ValueError:
        return expires_raw

    return expires_dt.astimezone().strftime("%b %d %I:%M %p")


def _display_end_time(props):
    return props.get("ends") or props.get("expires")


def resolve_alert_channel(channel_index):
    try:
        incoming_ch = int(channel_index or 0)
    except (TypeError, ValueError):
        incoming_ch = 0

    try:
        cfg_ch = int(cfg.config.get("send_channel_index", 0))
    except (TypeError, ValueError):
        cfg_ch = 0

    if incoming_ch == 0 and cfg_ch != 0:
        return cfg_ch
    return incoming_ch


def send_broadcast(interface, message):
    packet = mesh_pb2.MeshPacket()
    packet.channel = _config_int("nws_alert_channel")
    packet.decoded.payload = message.encode("utf-8")
    packet.decoded.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP

    interface._sendPacket(packet, BROADCAST_ADDR)

    with state_lock:
        client = discord_client

    discord_channel = resolve_alert_channel(_config_int("nws_alert_channel"))
    DiscordUtil.send_msg(message, client, cfg.config, discord_channel)


def _build_new_message(event_type, expires_raw):
    return f"NWS {event_type}\nEnds {format_expiration(expires_raw)}"


def _build_updated_message(event_type, expires_raw):
    return f"NWS {event_type} UPDATED\nEnds {format_expiration(expires_raw)}"


def check_alerts():
    global last_check_time
    global retry_after_failure
    global failure_retry_used
    global alert_state_initialized

    with state_lock:
        interface = mesh_interface

    if interface is None:
        return

    headers = {
        "User-Agent": f"{_config_str('nws_agent_name')} ({_config_str('nws_contact')})"
    }
    check_started = _now_string()
    print(f"[NWS] Checking alerts at {check_started}")

    try:
        latitude = _config_float("nws_latitude")
        longitude = _config_float("nws_longitude")
        request_timeout = _config_int("nws_request_timeout")
        url = f"https://api.weather.gov/alerts/active?point={latitude},{longitude}"
        response = requests.get(url, headers=headers, timeout=request_timeout)
        response.raise_for_status()

        features = response.json().get("features", [])
        active_alerts = {}

        for alert in features:
            props = alert.get("properties", {})
            alert_id = alert.get("id")
            event_type = props.get("event", "")
            ends_raw = _display_end_time(props)
            expires_raw = props.get("expires")

            if not alert_id:
                continue
            if "Watch" not in event_type and "Warning" not in event_type:
                continue

            active_alerts[alert_id] = {
                "event": event_type,
                "ends": ends_raw,
                "expires": expires_raw,
            }

        outbound_messages = []

        with state_lock:
            previous_alerts = dict(alert_state)
            first_successful_snapshot = not alert_state_initialized

            if not first_successful_snapshot:
                for alert_id, details in active_alerts.items():
                    previous = previous_alerts.get(alert_id)
                    if previous is None:
                        outbound_messages.append(
                            _build_new_message(details["event"], details["ends"])
                        )
                    elif previous.get("ends") != details["ends"]:
                        outbound_messages.append(
                            _build_updated_message(details["event"], details["ends"])
                        )

            alert_state.clear()
            alert_state.update(active_alerts)
            alert_state_initialized = True
            last_check_time = check_started
            retry_after_failure = False
            failure_retry_used = False

        if first_successful_snapshot:
            print(
                f"[NWS] Initialized alert state with {len(active_alerts)} active watch/warning alert(s)"
            )
        elif outbound_messages:
            print(f"[NWS] Found {len(outbound_messages)} alert change(s)")
        else:
            print("[NWS] No new or updated alerts")

        for message in outbound_messages:
            send_broadcast(interface, message)
            print(f"[NWS] Broadcast: {message.replace(chr(10), ' | ')}")

    except Exception as exc:
        with state_lock:
            last_check_time = check_started
            if failure_retry_used:
                retry_after_failure = False
                failure_retry_used = False
            else:
                retry_after_failure = True
                failure_retry_used = True
        print(f"[NWS] Alert check failed: {exc}")


def alert_loop():
    global retry_after_failure

    print("[NWS] Alert loop started")

    while True:
        if not _plugin_enabled():
            wake_event.wait(5)
            wake_event.clear()
            continue

        with state_lock:
            if retry_after_failure:
                wait_seconds = _config_int("nws_failure_retry_delay")
            else:
                wait_seconds = _config_int("nws_check_interval")

        wake_event.wait(wait_seconds)
        wake_event.clear()
        check_alerts()


class pluginNWSAlerts(plugins.Base):
    def __init__(self):
        pass

    def start(self):
        global command_registered
        global thread_started

        if not command_registered:
            LibCommand.simpleCommand().registerCommand(
                "alertcheck",
                "Show NWS alert monitor status",
                cmd_alertcheck,
            )
            command_registered = True

        print(
            f"[NWS] use_nws_alerts raw value: {cfg.config.get('use_nws_alerts')!r}, "
            f"enabled={_plugin_enabled()}"
        )

        if not _plugin_enabled():
            print("[NWS] Plugin disabled in config")
            return

        with state_lock:
            if thread_started:
                return
            thread_started = True

        print("[NWS] Plugin starting")
        threading.Thread(target=alert_loop, daemon=True).start()

    def onConnect(self, interface, client):
        _capture_client(client)
        if _capture_interface(interface):
            _schedule_check_after_capture()
