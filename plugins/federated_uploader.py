"""
Federated Uploader Plugin for MeshLinkBeta

This plugin integrates the federated collector system into MeshLinkBeta.
It provides two upload strategies:

1. Real-time packet capture: Captures packets as they arrive
2. Periodic database export: Exports nodes.db data at regular intervals

Configuration (config.yml):
    federated_uploader:
        enabled: true
        collector_id: "meshlink-collector-01"
        api_url: "https://meshcollector.fly.dev"
        token: "your-secure-token-here"

        # Real-time packet capture
        outbox_db_path: "./federated_outbox.sqlite"
        enqueue_packet_types:
            - TEXT_MESSAGE_APP
            - POSITION_APP
            - NODEINFO_APP
            - TELEMETRY_APP
            - TRACEROUTE_APP
            - ROUTING_APP

        # Periodic database export
        export_enabled: true
        export_interval_minutes: 60
        nodes_db_path: "./nodes.db"
        export_hours_lookback: 2
"""

import logging
import time
import sys
import os
import threading
import sqlite3
import requests
import gzip
import json
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

# Add federated-meshtastic collector to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../federated-meshtastic/collector'))

try:
    from outbox import OutboxManager
except ImportError:
    logging.error("Failed to import OutboxManager. Make sure federated-meshtastic/collector is available.")
    OutboxManager = None

from plugins import Base
import plugins.liblogger as logger


class Plugin(Base):
    """Federated uploader plugin for MeshLinkBeta."""

    plugin_name = "federated_uploader"

    def __init__(self):
        """Initialize the federated uploader plugin."""
        super().__init__()

        # Initialize attributes with defaults first
        self.enabled = False
        self.collector_id = None
        self.api_url = None
        self.token = None
        self.outbox = None
        self.outbox_db_path = './federated_outbox.sqlite'
        self.enqueue_types = set()
        self.export_enabled = False
        self.export_thread = None
        self.export_stop_event = threading.Event()
        self.last_export_time = None
        self.nodes_db_path = None
        self.export_lookback_hours = 2
        self.export_interval = 3600

        if OutboxManager is None:
            logger.warn("OutboxManager not available. Real-time packet capture disabled.")
            logger.info("Periodic database export will still work if export_enabled=true")
            # Don't return - continue with initialization for export-only mode

        # Load config from cfg module
        import cfg
        config = cfg.config.get('federated_uploader', {})

        self.enabled = config.get('enabled', False)

        if not self.enabled:
            logger.info("Federated uploader plugin is disabled")
            return

        # Core configuration
        self.collector_id = config.get('collector_id', 'meshlink-collector')
        self.api_url = config.get('api_url', 'https://meshcollector.fly.dev').rstrip('/')
        self.token = config.get('token', '')

        # Real-time packet capture configuration
        self.outbox_db_path = config.get('outbox_db_path', './federated_outbox.sqlite')
        self.enqueue_types = set(config.get('enqueue_packet_types', [
            'TEXT_MESSAGE_APP',
            'POSITION_APP',
            'NODEINFO_APP',
            'TELEMETRY_APP',
            'TRACEROUTE_APP',
            'ROUTING_APP',
        ]))

        # Periodic export configuration
        self.export_enabled = config.get('export_enabled', True)
        self.export_interval = config.get('export_interval_minutes', 60) * 60  # Convert to seconds
        self.nodes_db_path = config.get('nodes_db_path', './nodes.db')
        self.export_lookback_hours = config.get('export_hours_lookback', 2)

        # Initialize outbox manager for real-time packets (optional)
        if OutboxManager is not None:
            try:
                self.outbox = OutboxManager(self.outbox_db_path, self.collector_id)
                logger.info(f"OutboxManager initialized for real-time capture")

                # Log initial stats
                stats = self.outbox.get_stats()
                logger.info(f"Outbox stats: {stats}")

            except Exception as e:
                logger.warn(f"Failed to initialize OutboxManager: {e}")
                logger.warn("Real-time packet capture disabled, periodic export still available")
                self.outbox = None
        else:
            logger.info("Running in export-only mode (no real-time packet capture)")

        logger.info(f"Federated uploader initialized: collector_id='{self.collector_id}', export_enabled={self.export_enabled}")

        # Export thread control is already initialized above
        # self.last_export_time is already initialized above

    def start(self):
        """Start the plugin and begin periodic exports if enabled."""
        if not self.enabled:
            return

        logger.info("Starting federated uploader plugin")

        # Validate API URL (required)
        if not self.api_url:
            logger.warn("No API URL configured! Set 'api_url' in config.yml")
            return

        # Token is optional for MeshMonitor integration
        if not self.token:
            logger.info("No API token configured (not needed for MeshMonitor)")

        # Start periodic export thread
        if self.export_enabled:
            if not os.path.exists(self.nodes_db_path):
                logger.warn(f"nodes.db not found at {self.nodes_db_path}. Periodic export disabled.")
                self.export_enabled = False
            else:
                logger.info(f"Starting periodic export thread (every {self.export_interval/60:.0f} minutes)")
                self.export_thread = threading.Thread(target=self._export_loop, daemon=True)
                self.export_thread.start()

    def onReceive(self, packet: Dict[str, Any], interface, client):
        """
        Called when a packet is received (real-time capture).

        Args:
            packet: The received packet
            interface: The Meshtastic interface
            client: The Discord client (if enabled)
        """
        if not self.enabled or self.outbox is None:
            return

        try:
            # Extract packet type
            packet_type = None
            if 'decoded' in packet and 'portnum' in packet['decoded']:
                packet_type = packet['decoded']['portnum']

            # Check if we should enqueue this packet type
            if packet_type and packet_type not in self.enqueue_types:
                logger.info(f"Skipping packet type {packet_type}")
                return

            # Build event data
            event_data = self._build_event_data(packet, interface)

            # Enqueue for upload
            event_id = self.outbox.enqueue('packet', event_data)

            if event_id:
                logger.info(f"Enqueued packet {packet.get('id')} from {event_data['from_node']}")
            else:
                logger.info(f"Duplicate packet {packet.get('id')}")

        except Exception as e:
            logger.warn(f"Error processing packet for federated upload: {e}")

    def _export_loop(self):
        """Background thread that periodically exports nodes.db data."""
        logger.info("Export loop started")

        # Run first export after a short delay to let MeshLinkBeta settle
        time.sleep(30)

        while not self.export_stop_event.is_set():
            try:
                self._run_export()
            except Exception as e:
                logger.warn(f"Error during periodic export: {e}")

            # Sleep until next export
            self.export_stop_event.wait(self.export_interval)

        logger.info("Export loop stopped")

    def _run_export(self):
        """Run a single export of nodes.db data to the API."""
        logger.info("Starting periodic export from nodes.db")
        start_time = time.time()

        try:
            # Connect to nodes.db
            conn = sqlite3.connect(self.nodes_db_path)
            conn.row_factory = sqlite3.Row

            # Calculate cutoff time
            cutoff = (datetime.utcnow() - timedelta(hours=self.export_lookback_hours)).isoformat() + "Z"

            # Export data
            nodes = self._export_nodes(conn, cutoff)
            packets = self._export_packets(conn, cutoff)
            topology = self._export_topology(conn, cutoff)
            traceroutes = self._export_traceroutes(conn, cutoff)

            conn.close()

            # Upload if we have any data
            if any([nodes, packets, topology, traceroutes]):
                data = {
                    'nodes': nodes,
                    'packets': packets,
                    'topology': topology,
                    'traceroutes': traceroutes
                }

                result = self._upload_batch(data)

                elapsed = time.time() - start_time
                logger.infogreen(f"Export completed in {elapsed:.1f}s: {result['summary']}")
                self.last_export_time = datetime.utcnow()
            else:
                logger.info(f"No new data to export (lookback: {self.export_lookback_hours}h)")

        except Exception as e:
            logger.warn(f"Export failed: {e}")

    def _export_nodes(self, conn: sqlite3.Connection, since: str) -> List[Dict[str, Any]]:
        """Export nodes from nodes table."""
        query = "SELECT * FROM nodes WHERE updated_at > ? ORDER BY last_seen_utc DESC"
        rows = conn.execute(query, (since,)).fetchall()

        nodes = []
        for row in rows:
            node = {
                'node_id': row['node_id'],
                'node_num': row['node_num'],
                'short_name': row['short_name'],
                'long_name': row['long_name'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'altitude': row['altitude'],
                'last_seen_utc': row['last_seen_utc'],
                'first_seen_utc': row['first_seen_utc'],
                'total_packets_received': row['total_packets_received'],
                'hardware_model': row['hardware_model'],
                'firmware_version': row['firmware_version'],
                'is_mqtt': bool(row['is_mqtt']) if row['is_mqtt'] is not None else None,
                'battery_level': row['battery_level'],
                'voltage': row['voltage'],
                'is_charging': bool(row['is_charging']) if row['is_charging'] is not None else None,
                'is_powered': bool(row['is_powered']) if row['is_powered'] is not None else None,
                'last_battery_update_utc': row['last_battery_update_utc']
            }
            # Remove None values
            nodes.append({k: v for k, v in node.items() if v is not None})

        logger.info(f"Exported {len(nodes)} nodes")
        return nodes

    def _export_packets(self, conn: sqlite3.Connection, since: str) -> List[Dict[str, Any]]:
        """Export packets from packet_history table (limited)."""
        query = """
            SELECT * FROM packet_history
            WHERE received_at_utc > ?
            ORDER BY received_at_utc DESC
            LIMIT 5000
        """
        rows = conn.execute(query, (since,)).fetchall()

        packets = []
        for row in rows:
            packet = {
                'node_id': row['node_id'],
                'received_at_utc': row['received_at_utc'],
                'packet_type': row['packet_type'],
                'channel_index': row['channel_index'],
                'hop_start': row['hop_start'],
                'hop_limit': row['hop_limit'],
                'hops_away': row['hops_away'],
                'via_mqtt': bool(row['via_mqtt']) if row['via_mqtt'] is not None else None,
                'relay_node_id': row['relay_node_id'],
                'relay_node_name': row['relay_node_name'],
                'rx_snr': row['rx_snr'],
                'rx_rssi': row['rx_rssi'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'altitude': row['altitude'],
                'battery_level': row['battery_level'],
                'voltage': row['voltage'],
                'is_charging': bool(row['is_charging']) if row['is_charging'] is not None else None,
                'temperature': row['temperature'],
                'humidity': row['humidity'],
                'pressure': row['pressure']
            }
            # Remove None values and sensitive fields
            packets.append({k: v for k, v in packet.items() if v is not None})

        logger.info(f"Exported {len(packets)} packets")
        return packets

    def _export_topology(self, conn: sqlite3.Connection, since: str) -> List[Dict[str, Any]]:
        """Export topology links from network_topology table."""
        query = """
            SELECT * FROM network_topology
            WHERE last_heard_utc > ? AND is_active = 1
            ORDER BY last_heard_utc DESC
        """
        rows = conn.execute(query, (since,)).fetchall()

        links = []
        for row in rows:
            link = {
                'source_node_id': row['source_node_id'],
                'neighbor_node_id': row['neighbor_node_id'],
                'first_heard_utc': row['first_heard_utc'],
                'last_heard_utc': row['last_heard_utc'],
                'total_packets': row['total_packets'],
                'avg_snr': row['avg_snr'],
                'avg_rssi': row['avg_rssi'],
                'min_snr': row['min_snr'],
                'max_snr': row['max_snr'],
                'min_rssi': row['min_rssi'],
                'max_rssi': row['max_rssi'],
                'link_quality_score': row['link_quality_score'],
                'is_active': bool(row['is_active']) if row['is_active'] is not None else None,
                'last_hop_count': row['last_hop_count']
            }
            # Remove None values
            links.append({k: v for k, v in link.items() if v is not None})

        logger.info(f"Exported {len(links)} topology links")
        return links

    def _export_traceroutes(self, conn: sqlite3.Connection, since: str) -> List[Dict[str, Any]]:
        """Export traceroutes from traceroutes table."""
        query = """
            SELECT * FROM traceroutes
            WHERE received_at_utc > ?
            ORDER BY received_at_utc DESC
            LIMIT 1000
        """
        rows = conn.execute(query, (since,)).fetchall()

        traceroutes = []
        for row in rows:
            trace = {
                'from_node_id': row['from_node_id'],
                'to_node_id': row['to_node_id'],
                'route_json': row['route_json'],
                'hop_count': row['hop_count'],
                'received_at_utc': row['received_at_utc'],
                'snr_data': row['snr_data'],
                'packet_id': row['packet_id']
            }
            # Remove None values
            traceroutes.append({k: v for k, v in trace.items() if v is not None})

        logger.info(f"Exported {len(traceroutes)} traceroutes")
        return traceroutes

    def _upload_batch(self, data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Upload batch data to MeshMonitor API."""
        # Build payload
        payload = {
            "collector_id": self.collector_id,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "schema_version": 1,
            "data": data
        }

        # Send as JSON (MeshMonitor handles it directly)
        json_bytes = json.dumps(payload).encode('utf-8')

        total_items = sum(len(items) for items in data.values())
        logger.info(f"Uploading {total_items} items to MeshMonitor (size: {len(json_bytes):,} bytes)")

        # Upload to MeshMonitor endpoint
        try:
            response = requests.post(
                f"{self.api_url}/api/ingest/meshlink",
                json=payload,
                headers={
                    "Content-Type": "application/json"
                },
                timeout=60
            )

            if response.status_code == 200:
                result = response.json()
                return result
            else:
                logger.warn(f"Upload failed: {response.status_code} - {response.text}")
                response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.warn(f"Upload error: {e}")
            raise

    def _build_event_data(self, packet: Dict[str, Any], interface) -> Dict[str, Any]:
        """
        Build event data from MeshLinkBeta packet format for real-time capture.

        Args:
            packet: Raw packet from Meshtastic
            interface: Meshtastic interface (for node lookups)

        Returns:
            Event data dictionary
        """
        # Basic packet info
        event_data = {
            'packet_id': packet.get('id', 0),
            'from_node': f"!{packet.get('from', 0):08x}",
            'to_node': f"!{packet.get('to', 0):08x}",
            'timestamp': time.time(),
            'channel': packet.get('channel', 0),
            'hop_limit': packet.get('hopLimit'),
            'hop_start': packet.get('hopStart'),
        }

        # RX metadata
        if 'rxRssi' in packet:
            event_data['rssi'] = packet['rxRssi']
        if 'rxSnr' in packet:
            event_data['snr'] = packet['rxSnr']

        # Relay information
        if 'viaMqtt' in packet and packet['viaMqtt']:
            event_data['via_mqtt'] = True

        # Relay node
        if 'relayNode' in packet and packet['relayNode']:
            relay = packet['relayNode']
            if isinstance(relay, int):
                event_data['relay_node'] = f"!{relay:08x}"
            else:
                event_data['relay_node'] = str(relay)

        # Decoded data
        if 'decoded' in packet:
            decoded = packet['decoded']
            portnum = decoded.get('portnum', '')

            event_data['port_num'] = portnum
            event_data['type'] = portnum

            # Position data
            if portnum == 'POSITION_APP' and 'position' in decoded:
                pos = decoded['position']
                event_data['from_node_info'] = event_data.get('from_node_info', {})
                if 'latitudeI' in pos:
                    event_data['from_node_info']['latitude'] = pos['latitudeI'] / 1e7
                elif 'latitude' in pos:
                    event_data['from_node_info']['latitude'] = pos['latitude']

                if 'longitudeI' in pos:
                    event_data['from_node_info']['longitude'] = pos['longitudeI'] / 1e7
                elif 'longitude' in pos:
                    event_data['from_node_info']['longitude'] = pos['longitude']

                if 'altitude' in pos:
                    event_data['from_node_info']['altitude'] = pos['altitude']

            # Node info
            if portnum == 'NODEINFO_APP' and 'user' in decoded:
                user = decoded['user']
                event_data['from_node_info'] = event_data.get('from_node_info', {})
                event_data['from_node_info'].update({
                    'short_name': user.get('shortName'),
                    'long_name': user.get('longName'),
                    'hardware': user.get('hwModel'),
                    'role': user.get('role'),
                })

            # Telemetry data
            if portnum == 'TELEMETRY_APP' and 'telemetry' in decoded:
                telem = decoded['telemetry']
                if 'deviceMetrics' in telem:
                    metrics = telem['deviceMetrics']
                    event_data['from_node_info'] = event_data.get('from_node_info', {})
                    if 'batteryLevel' in metrics:
                        event_data['from_node_info']['battery_level'] = metrics['batteryLevel']
                    if 'voltage' in metrics:
                        event_data['from_node_info']['voltage'] = metrics['voltage']

            # Text message (for payload type, not content for privacy)
            if portnum == 'TEXT_MESSAGE_APP':
                event_data['has_text'] = True

            # Traceroute
            if portnum == 'TRACEROUTE_APP' and 'route' in decoded:
                # Store traceroute as separate event
                self._enqueue_traceroute(packet, decoded, interface)

        return event_data

    def _enqueue_traceroute(self, packet: Dict[str, Any], decoded: Dict[str, Any], interface):
        """
        Enqueue a traceroute event.

        Args:
            packet: Full packet
            decoded: Decoded traceroute data
            interface: Meshtastic interface
        """
        try:
            route = decoded.get('route', [])
            if not route:
                return

            trace_data = {
                'trace_id': f"{packet.get('id', 0)}-{int(time.time())}",
                'source_node': f"!{packet.get('from', 0):08x}",
                'dest_node': f"!{packet.get('to', 0):08x}",
                'timestamp': time.time(),
                'hops': [
                    {
                        'node_id': f"!{node:08x}" if isinstance(node, int) else str(node),
                        'rssi': packet.get('rxRssi'),
                        'snr': packet.get('rxSnr'),
                        'timestamp': time.time(),
                    }
                    for node in route
                ]
            }

            event_id = self.outbox.enqueue('trace_event', trace_data)
            if event_id:
                logger.info(f"Enqueued traceroute {trace_data['trace_id']}")

        except Exception as e:
            logger.warn(f"Error processing traceroute: {e}")

    def get_stats(self) -> Optional[Dict[str, Any]]:
        """
        Get combined statistics.

        Returns:
            Dictionary with stats or None if disabled
        """
        if not self.enabled or self.outbox is None:
            return None

        try:
            stats = {
                'outbox': self.outbox.get_stats(),
                'export': {
                    'enabled': self.export_enabled,
                    'interval_minutes': self.export_interval / 60,
                    'last_export': self.last_export_time.isoformat() if self.last_export_time else None,
                    'lookback_hours': self.export_lookback_hours
                }
            }
            return stats
        except Exception as e:
            logger.warn(f"Error getting stats: {e}")
            return None

    def cleanup(self, days: int = 7) -> int:
        """
        Clean up old sent events from outbox.

        Args:
            days: Remove events older than this many days

        Returns:
            Number of events removed
        """
        if not self.enabled or self.outbox is None:
            return 0

        try:
            count = self.outbox.cleanup_old_sent(days)
            logger.info(f"Cleaned up {count} old events (older than {days} days)")
            return count
        except Exception as e:
            logger.warn(f"Error during cleanup: {e}")
            return 0

    def __del__(self):
        """Cleanup when plugin is destroyed."""
        if hasattr(self, 'export_stop_event'):
            self.export_stop_event.set()
        if hasattr(self, 'export_thread') and self.export_thread:
            self.export_thread.join(timeout=5)
