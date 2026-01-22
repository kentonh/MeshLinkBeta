"""
Node Tracking Plugin
Captures and stores information about all nodes and packets received on the mesh network.
"""

import plugins
import cfg
import json
import base64
from datetime import datetime
from typing import Optional, Dict, Any
import plugins.liblogger as logger
import plugins.libmesh as LibMesh
from plugins.libnode_db import NodeDatabase
from plugins.libnode_export import NodeExporter
import threading
import time

class NodeTracking(plugins.Base):
    """Main plugin for node tracking and network topology"""

    # Class variables shared across all instances
    _db = None
    _exporter = None
    _config = None
    _last_export_time = 0
    _export_interval = 60  # Export every 60 seconds
    _topology_cleanup_timer = None
    _auto_traceroute_timer = None
    _auto_telemetry_timer = None
    _interface = None
    _traceroute_in_progress = False
    _telemetry_request_in_progress = False
    
    def __init__(self):
        pass
    
    def start(self):
        """Initialize the node tracking system"""
        logger.info("Loading node tracking plugin")
        
        # Load configuration
        NodeTracking._config = cfg.config.get('node_tracking', {
            'enabled': True,
            'max_packets_per_node': 1000,
            'database_path': './nodes.db',
            'json_export_path': './nodes.json',
            'auto_export_json': True,
            'web_server': {
                'enabled': False,  # Will be implemented later
                'host': '0.0.0.0',
                'port': 8080
            },
            'track_packet_types': [
                'TEXT_MESSAGE_APP',
                'POSITION_APP',
                'NODEINFO_APP',
                'TELEMETRY_APP',
                'ROUTING_APP',
                'TRACEROUTE_APP'
            ],
            'topology': {
                'enabled': True,
                'link_timeout_minutes': 60,
                'min_packets_for_link': 3,
                'calculate_link_quality': True
            },
            'auto_traceroute': {
                'enabled': False,
                'interval_minutes': 30,
                'traceroute_age_hours': 4,
                'active_threshold_minutes': 60,
                'hop_limit': 7,
                'max_per_cycle': 5,
                'delay_seconds': 10,
                'exclude_mqtt_nodes': True
            },
            'auto_telemetry': {
                'enabled': False,
                'interval_minutes': 15,
                'request_age_hours': 2,
                'active_threshold_minutes': 120,
                'max_per_cycle': 10,
                'delay_seconds': 5,
                'exclude_mqtt_nodes': True,
                'skip_nodes_with_recent_traceroute': True
            }
        })
        
        if not NodeTracking._config.get('enabled', True):
            logger.info("Node tracking is disabled in config")
            return
        
        # Initialize database
        try:
            db_path = NodeTracking._config.get('database_path', './nodes.db')
            NodeTracking._db = NodeDatabase(db_path)
            NodeTracking._exporter = NodeExporter(NodeTracking._db)
            logger.infogreen(f"Node tracking database initialized at {db_path}")
        except Exception as e:
            logger.warn(f"Failed to initialize node tracking: {e}")
            NodeTracking._config['enabled'] = False
            return
        
        # Start topology cleanup timer if enabled
        if NodeTracking._config.get('topology', {}).get('enabled', True):
            self._start_topology_cleanup()
    
    def onReceive(self, packet, interface, client):
        """Handle incoming packets"""
        if not NodeTracking._config or not NodeTracking._config.get('enabled', True) or not NodeTracking._db:
            return
        
        try:
            # Extract basic packet info
            node_id = packet.get('fromId')
            if not node_id:
                return
            
            # Extract node information
            node_data = self._extract_node_data(packet, interface)
            
            # Update node in database
            if node_data:
                NodeTracking._db.upsert_node(node_data)
            
            # Check if we should track this packet type
            portnum = packet.get('decoded', {}).get('portnum', '')
            track_types = NodeTracking._config.get('track_packet_types', [])

            if portnum in track_types or not track_types:
                # Extract and store packet data
                packet_data = self._extract_packet_data(packet, interface)
                if packet_data:
                    max_packets = NodeTracking._config.get('max_packets_per_node', 1000)
                    NodeTracking._db.insert_packet(packet_data, max_packets)

                    # Log when TRACEROUTE packets are stored
                    if portnum == 'TRACEROUTE_APP':
                        logger.info(f"Stored TRACEROUTE packet from {node_id}")

            # Update topology if enabled
            if NodeTracking._config.get('topology', {}).get('enabled', True):
                self._update_topology(packet, interface)

            # Process traceroute packets for detailed topology
            if portnum == 'TRACEROUTE_APP':
                self._process_traceroute(packet, interface)

            # Process telemetry packets - check if this is a response to our request
            if portnum == 'TELEMETRY_APP':
                self._process_telemetry_response(packet, interface)

            # Auto-export if enabled
            if NodeTracking._config.get('auto_export_json', False):
                current_time = time.time()
                if current_time - NodeTracking._last_export_time > NodeTracking._export_interval:
                    self._export_data()
                    NodeTracking._last_export_time = current_time
                    
        except Exception as e:
            logger.warn(f"Error in node tracking onReceive: {e}")
    
    def _extract_node_data(self, packet: Dict[str, Any], interface) -> Optional[Dict[str, Any]]:
        """Extract node information from packet"""
        try:
            node_id = packet.get('fromId')
            if not node_id:
                return None
            
            node_data = {
                'node_id': node_id,
                'node_num': packet.get('from'),
                'is_mqtt': packet.get('viaMqtt', False)
            }
            
            # Get node info from interface
            node = LibMesh.getNode(interface, packet)
            if node:
                if 'user' in node:
                    node_data['short_name'] = node['user'].get('shortName')
                    node_data['long_name'] = node['user'].get('longName')
                    node_data['hardware_model'] = node['user'].get('hwModel')
                
                # Position data
                if 'position' in node:
                    pos = node['position']
                    node_data['latitude'] = pos.get('latitude')
                    node_data['longitude'] = pos.get('longitude')
                    node_data['altitude'] = pos.get('altitude')
                
                # Device metrics (battery, etc.)
                if 'deviceMetrics' in node:
                    metrics = node['deviceMetrics']
                    node_data['battery_level'] = metrics.get('batteryLevel')
                    node_data['voltage'] = metrics.get('voltage')
                    node_data['is_charging'] = metrics.get('airUtilTx') is not None  # Approximate
            
            # Extract from packet if NODEINFO_APP
            if packet.get('decoded', {}).get('portnum') == 'NODEINFO_APP':
                user_info = packet.get('decoded', {}).get('user', {})
                if user_info:
                    node_data['short_name'] = user_info.get('shortName')
                    node_data['long_name'] = user_info.get('longName')
                    node_data['hardware_model'] = user_info.get('hwModel')
            
            # Extract position from POSITION_APP packet
            if packet.get('decoded', {}).get('portnum') == 'POSITION_APP':
                position = packet.get('decoded', {}).get('position', {})
                if position:
                    node_data['latitude'] = position.get('latitude')
                    node_data['longitude'] = position.get('longitude')
                    node_data['altitude'] = position.get('altitude')
            
            # Extract telemetry from TELEMETRY_APP packet
            if packet.get('decoded', {}).get('portnum') == 'TELEMETRY_APP':
                telemetry = packet.get('decoded', {}).get('telemetry', {})
                if 'deviceMetrics' in telemetry:
                    metrics = telemetry['deviceMetrics']
                    node_data['battery_level'] = metrics.get('batteryLevel')
                    node_data['voltage'] = metrics.get('voltage')
            
            return node_data
            
        except Exception as e:
            logger.warn(f"Error extracting node data: {e}")
            return None
    
    def _match_relay_node(self, partial_id: int, interface, source_node_id: str) -> Optional[Dict[str, str]]:
        """
        Match a partial relay node ID (last byte) to an actual node.

        Args:
            partial_id: The partial node ID from relayNode field (last byte)
            interface: Meshtastic interface with node database
            source_node_id: The source node ID (to exclude from matches)

        Returns:
            Dict with 'id' and 'name' of matched node, or None if no match
        """
        try:
            matches = []

            # First, try to match using interface.nodes (fast, in-memory)
            if hasattr(interface, 'nodes') and interface.nodes:
                logger.info(f"Matching relay node {partial_id:#x} for packet from {source_node_id}, checking {len(interface.nodes)} interface nodes")

                # Check each known node
                for node_num, node_info in interface.nodes.items():
                    # Skip the source node (packet didn't relay through itself)
                    node_id = node_info.get('user', {}).get('id')
                    if node_id == source_node_id:
                        continue

                    # Extract last byte of this node's number
                    # relayNode contains the last 8 bits of the node number
                    # Ensure node_num is an integer (might be string in some cases)
                    try:
                        node_num_int = int(node_num) if isinstance(node_num, str) else node_num
                    except (ValueError, TypeError):
                        continue

                    last_byte = node_num_int & 0xFF

                    if last_byte == (partial_id & 0xFF):
                        # Found a match!
                        user = node_info.get('user', {})
                        node_name = user.get('longName') or user.get('shortName') or node_id or f"!{node_num_int:08x}"

                        logger.info(f"  Found interface match: {node_id or f'!{node_num_int:08x}'} ({node_name}) - last byte {last_byte:#x}")

                        # Get additional info for heuristics
                        snr = node_info.get('snr', -999)
                        last_heard = node_info.get('lastHeard', 0)

                        matches.append({
                            'id': node_id or f"!{node_num_int:08x}",
                            'name': node_name,
                            'num': node_num_int,
                            'snr': snr,
                            'last_heard': last_heard
                        })

            # If no matches in interface.nodes, try database as fallback
            if not matches and NodeTracking._db:
                logger.info(f"No interface match for {partial_id:#x}, checking database")

                try:
                    conn = NodeTracking._db._get_connection()
                    cursor = conn.cursor()

                    # Find nodes where last byte of node_num matches
                    cursor.execute("""
                        SELECT node_id, node_num, long_name, short_name, last_seen_utc, total_packets_received
                        FROM nodes
                        WHERE node_id != ? AND node_num IS NOT NULL
                    """, (source_node_id,))

                    db_nodes = cursor.fetchall()
                    logger.info(f"  Checking {len(db_nodes)} database nodes")

                    for row in db_nodes:
                        node_num = row['node_num']
                        if node_num is None:
                            continue

                        try:
                            node_num_int = int(node_num) if isinstance(node_num, str) else node_num
                        except (ValueError, TypeError):
                            continue

                        last_byte = node_num_int & 0xFF

                        if last_byte == (partial_id & 0xFF):
                            node_id = row['node_id']
                            node_name = row['long_name'] or row['short_name'] or node_id

                            logger.info(f"  Found database match: {node_id} ({node_name}) - last byte {last_byte:#x}")

                            matches.append({
                                'id': node_id,
                                'name': node_name,
                                'num': node_num_int,
                                'snr': -999,  # No SNR data from database
                                'last_heard': 0,  # Could parse last_seen_utc but not critical
                                'total_packets': row['total_packets_received'] or 0
                            })

                except Exception as e:
                    logger.warn(f"Error checking database for relay match: {e}")

            if not matches:
                logger.info(f"  No match found for relay node {partial_id:#x}")
                return None

            if len(matches) == 1:
                # Single match - use it
                return matches[0]

            # Multiple matches - use heuristics to pick best one
            # Prefer nodes with:
            # 1. Recent activity (last_heard from interface, or total_packets from db)
            # 2. Better signal quality (SNR)

            # Sort by last_heard (most recent first), then by SNR (best first), then by total_packets
            matches.sort(key=lambda x: (x.get('last_heard', 0), x.get('snr', -999), x.get('total_packets', 0)), reverse=True)

            best_match = matches[0]

            # Log when there are multiple matches
            if len(matches) > 1:
                others = ', '.join([m['name'] for m in matches[1:]])
                logger.info(f"Multiple relay matches for {partial_id:#x}: chose {best_match['name']}, also matched: {others}")

            return best_match

        except Exception as e:
            logger.warn(f"Error matching relay node: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _extract_packet_data(self, packet: Dict[str, Any], interface) -> Optional[Dict[str, Any]]:
        """Extract packet data for storage"""
        try:
            node_id = packet.get('fromId')
            if not node_id:
                return None
            
            decoded = packet.get('decoded', {})
            
            packet_data = {
                'node_id': node_id,
                'received_at_utc': datetime.utcnow().isoformat(),
                'packet_type': decoded.get('portnum'),
                'channel_index': packet.get('channel', 0),
                'hop_start': packet.get('hopStart'),
                'hop_limit': packet.get('hopLimit'),
                'via_mqtt': packet.get('viaMqtt', False),
                'rx_snr': packet.get('rxSnr'),
                'rx_rssi': packet.get('rxRssi'),
                'raw_packet': self._serialize_packet(packet)
            }

            # Calculate hops away
            if packet_data['hop_start'] and packet_data['hop_limit']:
                packet_data['hops_away'] = packet_data['hop_start'] - packet_data['hop_limit']

            # Extract relay node (if available in packet)
            # As of Meshtastic firmware 2.x, relayNode field is available
            # NOTE: relayNode contains only the LAST BITS of the node ID (not full ID)
            # We need to match it against known nodes to find the actual relay
            relay_node_partial = packet.get('relayNode')
            if relay_node_partial is not None and packet_data.get('hops_away', 0) > 0:
                matched_relay = self._match_relay_node(relay_node_partial, interface, node_id)
                if matched_relay:
                    # Validate that we got a full node ID (starts with !)
                    matched_id = matched_relay['id']
                    if isinstance(matched_id, str) and matched_id.startswith('!'):
                        packet_data['relay_node_id'] = matched_id
                        packet_data['relay_node_name'] = matched_relay['name']
                        logger.info(f"Packet from {node_id} relayed via {matched_relay['name']} (matched {relay_node_partial:#x})")
                    else:
                        logger.warn(f"Matched relay node has invalid ID format: {matched_id} for packet from {node_id}")
                else:
                    # Store partial ID even if we can't match it yet
                    # This preserves relay data for future retroactive matching
                    packet_data['relay_node_id'] = str(relay_node_partial)
                    logger.info(f"Could not match relay node {relay_node_partial:#x} for packet from {node_id}, storing partial ID for future matching")
            
            # Extract position if POSITION_APP
            if decoded.get('portnum') == 'POSITION_APP':
                position = decoded.get('position', {})
                packet_data['latitude'] = position.get('latitude')
                packet_data['longitude'] = position.get('longitude')
                packet_data['altitude'] = position.get('altitude')
            
            # Extract telemetry if TELEMETRY_APP
            if decoded.get('portnum') == 'TELEMETRY_APP':
                telemetry = decoded.get('telemetry', {})
                
                if 'deviceMetrics' in telemetry:
                    metrics = telemetry['deviceMetrics']
                    packet_data['battery_level'] = metrics.get('batteryLevel')
                    packet_data['voltage'] = metrics.get('voltage')
                    packet_data['temperature'] = metrics.get('temperature')
                
                if 'environmentMetrics' in telemetry:
                    env = telemetry['environmentMetrics']
                    packet_data['temperature'] = env.get('temperature')
                    packet_data['humidity'] = env.get('relativeHumidity')
                    packet_data['pressure'] = env.get('barometricPressure')
            
            # Extract text message if TEXT_MESSAGE_APP
            if decoded.get('portnum') == 'TEXT_MESSAGE_APP':
                packet_data['message_text'] = decoded.get('text')

            # Extract traceroute data if TRACEROUTE_APP
            if decoded.get('portnum') == 'TRACEROUTE_APP':
                traceroute = decoded.get('traceroute', {})
                route = traceroute.get('route', [])
                if route:
                    # Store the route as JSON
                    import json
                    packet_data['message_text'] = f"Traceroute: {len(route)} hops"
                    # Store full route in raw_packet field (already serialized)

            return packet_data
            
        except Exception as e:
            logger.warn(f"Error extracting packet data: {e}")
            return None
    
    def _serialize_packet(self, packet: Dict[str, Any]) -> str:
        """Serialize packet to JSON, handling bytes and protobuf objects"""
        try:
            def convert_to_serializable(obj):
                """Convert non-JSON-serializable objects for storage"""
                # Handle bytes
                if isinstance(obj, bytes):
                    return base64.b64encode(obj).decode('utf-8')
                
                # Handle dictionaries recursively
                elif isinstance(obj, dict):
                    return {k: convert_to_serializable(v) for k, v in obj.items()}
                
                # Handle lists recursively
                elif isinstance(obj, list):
                    return [convert_to_serializable(item) for item in obj]
                
                # Handle protobuf objects and other complex types
                elif hasattr(obj, '__class__') and obj.__class__.__module__ not in ['builtins', '__builtin__']:
                    # Try to convert to string representation
                    try:
                        return str(obj)
                    except:
                        return f"<{obj.__class__.__name__}>"
                
                # Return primitive types as-is
                return obj
            
            cleaned_packet = convert_to_serializable(packet)
            return json.dumps(cleaned_packet)
        except Exception as e:
            logger.warn(f"Error serializing packet: {e}")
            return "{}"
    
    def _update_topology(self, packet: Dict[str, Any], interface):
        """Update network topology based on packet"""
        try:
            source_id = packet.get('fromId')
            if not source_id:
                return

            # If packet has hop information, we can infer it came through network
            hop_start = packet.get('hopStart')
            hop_limit = packet.get('hopLimit')

            if hop_start and hop_limit:
                hops_away = hop_start - hop_limit

                # If hops_away > 0, packet was relayed
                # For now, we'll track source -> our node link
                # In future, we could try to infer intermediate hops

                my_node_id = interface.getMyNodeInfo().get('user', {}).get('id')
                if my_node_id and source_id != my_node_id:
                    # Update link from source to us
                    NodeTracking._db.update_topology(
                        source_id,
                        my_node_id,
                        snr=packet.get('rxSnr'),
                        rssi=packet.get('rxRssi'),
                        hop_count=hops_away
                    )

            # If via MQTT, track MQTT gateway as neighbor
            if packet.get('viaMqtt'):
                # Could track MQTT gateway here if we knew its node_id
                pass

        except Exception as e:
            logger.warn(f"Error updating topology: {e}")

    def _process_traceroute(self, packet: Dict[str, Any], interface):
        """Process traceroute packets to build detailed topology"""
        try:
            # Traceroute data can be in either 'trace' field (older format) or 'decoded.traceroute' (newer format)
            trace = packet.get('trace', {})
            if not trace:
                # Try the decoded.traceroute location
                decoded = packet.get('decoded', {})
                trace = decoded.get('traceroute', {})

            # Get the route from the traceroute packet (may be empty for incomplete/in-progress traceroutes)
            route = trace.get('route', [])

            # Store ALL traceroutes, even incomplete ones
            if route and len(route) >= 2:
                logger.infogreen(f"Processing complete traceroute with {len(route)} hops")
            else:
                logger.info(f"Processing incomplete/partial traceroute (route: {route})")

            # Convert node numbers to node IDs (only if we have a route)
            route_ids = []
            if route:
                for node_num in route:
                    # Try to find the node ID from the interface's node database
                    node_info = None
                    if hasattr(interface, 'nodes') and interface.nodes:
                        node_info = interface.nodes.get(node_num)

                    if node_info:
                        node_id = node_info.get('user', {}).get('id')
                        if node_id:
                            route_ids.append(node_id)
                        else:
                            # Fallback: construct ID from node number
                            route_ids.append(f"!{node_num:08x}")
                    else:
                        # Node not in database yet, construct ID
                        route_ids.append(f"!{node_num:08x}")

            # Get SNR data - use snrTowards which shows signal from each hop
            snr_towards = trace.get('snrTowards', [])

            # Process each hop in the route and update topology (only for complete routes)
            if len(route_ids) >= 2:
                for i in range(len(route_ids) - 1):
                    source_id = route_ids[i]
                    target_id = route_ids[i + 1]

                    # Get SNR for this hop if available
                    snr = snr_towards[i] if i < len(snr_towards) else None
                    rssi = None  # Not in trace data

                    # Update topology link for this hop
                    NodeTracking._db.update_topology(
                        source_id,
                        target_id,
                        snr=snr,
                        rssi=rssi,
                        hop_count=1  # Each link in traceroute is 1 hop
                    )

                    logger.info(f"  Traceroute hop {i+1}: {source_id} -> {target_id}")

            # Store ALL traceroutes in the database (complete or incomplete)
            from_node_id = packet.get('fromId')
            to_node_id = packet.get('toId')  # Destination, if available

            # Store the SNR towards data for the route
            snr_data = snr_towards if snr_towards else None

            traceroute_id = NodeTracking._db.insert_traceroute(
                from_node_id=from_node_id,
                to_node_id=to_node_id,
                route_ids=route_ids,  # May be empty for incomplete traceroutes
                snr_data=snr_data
            )

            # Mark any pending attempt to this node as completed
            # The traceroute response comes FROM the destination node
            if from_node_id:
                NodeTracking._db.complete_traceroute_attempt(from_node_id, traceroute_id)

            if route_ids:
                logger.infogreen(f"Traceroute stored: {len(route_ids)} nodes, {' -> '.join([rid[-4:] for rid in route_ids])}")
            else:
                logger.infogreen(f"Incomplete traceroute stored: from={from_node_id}, to={to_node_id}, snr={snr_data}")

        except Exception as e:
            logger.warn(f"Error processing traceroute: {e}")
            import traceback
            traceback.print_exc()

    def _process_telemetry_response(self, packet: Dict[str, Any], interface):
        """Process telemetry packets to check if they're responses to our requests"""
        try:
            from_node_id = packet.get('fromId')
            if not from_node_id:
                return

            # Extract signal quality metadata
            rx_snr = packet.get('rxSnr')
            rx_rssi = packet.get('rxRssi')

            # Calculate hops away
            hop_start = packet.get('hopStart')
            hop_limit = packet.get('hopLimit')
            hops_away = None
            if hop_start is not None and hop_limit is not None:
                hops_away = hop_start - hop_limit

            # Try to match relay node if packet was relayed
            relay_node_id = None
            relay_node_name = None
            relay_node_partial = packet.get('relayNode')
            if relay_node_partial is not None and hops_away and hops_away > 0:
                matched_relay = self._match_relay_node(relay_node_partial, interface, from_node_id)
                if matched_relay:
                    matched_id = matched_relay.get('id')
                    if isinstance(matched_id, str) and matched_id.startswith('!'):
                        relay_node_id = matched_id
                        relay_node_name = matched_relay.get('name')

            # Try to mark any pending telemetry request from this node as completed
            completed = NodeTracking._db.complete_telemetry_request(
                from_node_id=from_node_id,
                rx_snr=rx_snr,
                rx_rssi=rx_rssi,
                relay_node_id=relay_node_id,
                relay_node_name=relay_node_name,
                hops_away=hops_away
            )

            if completed:
                node_name = from_node_id
                # Try to get node name from interface
                node_info = None
                node_num = packet.get('from')
                if hasattr(interface, 'nodes') and interface.nodes and node_num:
                    node_info = interface.nodes.get(node_num)
                if node_info:
                    user = node_info.get('user', {})
                    node_name = user.get('longName') or user.get('shortName') or from_node_id

                relay_info = f" via {relay_node_name}" if relay_node_name else ""
                hops_info = f", {hops_away} hop(s)" if hops_away else ""
                logger.infogreen(f"Telemetry response received from {node_name}{relay_info}{hops_info} - SNR: {rx_snr}, RSSI: {rx_rssi}")

        except Exception as e:
            logger.warn(f"Error processing telemetry response: {e}")

    def _export_data(self):
        """Export data to JSON"""
        try:
            if NodeTracking._exporter:
                json_path = NodeTracking._config.get('json_export_path', './nodes.json')
                NodeTracking._exporter.export_nodes_to_json(json_path, include_topology=True)
        except Exception as e:
            logger.warn(f"Error exporting data: {e}")
    
    def _start_topology_cleanup(self):
        """Start periodic topology cleanup timer"""
        def cleanup():
            try:
                timeout_minutes = NodeTracking._config.get('topology', {}).get('link_timeout_minutes', 60)
                NodeTracking._db.mark_inactive_links(timeout_minutes)
            except Exception as e:
                logger.warn(f"Error in topology cleanup: {e}")
            
            # Schedule next cleanup
            if NodeTracking._config.get('enabled', True):
                NodeTracking._topology_cleanup_timer = threading.Timer(300, cleanup)  # Every 5 minutes
                NodeTracking._topology_cleanup_timer.daemon = True
                NodeTracking._topology_cleanup_timer.start()
        
        # Start initial cleanup
        cleanup()

    def _start_auto_traceroute(self):
        """Start periodic auto-traceroute timer"""
        auto_tr_config = NodeTracking._config.get('auto_traceroute', {})
        interval_minutes = auto_tr_config.get('interval_minutes', 30)

        def run_cycle():
            try:
                self._perform_auto_traceroute_cycle()
            except Exception as e:
                logger.warn(f"Error in auto-traceroute cycle: {e}")

            # Schedule next cycle if still enabled
            if NodeTracking._config.get('enabled', True) and auto_tr_config.get('enabled', False):
                NodeTracking._auto_traceroute_timer = threading.Timer(interval_minutes * 60, run_cycle)
                NodeTracking._auto_traceroute_timer.daemon = True
                NodeTracking._auto_traceroute_timer.start()

        logger.infogreen(f"Auto-traceroute enabled: checking every {interval_minutes} minutes")

        # Start first cycle after the interval (not immediately on connect)
        NodeTracking._auto_traceroute_timer = threading.Timer(interval_minutes * 60, run_cycle)
        NodeTracking._auto_traceroute_timer.daemon = True
        NodeTracking._auto_traceroute_timer.start()

    def _perform_auto_traceroute_cycle(self):
        """Query DB for nodes needing traceroutes and send them"""
        if NodeTracking._traceroute_in_progress:
            logger.info("Auto-traceroute: cycle already in progress, skipping")
            return

        if not NodeTracking._interface:
            logger.warn("Auto-traceroute: no interface available")
            return

        NodeTracking._traceroute_in_progress = True

        try:
            # Timeout any stale pending attempts (older than 2 minutes)
            timed_out = NodeTracking._db.timeout_stale_attempts(timeout_seconds=120)
            if timed_out > 0:
                logger.info(f"Auto-traceroute: marked {timed_out} stale attempts as timed out")

            auto_tr_config = NodeTracking._config.get('auto_traceroute', {})

            # Get config values
            active_threshold = auto_tr_config.get('active_threshold_minutes', 60)
            traceroute_age = auto_tr_config.get('traceroute_age_hours', 4)
            max_per_cycle = auto_tr_config.get('max_per_cycle', 5)
            delay_seconds = auto_tr_config.get('delay_seconds', 10)
            exclude_mqtt = auto_tr_config.get('exclude_mqtt_nodes', True)
            hop_limit = auto_tr_config.get('hop_limit', 7)

            # Query nodes needing traceroutes
            nodes = NodeTracking._db.get_nodes_needing_traceroute(
                active_threshold_minutes=active_threshold,
                traceroute_age_hours=traceroute_age,
                exclude_mqtt=exclude_mqtt,
                limit=max_per_cycle
            )

            if not nodes:
                logger.info("Auto-traceroute: no nodes need traceroutes at this time")
                return

            logger.infogreen(f"Auto-traceroute: sending traceroutes to {len(nodes)} nodes")

            # Import meshtastic modules for traceroute
            from meshtastic import mesh_pb2, portnums_pb2

            for i, node in enumerate(nodes):
                try:
                    node_num = node['node_num']
                    node_name = node.get('long_name') or node.get('short_name') or node['node_id']
                    last_tr = node.get('last_traceroute_utc') or 'never'

                    logger.info(f"Auto-traceroute: sending to {node_name} ({node['node_id']}) - last traceroute: {last_tr}")

                    # Create RouteDiscovery request
                    r = mesh_pb2.RouteDiscovery()

                    # Send traceroute request (non-blocking)
                    NodeTracking._interface.sendData(
                        r,
                        destinationId=node_num,
                        portNum=portnums_pb2.PortNum.TRACEROUTE_APP,
                        wantResponse=True,
                        hopLimit=hop_limit
                    )

                    # Log the attempt
                    NodeTracking._db.insert_traceroute_attempt(
                        to_node_id=node['node_id'],
                        to_node_name=node_name
                    )

                    # Delay between traceroutes (except for the last one)
                    if i < len(nodes) - 1 and delay_seconds > 0:
                        time.sleep(delay_seconds)

                except Exception as e:
                    logger.warn(f"Auto-traceroute: failed to send to {node.get('node_id')}: {e}")

            logger.infogreen(f"Auto-traceroute: cycle complete, sent {len(nodes)} traceroutes")

        except Exception as e:
            logger.warn(f"Auto-traceroute: cycle error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            NodeTracking._traceroute_in_progress = False

    def _start_auto_telemetry(self):
        """Start periodic auto-telemetry request timer"""
        auto_tel_config = NodeTracking._config.get('auto_telemetry', {})
        interval_minutes = auto_tel_config.get('interval_minutes', 15)

        def run_cycle():
            try:
                self._perform_auto_telemetry_cycle()
            except Exception as e:
                logger.warn(f"Error in auto-telemetry cycle: {e}")

            # Schedule next cycle if still enabled
            if NodeTracking._config.get('enabled', True) and auto_tel_config.get('enabled', False):
                NodeTracking._auto_telemetry_timer = threading.Timer(interval_minutes * 60, run_cycle)
                NodeTracking._auto_telemetry_timer.daemon = True
                NodeTracking._auto_telemetry_timer.start()

        logger.infogreen(f"Auto-telemetry enabled: checking every {interval_minutes} minutes")

        # Start first cycle after the interval (not immediately on connect)
        NodeTracking._auto_telemetry_timer = threading.Timer(interval_minutes * 60, run_cycle)
        NodeTracking._auto_telemetry_timer.daemon = True
        NodeTracking._auto_telemetry_timer.start()

    def _perform_auto_telemetry_cycle(self):
        """Query DB for nodes needing telemetry requests and send them"""
        if NodeTracking._telemetry_request_in_progress:
            logger.info("Auto-telemetry: cycle already in progress, skipping")
            return

        if not NodeTracking._interface:
            logger.warn("Auto-telemetry: no interface available")
            return

        NodeTracking._telemetry_request_in_progress = True

        try:
            # Timeout any stale pending requests (older than 2 minutes)
            timed_out = NodeTracking._db.timeout_stale_telemetry_requests(timeout_seconds=120)
            if timed_out > 0:
                logger.info(f"Auto-telemetry: marked {timed_out} stale requests as timed out")

            auto_tel_config = NodeTracking._config.get('auto_telemetry', {})
            auto_tr_config = NodeTracking._config.get('auto_traceroute', {})

            # Get config values
            active_threshold = auto_tel_config.get('active_threshold_minutes', 120)
            request_age = auto_tel_config.get('request_age_hours', 2)
            max_per_cycle = auto_tel_config.get('max_per_cycle', 10)
            delay_seconds = auto_tel_config.get('delay_seconds', 5)
            exclude_mqtt = auto_tel_config.get('exclude_mqtt_nodes', True)
            skip_recent_traceroutes = auto_tel_config.get('skip_nodes_with_recent_traceroute', True)
            traceroute_age = auto_tr_config.get('traceroute_age_hours', 4)

            # Query nodes needing telemetry requests
            nodes = NodeTracking._db.get_nodes_needing_telemetry_request(
                active_threshold_minutes=active_threshold,
                request_age_hours=request_age,
                exclude_mqtt=exclude_mqtt,
                skip_recent_traceroutes=skip_recent_traceroutes,
                traceroute_age_hours=traceroute_age,
                limit=max_per_cycle
            )

            if not nodes:
                logger.info("Auto-telemetry: no nodes need telemetry requests at this time")
                return

            logger.infogreen(f"Auto-telemetry: sending requests to {len(nodes)} nodes")

            for i, node in enumerate(nodes):
                try:
                    node_num = node['node_num']
                    node_id = node['node_id']
                    node_name = node.get('long_name') or node.get('short_name') or node_id

                    logger.info(f"Auto-telemetry: requesting from {node_name} ({node_id})")

                    # Send telemetry request using sendTelemetry with wantResponse
                    # This sends a request for device metrics
                    NodeTracking._interface.sendTelemetry(
                        destinationId=node_num,
                        wantResponse=True
                    )

                    # Log the attempt
                    NodeTracking._db.insert_telemetry_request(
                        to_node_id=node_id,
                        to_node_name=node_name
                    )

                    # Delay between requests (except for the last one)
                    if i < len(nodes) - 1 and delay_seconds > 0:
                        time.sleep(delay_seconds)

                except Exception as e:
                    logger.warn(f"Auto-telemetry: failed to send to {node.get('node_id')}: {e}")

            logger.infogreen(f"Auto-telemetry: cycle complete, sent {len(nodes)} requests")

        except Exception as e:
            logger.warn(f"Auto-telemetry: cycle error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            NodeTracking._telemetry_request_in_progress = False

    def onConnect(self, interface, client):
        """Handle connection to node"""
        if not NodeTracking._config or not NodeTracking._config.get('enabled', True):
            return

        # Store interface for auto-traceroute
        NodeTracking._interface = interface

        logger.info("Node tracking ready - will capture all packets")

        # Start auto-traceroute if enabled
        auto_tr_config = NodeTracking._config.get('auto_traceroute', {})
        if auto_tr_config.get('enabled', False):
            self._start_auto_traceroute()

        # Start auto-telemetry if enabled
        auto_tel_config = NodeTracking._config.get('auto_telemetry', {})
        if auto_tel_config.get('enabled', False):
            self._start_auto_telemetry()

        # Do initial export
        self._export_data()
    
    def onDisconnect(self, interface, client):
        """Handle disconnection from node"""
        if not NodeTracking._config or not NodeTracking._config.get('enabled', True):
            return

        # Export data before shutdown
        self._export_data()

        # Cancel cleanup timer
        if NodeTracking._topology_cleanup_timer:
            NodeTracking._topology_cleanup_timer.cancel()

        # Cancel auto-traceroute timer
        if NodeTracking._auto_traceroute_timer:
            NodeTracking._auto_traceroute_timer.cancel()
            NodeTracking._auto_traceroute_timer = None

        # Cancel auto-telemetry timer
        if NodeTracking._auto_telemetry_timer:
            NodeTracking._auto_telemetry_timer.cancel()
            NodeTracking._auto_telemetry_timer = None

        # Clear interface reference
        NodeTracking._interface = None