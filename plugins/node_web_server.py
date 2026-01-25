"""
Node Tracking Web Server
Provides REST API and web interface for viewing node tracking data.
"""

import plugins
import cfg
import json
import os
from datetime import datetime
import plugins.liblogger as logger
from plugins.libnode_db import NodeDatabase
from plugins.libnode_export import NodeExporter
import threading

class NodeWebServer(plugins.Base):
    """Web server for node tracking visualization"""
    
    def __init__(self):
        self.server_thread = None
        self.app = None
        self.db = None
        self.exporter = None
        self.config = None
    
    def start(self):
        """Initialize and start the web server"""
        logger.info("Loading node tracking web server")
        
        # Get config
        node_config = cfg.config.get('node_tracking', {})
        if not node_config.get('enabled', False):
            logger.info("Node tracking disabled, skipping web server")
            return
        
        web_config = node_config.get('web_server', {})
        if not web_config.get('enabled', False):
            logger.info("Node tracking web server disabled")
            return
        
        self.config = web_config
        
        # Try to import Flask
        try:
            from flask import Flask, jsonify, send_from_directory, request
            from flask_cors import CORS
        except ImportError:
            logger.warn("Flask not installed. Install with: pip install flask flask-cors")
            logger.info("Web server disabled - Flask required")
            return
        
        # Initialize database connection
        try:
            db_path = node_config.get('database_path', './nodes.db')
            self.db = NodeDatabase(db_path)
            self.exporter = NodeExporter(self.db)
        except Exception as e:
            logger.warn(f"Failed to connect to node database: {e}")
            return
        
        # Get absolute path to web directory
        # Working directory is MeshLinkBeta/, so web/ is directly accessible
        web_dir = os.path.join(os.getcwd(), 'web')
        
        # Create Flask app
        self.app = Flask(__name__, static_folder=web_dir, static_url_path='')
        CORS(self.app)  # Enable CORS for API access
        
        # Define routes
        @self.app.route('/api/site-config')
        def get_site_config():
            """Get site configuration for web pages"""
            bot_name = cfg.config.get("bot_name", "MeshLink")
            return jsonify({
                'success': True,
                'botName': bot_name
            })

        @self.app.route('/')
        def index():
            """Serve main page"""
            try:
                return send_from_directory(web_dir, 'nodes.html')
            except Exception as e:
                logger.warn(f"Failed to serve nodes.html: {e}")
                return "<h1>Node Tracking</h1><p>Web interface not yet available. Use API endpoints.</p>"

        @self.app.route('/map.html')
        def map_page():
            """Serve network map page"""
            try:
                return send_from_directory(web_dir, 'map.html')
            except Exception as e:
                logger.warn(f"Error serving map.html: {e}")
                return "Map page not found", 404

        @self.app.route('/telemetry.html')
        def telemetry_page():
            """Serve telemetry requests page"""
            try:
                return send_from_directory(web_dir, 'telemetry.html')
            except Exception as e:
                logger.warn(f"Failed to serve telemetry.html: {e}")
                return "<h1>Telemetry</h1><p>Telemetry page not available.</p>"
        
        @self.app.route('/api/nodes', methods=['GET'])
        def get_nodes():
            """Get all nodes"""
            try:
                nodes = self.db.get_all_nodes()
                return jsonify({
                    'success': True,
                    'count': len(nodes),
                    'nodes': nodes
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/nodes/<node_id>', methods=['GET'])
        def get_node(node_id):
            """Get specific node"""
            try:
                node = self.db.get_node(node_id)
                if node:
                    return jsonify({
                        'success': True,
                        'node': node
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': 'Node not found'
                    }), 404
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/nodes/<node_id>/packets', methods=['GET'])
        def get_node_packets(node_id):
            """Get packet history for node"""
            try:
                limit = int(request.args.get('limit', 100))
                packets = self.db.get_node_packets(node_id, limit)
                return jsonify({
                    'success': True,
                    'count': len(packets),
                    'packets': packets
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/nodes/<node_id>/neighbors', methods=['GET'])
        def get_node_neighbors(node_id):
            """Get neighbors for node"""
            try:
                neighbors = self.db.get_neighbors(node_id)
                return jsonify({
                    'success': True,
                    'count': len(neighbors),
                    'neighbors': neighbors
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/topology', methods=['GET'])
        def get_topology():
            """Get network topology"""
            try:
                active_only = request.args.get('active_only', 'true').lower() == 'true'
                topology = self.db.get_topology(active_only)
                return jsonify({
                    'success': True,
                    'count': len(topology),
                    'links': topology
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/topology/graph', methods=['GET'])
        def get_topology_graph():
            """Get topology in graph format"""
            try:
                nodes = self.db.get_all_nodes()
                topology = self.db.get_topology(active_only=True)
                
                # Format for graph visualization
                graph = {
                    'nodes': [
                        {
                            'id': node['node_id'],
                            'label': node['long_name'] or node['short_name'] or node['node_id'],
                            'battery': node['battery_level'],
                            'lastSeen': node['last_seen_utc']
                        }
                        for node in nodes
                    ],
                    'edges': [
                        {
                            'source': link['source_node_id'],
                            'target': link['neighbor_node_id'],
                            'quality': link['link_quality_score'],
                            'snr': link['avg_snr'],
                            'rssi': link['avg_rssi']
                        }
                        for link in topology
                    ]
                }
                
                return jsonify({
                    'success': True,
                    'graph': graph
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/topology/hop-graph', methods=['GET'])
        def get_hop_topology():
            """Get topology organized by hop distance from local node"""
            try:
                # Get all nodes and their recent packets
                nodes = self.db.get_all_nodes()

                # Build hop-based graph
                graph_nodes = []
                graph_edges = []
                direct_nodes = []

                # Add a virtual "Self" node representing the local device
                graph_nodes.append({
                    'id': 'LOCAL_NODE',
                    'label': 'Self (This Device)',
                    'short_name': 'Self',
                    'long_name': 'Self (This Device)',
                    'hops': -1,  # Special marker for local node
                    'battery': None,
                    'lastSeen': None,
                    'relay_via': None
                })

                # Process each node
                for node in nodes:
                    node_id = node['node_id']

                    # Get recent packets from this node to determine hop count
                    packets = self.db.get_node_packets(node_id, limit=20)

                    # Determine minimum hop count and relay node
                    min_hops = None
                    relay_via = None

                    for pkt in packets:
                        hops = pkt.get('hops_away')
                        if hops is not None:
                            # Track minimum hop count
                            if min_hops is None or hops < min_hops:
                                min_hops = hops

                            # Get relay_via from most recent packet with hops > 0
                            # (not from the minimum hop packet, since that might be direct)
                            if hops > 0 and relay_via is None:
                                relay_via = pkt.get('relay_node_id')

                    # Add node to graph
                    graph_nodes.append({
                        'id': node_id,
                        'label': node.get('long_name') or node.get('short_name') or node_id,
                        'short_name': node.get('short_name'),
                        'long_name': node.get('long_name'),
                        'hops': min_hops if min_hops is not None else 99,
                        'battery': node.get('battery_level'),
                        'lastSeen': node.get('last_seen_utc'),
                        'relay_via': relay_via
                    })

                    # Create edges based on hop count
                    if min_hops == 0:
                        # Direct connection to local node
                        direct_nodes.append(node_id)
                        graph_edges.append({
                            'from': 'LOCAL_NODE',
                            'to': node_id,
                            'hops': 0
                        })
                    elif relay_via and min_hops and min_hops > 0:
                        # Relayed through another node
                        # Only add edge if relay_via is a valid full node ID (starts with !)
                        if isinstance(relay_via, str) and relay_via.startswith('!'):
                            graph_edges.append({
                                'from': relay_via,
                                'to': node_id,
                                'hops': min_hops
                            })
                        else:
                            # Skip invalid relay IDs (partial IDs from relay matching)
                            pass

                return jsonify({
                    'success': True,
                    'nodes': graph_nodes,
                    'edges': graph_edges
                })
            except Exception as e:
                logger.warn(f"Error getting hop topology: {e}")
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/stats', methods=['GET'])
        def get_stats():
            """Get network statistics"""
            try:
                stats = self.db.get_statistics()
                return jsonify({
                    'success': True,
                    'statistics': stats
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/export/json', methods=['GET'])
        def export_json():
            """Export full data as JSON"""
            try:
                # Create temporary export
                import tempfile
                import os
                
                with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
                    temp_path = f.name
                
                self.exporter.export_nodes_to_json(temp_path, include_packets=True, include_topology=True)
                
                with open(temp_path, 'r') as f:
                    data = json.load(f)
                
                os.unlink(temp_path)
                
                return jsonify(data)
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500
        
        @self.app.route('/api/export/geojson', methods=['GET'])
        def export_geojson():
            """Export nodes as GeoJSON"""
            try:
                geojson = self.exporter.get_nodes_geojson()
                return jsonify(geojson)
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/traceroutes', methods=['GET'])
        def get_traceroutes():
            """Get all traceroutes"""
            try:
                limit = int(request.args.get('limit', 100))
                traceroutes = self.db.get_all_traceroutes(limit)
                return jsonify({
                    'success': True,
                    'count': len(traceroutes),
                    'traceroutes': traceroutes
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/traceroutes/<int:traceroute_id>', methods=['GET'])
        def get_traceroute(traceroute_id):
            """Get specific traceroute"""
            try:
                traceroute = self.db.get_traceroute(traceroute_id)
                if traceroute:
                    return jsonify({
                        'success': True,
                        'traceroute': traceroute
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': 'Traceroute not found'
                    }), 404
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/nodes/<node_id>/traceroutes', methods=['GET'])
        def get_node_traceroutes(node_id):
            """Get traceroutes involving a specific node"""
            try:
                limit = int(request.args.get('limit', 50))
                traceroutes = self.db.get_traceroutes_by_node(node_id, limit)
                return jsonify({
                    'success': True,
                    'count': len(traceroutes),
                    'traceroutes': traceroutes
                })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/map-data', methods=['GET'])
        def get_map_data():
            """Get connection data for coverage map based on connection-logic.md specs"""
            try:
                from datetime import datetime, timedelta

                # Get time window from query params (default 24 hours)
                hours = int(request.args.get('hours', 24))
                time_cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()

                # Get all nodes with GPS
                all_nodes = self.db.get_all_nodes()
                nodes_with_gps = []
                node_lookup = {}

                for node in all_nodes:
                    if node.get('latitude') and node.get('longitude'):
                        last_seen = node.get('last_seen_utc', '')
                        if last_seen and last_seen < time_cutoff:
                            continue

                        node_data = {
                            'id': node['node_id'],
                            'name': node.get('long_name') or node.get('short_name') or node['node_id'],
                            'shortName': node.get('short_name') or node['node_id'][-4:],
                            'position': {
                                'lat': node['latitude'],
                                'lon': node['longitude'],
                                'alt': node.get('altitude')
                            },
                            'battery': node.get('battery_level'),
                            'hwModel': node.get('hardware_model'),
                            'lastHeard': node.get('last_seen_utc'),
                            'totalPackets': node.get('total_packets_received', 0),
                            'isMqtt': node.get('is_mqtt', False),
                            'directLinkCount': 0
                        }
                        nodes_with_gps.append(node_data)
                        node_lookup[node['node_id']] = node_data

                # Track direct connections (deduplicated)
                # Key: tuple(sorted([node1, node2])) -> connection data
                direct_connections_map = {}

                # Track indirect coverage per RELAY node
                # Key: relay_node_id -> set of sending_node_ids
                # The relay node is the center, sending nodes define the edges
                indirect_coverage_map = {}

                # Get my node ID for local node reference
                my_node_id = None
                conn = self.db._get_connection()
                cursor = conn.cursor()

                # Source 1: Packets with relay node data
                # Direct connection if hops_away < 2 (relay -> local node)
                # Indirect connection if hops_away >= 2 (source -> relay is indirect)
                cursor.execute("""
                    SELECT DISTINCT
                        node_id, relay_node_id, relay_node_name,
                        hops_away, rx_snr, rx_rssi, received_at_utc
                    FROM packet_history
                    WHERE relay_node_id IS NOT NULL
                      AND relay_node_id LIKE '!%'
                      AND received_at_utc >= ?
                    ORDER BY received_at_utc DESC
                """, (time_cutoff,))

                for row in cursor.fetchall():
                    source_id = row['node_id']
                    relay_id = row['relay_node_id']
                    hops_away = row['hops_away'] or 0

                    if hops_away < 2:
                        # Direct connection: relay_node -> local_node (we received via this relay)
                        # This means relay can hear source directly
                        if relay_id in node_lookup:
                            # We don't know local node ID here, but we know relay heard source
                            # Actually per spec: "there is a direct connection" from relay to local
                            # For map purposes, we'll track relay <-> source as a link
                            key = tuple(sorted([source_id, relay_id]))
                            if key not in direct_connections_map and source_id in node_lookup:
                                direct_connections_map[key] = {
                                    'from': source_id,
                                    'to': relay_id,
                                    'rssi': row['rx_rssi'],
                                    'snr': row['rx_snr'],
                                    'source': 'relay-packet',
                                    'packetCount': 1
                                }
                            elif key in direct_connections_map:
                                direct_connections_map[key]['packetCount'] += 1
                    else:
                        # Indirect connection: relay can reach source indirectly (2+ hops)
                        # Center shape on RELAY, edge defined by SENDING node
                        if source_id in node_lookup and relay_id in node_lookup:
                            if relay_id not in indirect_coverage_map:
                                indirect_coverage_map[relay_id] = set()
                            indirect_coverage_map[relay_id].add(source_id)

                # Source 2: Traceroute data (both out and back paths show direct links)
                traceroutes = self.db.get_all_traceroutes(limit=200)
                for trace in traceroutes:
                    trace_time = trace.get('received_at_utc', '')
                    if trace_time and trace_time < time_cutoff:
                        continue

                    route = trace.get('route', [])
                    snr_data = trace.get('snr_data') or []

                    # Each consecutive pair in route is a direct link
                    for i in range(len(route) - 1):
                        from_id = route[i]
                        to_id = route[i + 1]

                        if from_id in node_lookup and to_id in node_lookup:
                            key = tuple(sorted([from_id, to_id]))
                            snr = snr_data[i] if i < len(snr_data) else None

                            if key not in direct_connections_map:
                                direct_connections_map[key] = {
                                    'from': from_id,
                                    'to': to_id,
                                    'rssi': None,
                                    'snr': snr,
                                    'source': 'traceroute',
                                    'packetCount': 1
                                }
                            else:
                                # Update with traceroute SNR if we don't have signal data
                                if direct_connections_map[key]['snr'] is None and snr is not None:
                                    direct_connections_map[key]['snr'] = snr
                                if direct_connections_map[key]['source'] == 'relay-packet':
                                    direct_connections_map[key]['source'] = 'relay+traceroute'
                                direct_connections_map[key]['packetCount'] += 1

                # Source 3: Telemetry responses with relay data (hop count < 2)
                cursor.execute("""
                    SELECT to_node_id, relay_node_id, relay_node_name,
                           hops_away, rx_snr, rx_rssi, completed_at_utc
                    FROM telemetry_requests
                    WHERE status = 'completed'
                      AND relay_node_id IS NOT NULL
                      AND relay_node_id LIKE '!%'
                      AND completed_at_utc >= ?
                """, (time_cutoff,))

                for row in cursor.fetchall():
                    source_id = row['to_node_id']  # The node that responded
                    relay_id = row['relay_node_id']
                    hops_away = row['hops_away'] or 0

                    if hops_away < 2:
                        # Direct connection between source and relay
                        if source_id in node_lookup and relay_id in node_lookup:
                            key = tuple(sorted([source_id, relay_id]))
                            if key not in direct_connections_map:
                                direct_connections_map[key] = {
                                    'from': source_id,
                                    'to': relay_id,
                                    'rssi': row['rx_rssi'],
                                    'snr': row['rx_snr'],
                                    'source': 'telemetry',
                                    'packetCount': 1
                                }
                            else:
                                direct_connections_map[key]['packetCount'] += 1
                    else:
                        # Indirect connection: relay can reach source indirectly (2+ hops)
                        # Center shape on RELAY, edge defined by SENDING node
                        if source_id in node_lookup and relay_id in node_lookup:
                            if relay_id not in indirect_coverage_map:
                                indirect_coverage_map[relay_id] = set()
                            indirect_coverage_map[relay_id].add(source_id)

                # Convert to lists for JSON response
                direct_connections = list(direct_connections_map.values())

                # Update direct link counts on nodes
                for conn in direct_connections:
                    if conn['from'] in node_lookup:
                        node_lookup[conn['from']]['directLinkCount'] += 1
                    if conn['to'] in node_lookup:
                        node_lookup[conn['to']]['directLinkCount'] += 1

                # Convert indirect coverage map to list
                # Each entry: relay node (center) with list of sending nodes (edges)
                indirect_coverage = []
                for relay_id, sending_ids in indirect_coverage_map.items():
                    if len(sending_ids) > 0:
                        indirect_coverage.append({
                            'relayNodeId': relay_id,
                            'sendingNodeIds': list(sending_ids)
                        })

                # Stats
                stats = {
                    'totalNodes': len(nodes_with_gps),
                    'directConnections': len(direct_connections),
                    'indirectCoverage': len(indirect_coverage)
                }

                return jsonify({
                    'success': True,
                    'nodes': nodes_with_gps,
                    'directConnections': direct_connections,
                    'indirectCoverage': indirect_coverage,
                    'stats': stats
                })

            except Exception as e:
                logger.warn(f"Error getting map data: {e}")
                import traceback
                traceback.print_exc()
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/telemetry-requests', methods=['GET'])
        def get_telemetry_requests():
            """Get telemetry requests with optional filtering"""
            try:
                limit = int(request.args.get('limit', 100))
                status = request.args.get('status')

                # Get requests
                if status and status != 'all':
                    requests = self.db.get_telemetry_requests(limit=limit, status=status)
                else:
                    requests = self.db.get_telemetry_requests(limit=limit)

                # Get stats
                stats = {
                    'total': 0,
                    'completed': 0,
                    'pending': 0,
                    'timeout': 0
                }

                # Count by status
                all_requests = self.db.get_telemetry_requests(limit=10000)
                for req in all_requests:
                    stats['total'] += 1
                    if req['status'] == 'completed':
                        stats['completed'] += 1
                    elif req['status'] == 'pending':
                        stats['pending'] += 1
                    elif req['status'] == 'timeout':
                        stats['timeout'] += 1

                return jsonify({
                    'success': True,
                    'requests': requests,
                    'stats': stats
                })

            except Exception as e:
                logger.warn(f"Error getting telemetry requests: {e}")
                import traceback
                traceback.print_exc()
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        # Start server in background thread
        host = self.config.get('host', '0.0.0.0')
        port = self.config.get('port', 8080)
        
        def run_server():
            try:
                logger.infogreen(f"Node tracking web server starting on http://{host}:{port}")
                self.app.run(host=host, port=port, debug=False, use_reloader=False)
            except Exception as e:
                logger.warn(f"Web server error: {e}")
        
        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()
        
        logger.infogreen(f"Node tracking web interface available at http://{host}:{port}")