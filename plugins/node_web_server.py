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
                logger.warn(f"Failed to serve map.html: {e}")
                return "<h1>Network Map</h1><p>Map interface not yet available.</p>"
        
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
            """Get combined data for map visualization including nodes, connections, and traceroutes"""
            try:
                from datetime import datetime, timedelta

                # Get time window from query params (default 24 hours)
                hours = int(request.args.get('hours', 24))
                time_cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()

                # Get all nodes
                all_nodes = self.db.get_all_nodes()

                # Filter to nodes with GPS coordinates
                nodes_with_gps = []
                node_lookup = {}  # For quick lookup by node_id

                for node in all_nodes:
                    if node.get('latitude') and node.get('longitude'):
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
                            'isMqtt': node.get('is_mqtt', False)
                        }
                        nodes_with_gps.append(node_data)
                        node_lookup[node['node_id']] = node_data

                # Get topology connections
                topology = self.db.get_topology(active_only=False)
                connections = []

                for link in topology:
                    source_id = link['source_node_id']
                    target_id = link['neighbor_node_id']

                    # Only include connections where both nodes have GPS
                    if source_id in node_lookup and target_id in node_lookup:
                        connections.append({
                            'from': source_id,
                            'to': target_id,
                            'rssi': link.get('avg_rssi'),
                            'snr': link.get('avg_snr'),
                            'quality': link.get('link_quality_score'),
                            'packets': link.get('total_packets', 0),
                            'lastHeard': link.get('last_heard_utc'),
                            'isActive': link.get('is_active', False),
                            'hopCount': link.get('last_hop_count', 1),
                            'bidirectional': False,  # Will be calculated below
                            'isDirect': link.get('last_hop_count', 1) == 1
                        })

                # Detect bidirectional connections
                connection_set = set()
                for conn in connections:
                    key = tuple(sorted([conn['from'], conn['to']]))
                    if key in connection_set:
                        # Mark both directions as bidirectional
                        for c in connections:
                            if tuple(sorted([c['from'], c['to']])) == key:
                                c['bidirectional'] = True
                    connection_set.add(key)

                # Get traceroutes and add those connections too
                traceroutes = self.db.get_all_traceroutes(limit=100)
                traceroute_connections = []

                for trace in traceroutes:
                    route = trace.get('route', [])
                    snr_data = trace.get('snr_data') or []

                    for i in range(len(route) - 1):
                        from_id = route[i]
                        to_id = route[i + 1]

                        # Only include if both nodes have GPS
                        if from_id in node_lookup and to_id in node_lookup:
                            snr = snr_data[i] if i < len(snr_data) else None
                            traceroute_connections.append({
                                'from': from_id,
                                'to': to_id,
                                'snr': snr,
                                'fromTraceroute': True,
                                'traceTime': trace.get('received_at_utc')
                            })

                # Calculate map center (average of all node positions)
                if nodes_with_gps:
                    avg_lat = sum(n['position']['lat'] for n in nodes_with_gps) / len(nodes_with_gps)
                    avg_lon = sum(n['position']['lon'] for n in nodes_with_gps) / len(nodes_with_gps)
                    map_center = [avg_lat, avg_lon]
                else:
                    map_center = [0, 0]

                # Stats
                stats = {
                    'totalNodes': len(all_nodes),
                    'nodesWithGps': len(nodes_with_gps),
                    'totalConnections': len(connections),
                    'bidirectionalConnections': len([c for c in connections if c['bidirectional']]),
                    'tracerouteConnections': len(traceroute_connections),
                    'mapCenter': map_center
                }

                return jsonify({
                    'success': True,
                    'nodes': nodes_with_gps,
                    'connections': connections,
                    'tracerouteConnections': traceroute_connections,
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