"""
Node Database Manager
Handles all database operations for node tracking, packet history, and network topology.
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any
import threading
import plugins.liblogger as logger

# Thread-local storage for database connections
_thread_local = threading.local()

# Airplane detection threshold: 1000 feet = 304.8 meters
AIRPLANE_ALTITUDE_THRESHOLD_METERS = 750

class NodeDatabase:
    """Manages SQLite database for node tracking"""
    
    def __init__(self, db_path: str = "./nodes.db"):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._initialize_database()
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection"""
        if not hasattr(_thread_local, 'connection'):
            _thread_local.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0
            )
            _thread_local.connection.row_factory = sqlite3.Row
        return _thread_local.connection
    
    def _initialize_database(self):
        """Create database tables if they don't exist"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Create nodes table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    node_num INTEGER,
                    short_name TEXT,
                    long_name TEXT,
                    latitude REAL,
                    longitude REAL,
                    altitude REAL,
                    last_seen_utc TEXT,
                    first_seen_utc TEXT,
                    total_packets_received INTEGER DEFAULT 0,
                    hardware_model TEXT,
                    firmware_version TEXT,
                    is_mqtt BOOLEAN DEFAULT 0,
                    battery_level INTEGER,
                    voltage REAL,
                    is_charging BOOLEAN,
                    is_powered BOOLEAN,
                    last_battery_update_utc TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    is_ignored BOOLEAN DEFAULT 0,
                    is_airplane BOOLEAN DEFAULT 0
                )
            """)
            
            # Create packet_history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS packet_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT NOT NULL,
                    received_at_utc TEXT NOT NULL,
                    packet_type TEXT,
                    channel_index INTEGER,
                    hop_start INTEGER,
                    hop_limit INTEGER,
                    hops_away INTEGER,
                    via_mqtt BOOLEAN DEFAULT 0,
                    relay_node_id TEXT,
                    relay_node_name TEXT,
                    rx_snr REAL,
                    rx_rssi INTEGER,
                    latitude REAL,
                    longitude REAL,
                    altitude REAL,
                    battery_level INTEGER,
                    voltage REAL,
                    is_charging BOOLEAN,
                    temperature REAL,
                    humidity REAL,
                    pressure REAL,
                    message_text TEXT,
                    raw_packet TEXT,
                    FOREIGN KEY (node_id) REFERENCES nodes(node_id)
                )
            """)
            
            # Create network_topology table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS network_topology (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_node_id TEXT NOT NULL,
                    neighbor_node_id TEXT NOT NULL,
                    first_heard_utc TEXT NOT NULL,
                    last_heard_utc TEXT NOT NULL,
                    total_packets INTEGER DEFAULT 0,
                    avg_snr REAL,
                    avg_rssi REAL,
                    min_snr REAL,
                    max_snr REAL,
                    min_rssi REAL,
                    max_rssi REAL,
                    link_quality_score REAL,
                    is_active BOOLEAN DEFAULT 1,
                    last_hop_count INTEGER,
                    UNIQUE(source_node_id, neighbor_node_id),
                    FOREIGN KEY (source_node_id) REFERENCES nodes(node_id),
                    FOREIGN KEY (neighbor_node_id) REFERENCES nodes(node_id)
                )
            """)
            
            # Create traceroutes table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS traceroutes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_node_id TEXT NOT NULL,
                    to_node_id TEXT,
                    route_json TEXT NOT NULL,
                    hop_count INTEGER NOT NULL,
                    received_at_utc TEXT NOT NULL,
                    snr_data TEXT,
                    packet_id INTEGER,
                    FOREIGN KEY (from_node_id) REFERENCES nodes(node_id),
                    FOREIGN KEY (to_node_id) REFERENCES nodes(node_id),
                    FOREIGN KEY (packet_id) REFERENCES packet_history(id)
                )
            """)

            # Create traceroute_attempts table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS traceroute_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    to_node_id TEXT NOT NULL,
                    to_node_name TEXT,
                    requested_at_utc TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    completed_at_utc TEXT,
                    traceroute_id INTEGER,
                    FOREIGN KEY (to_node_id) REFERENCES nodes(node_id),
                    FOREIGN KEY (traceroute_id) REFERENCES traceroutes(id)
                )
            """)

            # Create telemetry_requests table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS telemetry_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    to_node_id TEXT NOT NULL,
                    to_node_name TEXT,
                    requested_at_utc TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    completed_at_utc TEXT,
                    rx_snr REAL,
                    rx_rssi INTEGER,
                    relay_node_id TEXT,
                    relay_node_name TEXT,
                    hops_away INTEGER,
                    FOREIGN KEY (to_node_id) REFERENCES nodes(node_id)
                )
            """)

            # Create indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_packet_node ON packet_history(node_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_packet_time ON packet_history(received_at_utc)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_topology_source ON network_topology(source_node_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_topology_neighbor ON network_topology(neighbor_node_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_topology_active ON network_topology(is_active)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_traceroute_from ON traceroutes(from_node_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_traceroute_time ON traceroutes(received_at_utc)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_attempt_to_node ON traceroute_attempts(to_node_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_attempt_status ON traceroute_attempts(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_attempt_time ON traceroute_attempts(requested_at_utc)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_to_node ON telemetry_requests(to_node_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_status ON telemetry_requests(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_time ON telemetry_requests(requested_at_utc)")

            # Migration: Add is_ignored, is_airplane, and last_name_update_utc columns if they don't exist
            cursor.execute("PRAGMA table_info(nodes)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'is_ignored' not in columns:
                cursor.execute("ALTER TABLE nodes ADD COLUMN is_ignored BOOLEAN DEFAULT 0")
                logger.info("Added is_ignored column to nodes table")
            if 'is_airplane' not in columns:
                cursor.execute("ALTER TABLE nodes ADD COLUMN is_airplane BOOLEAN DEFAULT 0")
                logger.info("Added is_airplane column to nodes table")
            if 'last_name_update_utc' not in columns:
                cursor.execute("ALTER TABLE nodes ADD COLUMN last_name_update_utc TEXT")
                logger.info("Added last_name_update_utc column to nodes table")

            # Migration: Add message_text column to packet_history if it doesn't exist
            cursor.execute("PRAGMA table_info(packet_history)")
            packet_columns = [row[1] for row in cursor.fetchall()]
            if 'message_text' not in packet_columns:
                cursor.execute("ALTER TABLE packet_history ADD COLUMN message_text TEXT")
                logger.info("Added message_text column to packet_history table")

            conn.commit()
            logger.infogreen("Node tracking database initialized successfully")
            
        except Exception as e:
            logger.warn(f"Failed to initialize database: {e}")
            raise
    
    def upsert_node(self, node_data: Dict[str, Any]) -> bool:
        """Insert or update node information"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            now = datetime.utcnow().isoformat()
            node_id = node_data.get('node_id')
            
            # Check if node exists
            cursor.execute("SELECT node_id, first_seen_utc FROM nodes WHERE node_id = ?", (node_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing node
                update_fields = []
                update_values = []

                # Check if names should be updated (every 24 hours)
                cursor.execute(
                    "SELECT last_name_update_utc FROM nodes WHERE node_id = ?",
                    (node_id,),
                )
                name_row = cursor.fetchone()
                last_name_update = name_row["last_name_update_utc"] if name_row else None
                should_update_names = True
                if last_name_update:
                    try:
                        last_update_time = datetime.fromisoformat(last_name_update.replace("Z", "+00:00").replace("+00:00", ""))
                        hours_since_update = (datetime.utcnow() - last_update_time).total_seconds() / 3600
                        should_update_names = hours_since_update >= 24
                    except (ValueError, TypeError):
                        should_update_names = True

                # Fields that always update if present
                always_update_fields = ['node_num', 'latitude', 'longitude',
                             'altitude', 'hardware_model', 'firmware_version', 'is_mqtt',
                             'battery_level', 'voltage', 'is_charging', 'is_powered']

                for field in always_update_fields:
                    if field in node_data and node_data[field] is not None:
                        update_fields.append(f"{field} = ?")
                        update_values.append(node_data[field])

                # Names update only every 24 hours
                if should_update_names:
                    for field in ['short_name', 'long_name']:
                        if field in node_data and node_data[field] is not None:
                            update_fields.append(f"{field} = ?")
                            update_values.append(node_data[field])
                    update_fields.append("last_name_update_utc = ?")
                    update_values.append(now)

                # Airplane detection: update is_airplane based on altitude
                altitude = node_data.get('altitude')
                if altitude is not None:
                    is_airplane = 1 if altitude > AIRPLANE_ALTITUDE_THRESHOLD_METERS else 0
                    update_fields.append("is_airplane = ?")
                    update_values.append(is_airplane)
                
                # Always update these
                update_fields.extend(['last_seen_utc = ?', 'updated_at = ?', 
                                     'total_packets_received = total_packets_received + 1'])
                update_values.extend([now, now])
                
                # Update battery timestamp if battery data present
                if 'battery_level' in node_data and node_data['battery_level'] is not None:
                    update_fields.append('last_battery_update_utc = ?')
                    update_values.append(now)
                
                update_values.append(node_id)
                
                query = f"UPDATE nodes SET {', '.join(update_fields)} WHERE node_id = ?"
                cursor.execute(query, update_values)
                
            else:
                # Insert new node
                node_data['first_seen_utc'] = now
                node_data['last_seen_utc'] = now
                node_data['created_at'] = now
                node_data['updated_at'] = now
                node_data['total_packets_received'] = 1
                node_data['last_name_update_utc'] = now

                if 'battery_level' in node_data and node_data['battery_level'] is not None:
                    node_data['last_battery_update_utc'] = now

                fields = list(node_data.keys())
                placeholders = ','.join(['?' for _ in fields])
                query = f"INSERT INTO nodes ({','.join(fields)}) VALUES ({placeholders})"
                cursor.execute(query, [node_data[f] for f in fields])
                
                logger.infogreen(f"New node discovered: {node_id} ({node_data.get('long_name', 'Unknown')})")
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.warn(f"Failed to upsert node {node_id}: {e}")
            return False
    
    def insert_packet(self, packet_data: Dict[str, Any], max_packets_per_node: int = 1000) -> bool:
        """Insert packet history and cleanup old packets if needed"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Insert packet
            fields = list(packet_data.keys())
            placeholders = ','.join(['?' for _ in fields])
            query = f"INSERT INTO packet_history ({','.join(fields)}) VALUES ({placeholders})"
            cursor.execute(query, [packet_data[f] for f in fields])
            
            # Check packet count for this node
            node_id = packet_data['node_id']
            cursor.execute("SELECT COUNT(*) as count FROM packet_history WHERE node_id = ?", (node_id,))
            count = cursor.fetchone()['count']
            
            # Delete oldest packets if over limit
            if count > max_packets_per_node:
                delete_count = count - max_packets_per_node
                cursor.execute("""
                    DELETE FROM packet_history 
                    WHERE id IN (
                        SELECT id FROM packet_history 
                        WHERE node_id = ? 
                        ORDER BY received_at_utc ASC 
                        LIMIT ?
                    )
                """, (node_id, delete_count))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.warn(f"Failed to insert packet: {e}")
            return False
    
    def update_topology(self, source_id: str, neighbor_id: str, 
                       snr: Optional[float] = None, rssi: Optional[int] = None,
                       hop_count: Optional[int] = None) -> bool:
        """Update network topology information"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            now = datetime.utcnow().isoformat()
            
            # Check if link exists
            cursor.execute("""
                SELECT id, total_packets, avg_snr, avg_rssi, min_snr, max_snr, min_rssi, max_rssi
                FROM network_topology 
                WHERE source_node_id = ? AND neighbor_node_id = ?
            """, (source_id, neighbor_id))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing link
                total_packets = existing['total_packets'] + 1
                
                # Calculate running averages and min/max
                if snr is not None:
                    avg_snr = ((existing['avg_snr'] or 0) * existing['total_packets'] + snr) / total_packets
                    min_snr = min(existing['min_snr'] or snr, snr)
                    max_snr = max(existing['max_snr'] or snr, snr)
                else:
                    avg_snr = existing['avg_snr']
                    min_snr = existing['min_snr']
                    max_snr = existing['max_snr']
                
                if rssi is not None:
                    avg_rssi = ((existing['avg_rssi'] or 0) * existing['total_packets'] + rssi) / total_packets
                    min_rssi = min(existing['min_rssi'] or rssi, rssi)
                    max_rssi = max(existing['max_rssi'] or rssi, rssi)
                else:
                    avg_rssi = existing['avg_rssi']
                    min_rssi = existing['min_rssi']
                    max_rssi = existing['max_rssi']
                
                # Calculate link quality score
                quality = self._calculate_link_quality(avg_snr, avg_rssi, total_packets)
                
                cursor.execute("""
                    UPDATE network_topology SET
                        last_heard_utc = ?,
                        total_packets = ?,
                        avg_snr = ?,
                        avg_rssi = ?,
                        min_snr = ?,
                        max_snr = ?,
                        min_rssi = ?,
                        max_rssi = ?,
                        link_quality_score = ?,
                        is_active = 1,
                        last_hop_count = ?
                    WHERE source_node_id = ? AND neighbor_node_id = ?
                """, (now, total_packets, avg_snr, avg_rssi, min_snr, max_snr, 
                      min_rssi, max_rssi, quality, hop_count, source_id, neighbor_id))
            else:
                # Insert new link
                quality = self._calculate_link_quality(snr, rssi, 1)
                cursor.execute("""
                    INSERT INTO network_topology (
                        source_node_id, neighbor_node_id, first_heard_utc, last_heard_utc,
                        total_packets, avg_snr, avg_rssi, min_snr, max_snr, min_rssi, max_rssi,
                        link_quality_score, is_active, last_hop_count
                    ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """, (source_id, neighbor_id, now, now, snr, rssi, snr, snr, rssi, rssi, quality, hop_count))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.warn(f"Failed to update topology: {e}")
            return False
    
    def _calculate_link_quality(self, snr: Optional[float], rssi: Optional[int], 
                                packet_count: int) -> float:
        """Calculate link quality score (0-100)"""
        score = 0.0
        
        # SNR component (40%)
        if snr is not None:
            # SNR typically ranges from -20 to +20 dB
            # Map to 0-100 scale
            snr_normalized = min(100, max(0, (snr + 20) * 2.5))
            score += snr_normalized * 0.4
        
        # RSSI component (40%)
        if rssi is not None:
            # RSSI typically ranges from -120 to -30 dBm
            # Map to 0-100 scale
            rssi_normalized = min(100, max(0, (rssi + 120) * 1.11))
            score += rssi_normalized * 0.4
        
        # Reliability component (20%)
        # More packets = more reliable
        reliability = min(100, packet_count * 2)
        score += reliability * 0.2
        
        return round(score, 2)
    
    def mark_inactive_links(self, timeout_minutes: int = 60):
        """Mark links as inactive if not heard recently"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            timeout_time = (datetime.utcnow() - timedelta(minutes=timeout_minutes)).isoformat()
            
            cursor.execute("""
                UPDATE network_topology 
                SET is_active = 0 
                WHERE last_heard_utc < ? AND is_active = 1
            """, (timeout_time,))
            
            conn.commit()
            
        except Exception as e:
            logger.warn(f"Failed to mark inactive links: {e}")
    
    def get_all_nodes(self) -> List[Dict[str, Any]]:
        """Get all nodes with their information"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM nodes ORDER BY last_seen_utc DESC")
            rows = cursor.fetchall()
            
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.warn(f"Failed to get nodes: {e}")
            return []
    
    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get specific node information"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,))
            row = cursor.fetchone()
            
            return dict(row) if row else None
            
        except Exception as e:
            logger.warn(f"Failed to get node {node_id}: {e}")
            return None
    
    def get_node_packets(self, node_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get packet history for a node"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM packet_history 
                WHERE node_id = ? 
                ORDER BY received_at_utc DESC 
                LIMIT ?
            """, (node_id, limit))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.warn(f"Failed to get packets for {node_id}: {e}")
            return []
    
    def get_topology(self, active_only: bool = True) -> List[Dict[str, Any]]:
        """Get network topology data"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if active_only:
                cursor.execute("SELECT * FROM network_topology WHERE is_active = 1")
            else:
                cursor.execute("SELECT * FROM network_topology")
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.warn(f"Failed to get topology: {e}")
            return []
    
    def get_neighbors(self, node_id: str) -> List[Dict[str, Any]]:
        """Get all neighbors (direct connections) for a node"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT * FROM network_topology 
                WHERE (source_node_id = ? OR neighbor_node_id = ?) 
                AND is_active = 1
            """, (node_id, node_id))
            
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
        except Exception as e:
            logger.warn(f"Failed to get neighbors for {node_id}: {e}")
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get overall network statistics"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            stats = {}
            
            # Node count
            cursor.execute("SELECT COUNT(*) as count FROM nodes")
            stats['total_nodes'] = cursor.fetchone()['count']
            
            # Active nodes (seen in last hour)
            one_hour_ago = (datetime.utcnow() - timedelta(hours=1)).isoformat()
            cursor.execute("SELECT COUNT(*) as count FROM nodes WHERE last_seen_utc > ?", (one_hour_ago,))
            stats['active_nodes'] = cursor.fetchone()['count']
            
            # Total packets
            cursor.execute("SELECT COUNT(*) as count FROM packet_history")
            stats['total_packets'] = cursor.fetchone()['count']
            
            # Active links
            cursor.execute("SELECT COUNT(*) as count FROM network_topology WHERE is_active = 1")
            stats['active_links'] = cursor.fetchone()['count']
            
            # Average link quality
            cursor.execute("SELECT AVG(link_quality_score) as avg FROM network_topology WHERE is_active = 1")
            result = cursor.fetchone()
            stats['avg_link_quality'] = round(result['avg'], 2) if result['avg'] else 0
            
            return stats
            
        except Exception as e:
            logger.warn(f"Failed to get statistics: {e}")
            return {}
    
    def insert_traceroute(self, from_node_id: str, route_ids: List[str],
                          to_node_id: Optional[str] = None, snr_data: Optional[List[float]] = None,
                          packet_id: Optional[int] = None) -> Optional[int]:
        """Insert a traceroute record. Returns the traceroute ID or None on failure."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            now = datetime.utcnow().isoformat()
            route_json = json.dumps(route_ids)
            snr_json = json.dumps(snr_data) if snr_data else None
            # hop_count = number of intermediate relay nodes (0 = direct)
            hop_count = len(route_ids)

            cursor.execute("""
                INSERT INTO traceroutes (from_node_id, to_node_id, route_json, hop_count,
                                        received_at_utc, snr_data, packet_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (from_node_id, to_node_id, route_json, hop_count, now, snr_json, packet_id))

            conn.commit()
            return cursor.lastrowid

        except Exception as e:
            logger.warn(f"Failed to insert traceroute: {e}")
            return None

    def get_all_traceroutes(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all traceroutes ordered by most recent"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT t.*,
                       fn.long_name as from_long_name, fn.short_name as from_short_name,
                       tn.long_name as to_long_name, tn.short_name as to_short_name
                FROM traceroutes t
                LEFT JOIN nodes fn ON t.from_node_id = fn.node_id
                LEFT JOIN nodes tn ON t.to_node_id = tn.node_id
                ORDER BY t.received_at_utc DESC
                LIMIT ?
            """, (limit,))

            rows = cursor.fetchall()
            result = []

            # Build a lookup of all node short names
            cursor.execute("SELECT node_id, short_name FROM nodes")
            node_names = {row['node_id']: row['short_name'] for row in cursor.fetchall()}

            for row in rows:
                trace = dict(row)
                # Parse JSON fields
                trace['route'] = json.loads(trace['route_json'])
                if trace['snr_data']:
                    trace['snr_data'] = json.loads(trace['snr_data'])
                # Add route_names with short names for each hop
                trace['route_names'] = [
                    node_names.get(node_id, node_id[-4:]) for node_id in trace['route']
                ]
                result.append(trace)

            return result

        except Exception as e:
            logger.warn(f"Failed to get traceroutes: {e}")
            return []

    def get_traceroute(self, traceroute_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific traceroute by ID"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT t.*,
                       fn.long_name as from_long_name, fn.short_name as from_short_name,
                       tn.long_name as to_long_name, tn.short_name as to_short_name
                FROM traceroutes t
                LEFT JOIN nodes fn ON t.from_node_id = fn.node_id
                LEFT JOIN nodes tn ON t.to_node_id = tn.node_id
                WHERE t.id = ?
            """, (traceroute_id,))

            row = cursor.fetchone()
            if not row:
                return None

            trace = dict(row)
            trace['route'] = json.loads(trace['route_json'])
            if trace['snr_data']:
                trace['snr_data'] = json.loads(trace['snr_data'])

            return trace

        except Exception as e:
            logger.warn(f"Failed to get traceroute {traceroute_id}: {e}")
            return None

    def get_traceroutes_by_node(self, node_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get all traceroutes involving a specific node (as source, destination, or in route)"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT t.*,
                       fn.long_name as from_long_name, fn.short_name as from_short_name,
                       tn.long_name as to_long_name, tn.short_name as to_short_name
                FROM traceroutes t
                LEFT JOIN nodes fn ON t.from_node_id = fn.node_id
                LEFT JOIN nodes tn ON t.to_node_id = tn.node_id
                WHERE t.from_node_id = ? OR t.to_node_id = ? OR t.route_json LIKE ?
                ORDER BY t.received_at_utc DESC
                LIMIT ?
            """, (node_id, node_id, f'%{node_id}%', limit))

            rows = cursor.fetchall()
            result = []

            for row in rows:
                trace = dict(row)
                trace['route'] = json.loads(trace['route_json'])
                if trace['snr_data']:
                    trace['snr_data'] = json.loads(trace['snr_data'])
                result.append(trace)

            return result

        except Exception as e:
            logger.warn(f"Failed to get traceroutes for node {node_id}: {e}")
            return []

    def get_nodes_needing_traceroute(self, active_threshold_minutes: int = 60,
                                      traceroute_age_hours: int = 4,
                                      exclude_mqtt: bool = True,
                                      limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get nodes that need a traceroute sent to them.

        Args:
            active_threshold_minutes: Node must be seen within this time to be "active"
            traceroute_age_hours: Send traceroute if last one older than this
            exclude_mqtt: Don't include MQTT-only nodes
            limit: Maximum number of nodes to return

        Returns:
            List of nodes needing traceroutes, ordered by: never-traced first, then oldest traceroute
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Calculate time thresholds
            active_cutoff = (datetime.utcnow() - timedelta(minutes=active_threshold_minutes)).isoformat()
            traceroute_cutoff = (datetime.utcnow() - timedelta(hours=traceroute_age_hours)).isoformat()

            # Build query to find active nodes needing traceroutes
            # LEFT JOIN with traceroutes to find last traceroute per node (by to_node_id)
            query = """
                SELECT
                    n.node_id,
                    n.node_num,
                    n.long_name,
                    n.short_name,
                    n.is_mqtt,
                    n.last_seen_utc,
                    MAX(t.received_at_utc) as last_traceroute_utc
                FROM nodes n
                LEFT JOIN traceroutes t ON n.node_id = t.to_node_id
                WHERE n.last_seen_utc >= ?
                  AND n.node_num IS NOT NULL
            """

            params = [active_cutoff]

            if exclude_mqtt:
                query += " AND (n.is_mqtt = 0 OR n.is_mqtt IS NULL)"

            query += """
                GROUP BY n.node_id, n.node_num, n.long_name, n.short_name, n.is_mqtt, n.last_seen_utc
                HAVING last_traceroute_utc IS NULL OR last_traceroute_utc < ?
                ORDER BY
                    CASE WHEN last_traceroute_utc IS NULL THEN 0 ELSE 1 END,
                    last_traceroute_utc ASC
                LIMIT ?
            """

            params.extend([traceroute_cutoff, limit])

            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

        except Exception as e:
            logger.warn(f"Failed to get nodes needing traceroute: {e}")
            return []

    def insert_traceroute_attempt(self, to_node_id: str, to_node_name: Optional[str] = None) -> Optional[int]:
        """Log a traceroute attempt when sending a request"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            now = datetime.utcnow().isoformat()

            cursor.execute("""
                INSERT INTO traceroute_attempts (to_node_id, to_node_name, requested_at_utc, status)
                VALUES (?, ?, ?, 'pending')
            """, (to_node_id, to_node_name, now))

            conn.commit()
            return cursor.lastrowid

        except Exception as e:
            logger.warn(f"Failed to insert traceroute attempt: {e}")
            return None

    def complete_traceroute_attempt(self, to_node_id: str, traceroute_id: Optional[int] = None) -> bool:
        """Mark the most recent pending attempt to a node as completed"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            now = datetime.utcnow().isoformat()

            # Find the most recent pending attempt to this node
            cursor.execute("""
                UPDATE traceroute_attempts
                SET status = 'completed', completed_at_utc = ?, traceroute_id = ?
                WHERE id = (
                    SELECT id FROM traceroute_attempts
                    WHERE to_node_id = ? AND status = 'pending'
                    ORDER BY requested_at_utc DESC
                    LIMIT 1
                )
            """, (now, traceroute_id, to_node_id))

            conn.commit()
            return cursor.rowcount > 0

        except Exception as e:
            logger.warn(f"Failed to complete traceroute attempt: {e}")
            return False

    def timeout_stale_attempts(self, timeout_seconds: int = 120) -> int:
        """Mark pending attempts older than timeout as timed out"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            timeout_cutoff = (datetime.utcnow() - timedelta(seconds=timeout_seconds)).isoformat()

            cursor.execute("""
                UPDATE traceroute_attempts
                SET status = 'timeout'
                WHERE status = 'pending' AND requested_at_utc < ?
            """, (timeout_cutoff,))

            conn.commit()
            return cursor.rowcount

        except Exception as e:
            logger.warn(f"Failed to timeout stale attempts: {e}")
            return 0

    def get_traceroute_attempts(self, limit: int = 100, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get traceroute attempts, optionally filtered by status"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            if status:
                cursor.execute("""
                    SELECT a.*, t.hop_count, t.route_json
                    FROM traceroute_attempts a
                    LEFT JOIN traceroutes t ON a.traceroute_id = t.id
                    WHERE a.status = ?
                    ORDER BY a.requested_at_utc DESC
                    LIMIT ?
                """, (status, limit))
            else:
                cursor.execute("""
                    SELECT a.*, t.hop_count, t.route_json
                    FROM traceroute_attempts a
                    LEFT JOIN traceroutes t ON a.traceroute_id = t.id
                    ORDER BY a.requested_at_utc DESC
                    LIMIT ?
                """, (limit,))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        except Exception as e:
            logger.warn(f"Failed to get traceroute attempts: {e}")
            return []

    def get_attempt_stats(self) -> Dict[str, Any]:
        """Get statistics about traceroute attempts"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            stats = {}

            # Count by status
            cursor.execute("""
                SELECT status, COUNT(*) as count
                FROM traceroute_attempts
                GROUP BY status
            """)
            for row in cursor.fetchall():
                stats[f"attempts_{row['status']}"] = row['count']

            # Recent success rate (last 24 hours)
            cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'timeout' THEN 1 ELSE 0 END) as timeout
                FROM traceroute_attempts
                WHERE requested_at_utc >= ?
            """, (cutoff,))
            row = cursor.fetchone()
            if row and row['total'] > 0:
                stats['recent_total'] = row['total']
                stats['recent_completed'] = row['completed']
                stats['recent_timeout'] = row['timeout']
                stats['recent_success_rate'] = round(row['completed'] / row['total'] * 100, 1)

            return stats

        except Exception as e:
            logger.warn(f"Failed to get attempt stats: {e}")
            return {}

    # Telemetry request tracking methods

    def insert_telemetry_request(self, to_node_id: str, to_node_name: Optional[str] = None) -> Optional[int]:
        """Log a telemetry request when sending"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            now = datetime.utcnow().isoformat()

            cursor.execute("""
                INSERT INTO telemetry_requests (to_node_id, to_node_name, requested_at_utc, status)
                VALUES (?, ?, ?, 'pending')
            """, (to_node_id, to_node_name, now))

            conn.commit()
            return cursor.lastrowid

        except Exception as e:
            logger.warn(f"Failed to insert telemetry request: {e}")
            return None

    def complete_telemetry_request(self, from_node_id: str, rx_snr: Optional[float] = None,
                                    rx_rssi: Optional[int] = None, relay_node_id: Optional[str] = None,
                                    relay_node_name: Optional[str] = None, hops_away: Optional[int] = None) -> bool:
        """Mark the most recent pending telemetry request from a node as completed"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            now = datetime.utcnow().isoformat()

            # Find the most recent pending request to this node
            cursor.execute("""
                UPDATE telemetry_requests
                SET status = 'completed', completed_at_utc = ?,
                    rx_snr = ?, rx_rssi = ?, relay_node_id = ?, relay_node_name = ?, hops_away = ?
                WHERE id = (
                    SELECT id FROM telemetry_requests
                    WHERE to_node_id = ? AND status = 'pending'
                    ORDER BY requested_at_utc DESC
                    LIMIT 1
                )
            """, (now, rx_snr, rx_rssi, relay_node_id, relay_node_name, hops_away, from_node_id))

            conn.commit()
            return cursor.rowcount > 0

        except Exception as e:
            logger.warn(f"Failed to complete telemetry request: {e}")
            return False

    def timeout_stale_telemetry_requests(self, timeout_seconds: int = 120) -> int:
        """Mark pending telemetry requests older than timeout as timed out"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            timeout_cutoff = (datetime.utcnow() - timedelta(seconds=timeout_seconds)).isoformat()

            cursor.execute("""
                UPDATE telemetry_requests
                SET status = 'timeout'
                WHERE status = 'pending' AND requested_at_utc < ?
            """, (timeout_cutoff,))

            conn.commit()
            return cursor.rowcount

        except Exception as e:
            logger.warn(f"Failed to timeout stale telemetry requests: {e}")
            return 0

    def get_nodes_needing_telemetry_request(self, active_threshold_minutes: int = 120,
                                             request_age_hours: int = 2,
                                             exclude_mqtt: bool = True,
                                             skip_recent_traceroutes: bool = True,
                                             traceroute_age_hours: int = 4,
                                             limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get nodes that need a telemetry request sent to them.

        Args:
            active_threshold_minutes: Node must be seen within this time to be "active"
            request_age_hours: Send request if last successful one older than this
            exclude_mqtt: Don't include MQTT-only nodes
            skip_recent_traceroutes: Skip nodes with successful recent traceroutes
            traceroute_age_hours: What counts as "recent" for traceroutes
            limit: Maximum number of nodes to return

        Returns:
            List of nodes needing telemetry requests
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # Calculate time thresholds
            active_cutoff = (datetime.utcnow() - timedelta(minutes=active_threshold_minutes)).isoformat()
            request_cutoff = (datetime.utcnow() - timedelta(hours=request_age_hours)).isoformat()
            traceroute_cutoff = (datetime.utcnow() - timedelta(hours=traceroute_age_hours)).isoformat()

            # Build query
            query = """
                SELECT
                    n.node_id,
                    n.node_num,
                    n.long_name,
                    n.short_name,
                    n.is_mqtt,
                    n.last_seen_utc,
                    MAX(tr.completed_at_utc) as last_telemetry_request_utc,
                    MAX(t.received_at_utc) as last_traceroute_utc
                FROM nodes n
                LEFT JOIN telemetry_requests tr ON n.node_id = tr.to_node_id AND tr.status = 'completed'
                LEFT JOIN traceroutes t ON n.node_id = t.to_node_id
                WHERE n.last_seen_utc >= ?
                  AND n.node_num IS NOT NULL
            """

            params = [active_cutoff]

            if exclude_mqtt:
                query += " AND (n.is_mqtt = 0 OR n.is_mqtt IS NULL)"

            query += """
                GROUP BY n.node_id, n.node_num, n.long_name, n.short_name, n.is_mqtt, n.last_seen_utc
                HAVING (last_telemetry_request_utc IS NULL OR last_telemetry_request_utc < ?)
            """
            params.append(request_cutoff)

            if skip_recent_traceroutes:
                query += " AND (last_traceroute_utc IS NULL OR last_traceroute_utc < ?)"
                params.append(traceroute_cutoff)

            query += """
                ORDER BY
                    CASE WHEN last_telemetry_request_utc IS NULL THEN 0 ELSE 1 END,
                    last_telemetry_request_utc ASC
                LIMIT ?
            """
            params.append(limit)

            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [dict(row) for row in rows]

        except Exception as e:
            logger.warn(f"Failed to get nodes needing telemetry request: {e}")
            return []

    def get_telemetry_requests(self, limit: int = 100, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get telemetry requests, optionally filtered by status"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            if status:
                cursor.execute("""
                    SELECT * FROM telemetry_requests
                    WHERE status = ?
                    ORDER BY requested_at_utc DESC
                    LIMIT ?
                """, (status, limit))
            else:
                cursor.execute("""
                    SELECT * FROM telemetry_requests
                    ORDER BY requested_at_utc DESC
                    LIMIT ?
                """, (limit,))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        except Exception as e:
            logger.warn(f"Failed to get telemetry requests: {e}")
            return []

    def get_telemetry_request_stats(self) -> Dict[str, Any]:
        """Get statistics about telemetry requests"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            stats = {}

            # Count by status
            cursor.execute("""
                SELECT status, COUNT(*) as count
                FROM telemetry_requests
                GROUP BY status
            """)
            for row in cursor.fetchall():
                stats[f"telemetry_{row['status']}"] = row['count']

            # Recent success rate (last 24 hours)
            cutoff = (datetime.utcnow() - timedelta(hours=24)).isoformat()
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'timeout' THEN 1 ELSE 0 END) as timeout,
                    AVG(CASE WHEN status = 'completed' THEN rx_snr END) as avg_snr,
                    AVG(CASE WHEN status = 'completed' THEN rx_rssi END) as avg_rssi
                FROM telemetry_requests
                WHERE requested_at_utc >= ?
            """, (cutoff,))
            row = cursor.fetchone()
            if row and row['total'] > 0:
                stats['telemetry_recent_total'] = row['total']
                stats['telemetry_recent_completed'] = row['completed']
                stats['telemetry_recent_timeout'] = row['timeout']
                stats['telemetry_recent_success_rate'] = round(row['completed'] / row['total'] * 100, 1)
                if row['avg_snr']:
                    stats['telemetry_avg_snr'] = round(row['avg_snr'], 1)
                if row['avg_rssi']:
                    stats['telemetry_avg_rssi'] = round(row['avg_rssi'], 0)

            return stats

        except Exception as e:
            logger.warn(f"Failed to get telemetry request stats: {e}")
            return {}

    def set_node_ignored(self, node_id: str, ignored: bool) -> bool:
        """Set or unset the ignored status for a node"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(
                "UPDATE nodes SET is_ignored = ? WHERE node_id = ?",
                (1 if ignored else 0, node_id)
            )
            conn.commit()
            return cursor.rowcount > 0

        except Exception as e:
            logger.warn(f"Failed to set ignored status for {node_id}: {e}")
            return False

    def close(self):
        """Close database connection"""
        if hasattr(_thread_local, 'connection'):
            _thread_local.connection.close()
            delattr(_thread_local, 'connection')