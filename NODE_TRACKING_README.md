# Node Tracking System - User Guide

## Overview

The Node Tracking System is a comprehensive feature that captures and stores information about all nodes detected on your Meshtastic mesh network. It provides detailed insights into node activity, battery status, network topology, and signal quality.

## Features

### 🔍 Node Discovery & Tracking
- **Automatic Detection**: Automatically discovers and tracks all nodes that send packets
- **Node Information**: Stores name, hardware model, firmware version, and unique ID
- **First/Last Seen**: Tracks when nodes were first discovered and last active
- **Total Packets**: Counts all packets received from each node

### 🔋 Battery & Power Monitoring
- **Battery Level**: Tracks current battery percentage
- **Voltage Monitoring**: Records battery voltage
- **Charging Status**: Identifies when devices are charging
- **Power Trends**: Historical battery data for trend analysis
- **Alert Capability**: Optional low battery alerts (configurable)

### 📍 Location Tracking
- **GPS Coordinates**: Stores latitude, longitude, and altitude
- **Map Integration**: Direct links to Google Maps for node locations
- **Position History**: Tracks location changes over time

### 📶 Signal Quality Metrics
- **SNR (Signal-to-Noise Ratio)**: Measures signal clarity
- **RSSI (Received Signal Strength)**: Measures signal strength
- **Link Quality Scores**: Calculated quality ratings (0-100) for each connection
- **Min/Max/Average**: Statistical analysis of signal quality

### 🌐 Network Topology
- **Neighbor Discovery**: Identifies which nodes can communicate with each other
- **Link Quality**: Calculates reliability and quality of each connection
- **Active/Inactive Links**: Tracks link status based on recent activity
- **Network Visualization**: Interactive graph showing network structure

### 📊 Web Interface
- **Node List**: Sortable, filterable table of all discovered nodes
- **Network Topology**: Visual representation of mesh network connections
- **Map View**: Geographic display of node locations
- **Real-time Updates**: Auto-refreshes every 30 seconds
- **Node Details**: Detailed view with packet history and neighbors

## Installation

### Prerequisites

```bash
# Node tracking is included by default, but web server requires additional packages
pip install flask flask-cors
```

### Configuration

1. Copy the example configuration:
```bash
cp config-example.yml config.yml
```

2. Edit `config.yml` and configure node tracking:

```yaml
node_tracking:
  enabled: true                          # Enable node tracking
  max_packets_per_node: 1000            # Packet history limit per node
  database_path: "./nodes.db"           # SQLite database location
  json_export_path: "./nodes.json"      # JSON export location
  auto_export_json: true                # Auto-export to JSON
  
  web_server:
    enabled: true                        # Enable web interface
    host: "0.0.0.0"                     # Bind to all interfaces
    port: 8080                          # Web server port
  
  track_packet_types:                   # Packet types to track
    - "TEXT_MESSAGE_APP"
    - "POSITION_APP"
    - "NODEINFO_APP"
    - "TELEMETRY_APP"
    - "ROUTING_APP"
  
  topology:
    enabled: true                        # Enable topology tracking
    link_timeout_minutes: 60            # Link inactive timeout
    min_packets_for_link: 3             # Minimum packets for valid link
    calculate_link_quality: true         # Calculate quality scores
```

3. Start MeshLink:
```bash
python3 main.py
```

## Usage

### Accessing the Web Interface

Once MeshLink is running with the web server enabled, access the interface at:

```
http://localhost:8080
```

Or from another device on your network:
```
http://YOUR_IP_ADDRESS:8080
```

### Web Interface Features

#### 1. Node List Tab
- **Search**: Filter nodes by name or ID
- **Battery Filter**: Show only nodes with high/medium/low battery
- **Sort Options**: Sort by name, last seen, battery, or packet count
- **Details Button**: Click to view complete node information

#### 2. Network Topology Tab
- **View Connections**: See which nodes can communicate
- **Quality Filter**: Filter links by quality threshold
- **Link Details**: View SNR, RSSI, and packet counts for each link

#### 3. Map View Tab
- **Geographic Display**: Shows nodes with GPS coordinates
- **Map Links**: Click to view each node's location on Google Maps

### Statistics Dashboard

The top bar displays real-time network statistics:
- **Total Nodes**: All discovered nodes
- **Active Nodes**: Nodes seen in the last hour
- **Total Packets**: Cumulative packet count
- **Active Links**: Currently active network connections
- **Average Link Quality**: Network-wide link quality percentage

### REST API

The system provides a REST API for external integrations:

```
GET /api/nodes                      # List all nodes
GET /api/nodes/{node_id}            # Get specific node
GET /api/nodes/{node_id}/packets    # Get packet history
GET /api/nodes/{node_id}/neighbors  # Get node neighbors
GET /api/topology                   # Get network topology
GET /api/topology/graph             # Get graph format
GET /api/stats                      # Get statistics
GET /api/export/json                # Export full data
GET /api/export/geojson             # Export as GeoJSON
```

Example API usage:
```bash
# Get all nodes
curl http://localhost:8080/api/nodes

# Get specific node
curl http://localhost:8080/api/nodes/!a1b2c3d4

# Get network statistics
curl http://localhost:8080/api/stats
```

## Data Storage

### SQLite Database

Node data is stored in `nodes.db` with three main tables:

1. **nodes**: Node information (name, battery, location, etc.)
2. **packet_history**: Historical packet data (limited per node)
3. **network_topology**: Link quality and neighbor relationships

### JSON Export

Data is automatically exported to `nodes.json` for:
- Backup purposes
- External tool integration
- Human-readable viewing

## Performance Considerations

### Storage Management
- Old packets are automatically deleted when exceeding `max_packets_per_node`
- Default limit: 1,000 packets per node
- Database automatically grows as needed
- Typical storage: ~1MB per 1,000 packets

### Memory Usage
- Minimal memory footprint
- Database operations use connection pooling
- Web server runs in background thread

### Network Impact
- No additional mesh traffic generated
- Only captures existing packets
- Web interface is local network only

## Troubleshooting

### Web Server Won't Start

**Issue**: Web server fails to start

**Solutions**:
1. Check if Flask is installed: `pip install flask flask-cors`
2. Verify port 8080 is not in use: `lsof -i :8080` (Linux/Mac) or `netstat -ano | findstr :8080` (Windows)
3. Try a different port in `config.yml`
4. Check firewall settings

### No Nodes Appearing

**Issue**: Node tracking enabled but no nodes shown

**Solutions**:
1. Verify node tracking is enabled in `config.yml`
2. Ensure MeshLink is receiving packets (check console output)
3. Check database permissions: `ls -l nodes.db`
4. Review logs for error messages

### Database Errors

**Issue**: SQLite database errors

**Solutions**:
1. Stop MeshLink
2. Backup database: `cp nodes.db nodes.db.backup`
3. Delete database: `rm nodes.db`
4. Restart MeshLink (database will be recreated)

### High Memory Usage

**Issue**: Memory usage increasing over time

**Solutions**:
1. Reduce `max_packets_per_node` in config
2. Disable packet types you don't need in `track_packet_types`
3. Restart MeshLink periodically

## Advanced Configuration

### Custom Packet Filtering

Track only specific packet types:

```yaml
track_packet_types:
  - "TEXT_MESSAGE_APP"    # Only track text messages
  - "TELEMETRY_APP"       # And telemetry data
```

### Battery Alerts

Enable low battery notifications:

```yaml
battery_alerts:
  enabled: true
  low_battery_threshold: 20
  critical_battery_threshold: 10
```

### Topology Tuning

Adjust topology tracking sensitivity:

```yaml
topology:
  link_timeout_minutes: 120        # Longer timeout for sparse networks
  min_packets_for_link: 5          # More packets required for valid link
```

## Data Export & Backup

### Manual Export

Export data programmatically:

```bash
# Export as JSON
curl http://localhost:8080/api/export/json > backup.json

# Export as GeoJSON (for mapping tools)
curl http://localhost:8080/api/export/geojson > nodes.geojson
```

### Database Backup

```bash
# Stop MeshLink first
sqlite3 nodes.db ".backup nodes_backup.db"
```

### Restore from Backup

```bash
# Stop MeshLink
cp nodes_backup.db nodes.db
# Start MeshLink
```

## Privacy & Security

### Data Protection
- All data stored locally on your device
- No external data transmission
- Web interface accessible only on local network

### Security Recommendations
1. **Firewall**: Block port 8080 from external access
2. **Authentication**: Consider adding reverse proxy with authentication
3. **Encryption**: Use HTTPS if exposing over internet (not recommended)

### Data Retention
- Configure `max_packets_per_node` based on privacy needs
- Lower values = less historical data stored
- Delete database to remove all tracking data

## Integration Examples

### External Monitoring

Use the API to integrate with monitoring tools:

```python
import requests

# Get current statistics
response = requests.get('http://localhost:8080/api/stats')
stats = response.json()

# Check for low battery nodes
nodes = requests.get('http://localhost:8080/api/nodes').json()
low_battery = [n for n in nodes['nodes'] if n.get('battery_level', 100) < 20]

if low_battery:
    print(f"Warning: {len(low_battery)} nodes with low battery")
```

### Automated Exports

Create automated backups:

```bash
#!/bin/bash
# Save as backup_nodes.sh

DATE=$(date +%Y%m%d_%H%M%S)
curl http://localhost:8080/api/export/json > "backups/nodes_${DATE}.json"
```

## Limitations

### Current Version Limitations
1. **Relay Node Detection**: Limited to direct neighbor detection
2. **MQTT Gateway**: Not explicitly identified in topology
3. **Full Path Tracking**: Cannot track complete routing paths
4. **Real-time Updates**: Web interface polls every 30 seconds (no WebSocket)

### Future Enhancements
- Advanced graph visualizations with D3.js/Cytoscape.js
- Real-time WebSocket updates
- Network simulation and optimization suggestions
- Mobile-responsive design improvements
- Export to network analysis tools (Gephi, etc.)

## FAQ

**Q: Does node tracking affect mesh performance?**
A: No, it only captures existing packets without generating additional mesh traffic.

**Q: Can I track nodes I haven't directly communicated with?**
A: Yes, any node whose packets reach your radio will be tracked.

**Q: How much disk space does it use?**
A: Approximately 1-2MB per 1,000 packets. With default settings (1,000 packets × 50 nodes), expect ~50-100MB.

**Q: Can I disable tracking for specific nodes?**
A: Not currently, but you can filter them out in the web interface.

**Q: Is the web interface accessible remotely?**
A: It's accessible on your local network. For remote access, use a VPN or reverse proxy (with appropriate security).

## Support

For issues, questions, or feature requests:
- Check the main README.md
- Review the architectural design in NODE_TRACKING_DESIGN.md
- Check console logs for error messages
- Verify configuration settings

## Credits

Node Tracking System developed as an extension to MeshLink for comprehensive Meshtastic network monitoring and analysis.