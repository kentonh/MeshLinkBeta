// MeshLink Network Map - JavaScript

// API Configuration
const API_BASE = window.location.origin;

// Global state
let map = null;
let mapData = null;
let nodeMarkers = [];
let connectionLines = [];
let tracerouteLines = [];
let selectedNode = null;
let timeWindow = 24;

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    initializeMap();
    initializeControls();
    loadMapData();

    // Auto-refresh every 60 seconds
    setInterval(() => {
        loadMapData(true);
    }, 60000);
});

// Initialize Leaflet map
function initializeMap() {
    map = L.map('map', {
        center: [0, 0],
        zoom: 2,
        zoomControl: true
    });

    // Add OpenStreetMap tile layer
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        maxZoom: 19
    }).addTo(map);

    // Close details panel when clicking on map
    map.on('click', function() {
        closeDetailsPanel();
    });
}

// Initialize controls
function initializeControls() {
    // Time window selector
    const timeSelect = document.getElementById('time-window');
    timeSelect.addEventListener('change', (e) => {
        timeWindow = parseInt(e.target.value);
        loadMapData();
    });

    // Refresh button
    document.getElementById('refresh-btn').addEventListener('click', () => {
        loadMapData();
    });

    // Legend toggle
    document.getElementById('legend-toggle').addEventListener('click', function() {
        const legend = document.getElementById('map-legend');
        const isHidden = legend.classList.toggle('collapsed');
        this.textContent = isHidden ? 'Show Legend' : 'Hide Legend';
    });

    // Close details panel
    document.getElementById('close-details').addEventListener('click', closeDetailsPanel);
}

// Load map data from API
async function loadMapData(silent = false) {
    if (!silent) {
        showLoading(true);
    }

    try {
        const response = await fetch(`${API_BASE}/api/map-data?hours=${timeWindow}`);
        const data = await response.json();

        if (data.success) {
            mapData = data;
            updateStats(data.stats);
            renderMap();
        } else {
            console.error('Failed to load map data:', data.error);
        }
    } catch (error) {
        console.error('Error loading map data:', error);
    } finally {
        showLoading(false);
    }
}

// Update statistics display
function updateStats(stats) {
    document.getElementById('stat-nodes').textContent = stats.nodesWithGps || 0;
    document.getElementById('stat-connections').textContent = stats.totalConnections || 0;
    document.getElementById('stat-traceroutes').textContent = stats.tracerouteConnections || 0;
}

// Show/hide loading indicator
function showLoading(show) {
    const indicator = document.getElementById('loading-indicator');
    indicator.style.display = show ? 'flex' : 'none';
}

// Render the map with nodes and connections
function renderMap() {
    if (!mapData) return;

    // Clear existing markers and lines
    clearMapLayers();

    const nodes = mapData.nodes || [];
    const connections = mapData.connections || [];
    const tracerouteConnections = mapData.tracerouteConnections || [];

    // Create node lookup for connections
    const nodeById = {};
    nodes.forEach(node => {
        nodeById[node.id] = node;
    });

    // Draw topology connections first (under markers)
    connections.forEach(conn => {
        const fromNode = nodeById[conn.from];
        const toNode = nodeById[conn.to];

        if (fromNode && toNode) {
            const line = drawConnection(fromNode, toNode, conn);
            if (line) {
                connectionLines.push(line);
            }
        }
    });

    // Draw traceroute connections (different style)
    tracerouteConnections.forEach(conn => {
        const fromNode = nodeById[conn.from];
        const toNode = nodeById[conn.to];

        if (fromNode && toNode) {
            const line = drawTracerouteConnection(fromNode, toNode, conn);
            if (line) {
                tracerouteLines.push(line);
            }
        }
    });

    // Draw node markers
    nodes.forEach(node => {
        const marker = createNodeMarker(node);
        nodeMarkers.push(marker);
    });

    // Fit map bounds to show all nodes
    if (nodes.length > 0) {
        fitMapBounds(nodes);
    }
}

// Clear all map layers
function clearMapLayers() {
    nodeMarkers.forEach(marker => marker.remove());
    connectionLines.forEach(line => line.remove());
    tracerouteLines.forEach(line => line.remove());

    nodeMarkers = [];
    connectionLines = [];
    tracerouteLines = [];
}

// Get node color based on last heard time
function getNodeColor(node) {
    if (!node.lastHeard) return '#9e9e9e'; // Gray - unknown

    const lastHeard = parseUTCDate(node.lastHeard);
    const now = new Date();
    const ageHours = (now - lastHeard) / (1000 * 60 * 60);

    if (ageHours < 1) return '#4caf50';    // Green - very recent
    if (ageHours < 24) return '#8bc34a';   // Light green - recent
    if (ageHours < 168) return '#ffc107';  // Yellow - this week
    return '#f44336';                       // Red - old
}

// Get connection color based on signal quality
function getConnectionColor(rssi, snr) {
    if (rssi === null || rssi === undefined) return '#9e9e9e';
    if (rssi > -110 && (snr === null || snr > 0)) return '#4caf50';  // Green - good
    if (rssi > -120 && (snr === null || snr > -5)) return '#ffc107'; // Yellow - fair
    return '#f44336'; // Red - poor
}

// Get marker radius based on battery level
function getNodeRadius(battery) {
    if (!battery) return 8;
    return 5 + (battery / 100) * 10; // 5-15px based on battery
}

// Create a node marker
function createNodeMarker(node) {
    const color = getNodeColor(node);
    const radius = getNodeRadius(node.battery);

    const marker = L.circleMarker([node.position.lat, node.position.lon], {
        radius: radius,
        fillColor: color,
        color: '#ffffff',
        weight: 2,
        opacity: 1,
        fillOpacity: 0.85
    }).addTo(map);

    // Add popup
    const popupContent = createNodePopup(node);
    marker.bindPopup(popupContent, {
        maxWidth: 300,
        className: 'node-popup'
    });

    // Add tooltip with node name
    marker.bindTooltip(node.shortName || node.name, {
        permanent: false,
        direction: 'top',
        offset: [0, -radius]
    });

    // Click handler for details panel
    marker.on('click', function(e) {
        L.DomEvent.stopPropagation(e);
        showNodeDetails(node);
    });

    return marker;
}

// Create popup content for a node
function createNodePopup(node) {
    const lastHeard = node.lastHeard ? formatRelativeTime(node.lastHeard) : 'Unknown';
    const batteryStr = node.battery !== null ? `${node.battery}%` : 'N/A';

    return `
        <div class="popup-content">
            <div class="popup-title">${escapeHtml(node.name)}</div>
            <div class="popup-id">${escapeHtml(node.id)}</div>
            <div class="popup-details">
                <div class="popup-row">
                    <span class="popup-label">Last Heard:</span>
                    <span>${lastHeard}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Battery:</span>
                    <span>${batteryStr}</span>
                </div>
                ${node.hwModel ? `
                <div class="popup-row">
                    <span class="popup-label">Hardware:</span>
                    <span>${escapeHtml(node.hwModel)}</span>
                </div>
                ` : ''}
                <div class="popup-row">
                    <span class="popup-label">Position:</span>
                    <span>${node.position.lat.toFixed(5)}, ${node.position.lon.toFixed(5)}</span>
                </div>
                ${node.position.alt ? `
                <div class="popup-row">
                    <span class="popup-label">Altitude:</span>
                    <span>${node.position.alt.toFixed(0)}m</span>
                </div>
                ` : ''}
                <div class="popup-row">
                    <span class="popup-label">Packets:</span>
                    <span>${node.totalPackets || 0}</span>
                </div>
            </div>
            <button class="popup-btn" onclick="showNodeDetails(mapData.nodes.find(n => n.id === '${node.id}'))">
                View Details
            </button>
        </div>
    `;
}

// Draw a topology connection line
function drawConnection(fromNode, toNode, conn) {
    // Only draw direct connections (1 hop)
    if (!conn.isDirect) return null;

    const color = getConnectionColor(conn.rssi, conn.snr);

    const line = L.polyline([
        [fromNode.position.lat, fromNode.position.lon],
        [toNode.position.lat, toNode.position.lon]
    ], {
        color: color,
        weight: 4,
        opacity: 0.7
    }).addTo(map);

    // Add popup with connection info
    const rssiStr = conn.rssi !== null ? `${conn.rssi.toFixed(1)} dBm` : 'N/A';
    const snrStr = conn.snr !== null ? `${conn.snr.toFixed(1)} dB` : 'N/A';
    const qualityStr = conn.quality !== null ? `${conn.quality.toFixed(0)}%` : 'N/A';

    line.bindPopup(`
        <div class="popup-content">
            <div class="popup-title">${escapeHtml(fromNode.shortName)} → ${escapeHtml(toNode.shortName)}</div>
            <div class="popup-details">
                <div class="popup-row">
                    <span class="popup-label">RSSI:</span>
                    <span>${rssiStr}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">SNR:</span>
                    <span>${snrStr}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Quality:</span>
                    <span>${qualityStr}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Packets:</span>
                    <span>${conn.packets || 0}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Connection:</span>
                    <span>${conn.isDirect ? 'Direct' : 'Multi-hop'}</span>
                </div>
            </div>
        </div>
    `, { maxWidth: 250 });

    return line;
}

// Draw a traceroute connection line (different style)
function drawTracerouteConnection(fromNode, toNode, conn) {
    // Check if this connection already exists in topology connections
    const exists = mapData.connections.some(c =>
        (c.from === conn.from && c.to === conn.to) ||
        (c.from === conn.to && c.to === conn.from)
    );

    // Skip if already drawn as topology connection
    if (exists) return null;

    const line = L.polyline([
        [fromNode.position.lat, fromNode.position.lon],
        [toNode.position.lat, toNode.position.lon]
    ], {
        color: '#9c27b0', // Purple for traceroute-only connections
        weight: 2,
        opacity: 0.5,
        dashArray: '3, 8'
    }).addTo(map);

    const snrStr = conn.snr !== null ? `${conn.snr.toFixed(1)} dB` : 'N/A';

    line.bindPopup(`
        <div class="popup-content">
            <div class="popup-title">Traceroute: ${escapeHtml(fromNode.shortName)} → ${escapeHtml(toNode.shortName)}</div>
            <div class="popup-details">
                <div class="popup-row">
                    <span class="popup-label">SNR:</span>
                    <span>${snrStr}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Source:</span>
                    <span>Traceroute data</span>
                </div>
            </div>
        </div>
    `, { maxWidth: 250 });

    return line;
}

// Fit map bounds to show all nodes
function fitMapBounds(nodes) {
    if (nodes.length === 0) return;

    if (nodes.length === 1) {
        const node = nodes[0];
        map.setView([node.position.lat, node.position.lon], 13);
    } else {
        const bounds = L.latLngBounds(nodes.map(n => [n.position.lat, n.position.lon]));
        map.fitBounds(bounds, { padding: [50, 50] });
    }
}

// Show node details in side panel
function showNodeDetails(node) {
    selectedNode = node;

    const panel = document.getElementById('details-panel');
    const title = document.getElementById('details-title');
    const content = document.getElementById('details-content');

    title.textContent = node.name || node.id;

    const lastHeard = node.lastHeard ? formatDateTime(node.lastHeard) : 'Unknown';
    const lastHeardRel = node.lastHeard ? formatRelativeTime(node.lastHeard) : '';

    content.innerHTML = `
        <div class="detail-section">
            <h4>Node Information</h4>
            <div class="detail-row">
                <span class="detail-label">Node ID:</span>
                <span class="detail-value monospace">${escapeHtml(node.id)}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Short Name:</span>
                <span class="detail-value">${escapeHtml(node.shortName || 'N/A')}</span>
            </div>
            ${node.hwModel ? `
            <div class="detail-row">
                <span class="detail-label">Hardware:</span>
                <span class="detail-value">${escapeHtml(node.hwModel)}</span>
            </div>
            ` : ''}
        </div>

        <div class="detail-section">
            <h4>Status</h4>
            <div class="detail-row">
                <span class="detail-label">Last Heard:</span>
                <span class="detail-value">${lastHeard}<br><small>${lastHeardRel}</small></span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Total Packets:</span>
                <span class="detail-value">${node.totalPackets || 0}</span>
            </div>
            ${node.battery !== null ? `
            <div class="detail-row">
                <span class="detail-label">Battery:</span>
                <span class="detail-value">
                    <div class="battery-bar-container">
                        <div class="battery-bar" style="width: ${node.battery}%; background: ${getBatteryColor(node.battery)}"></div>
                    </div>
                    ${node.battery}%
                </span>
            </div>
            ` : ''}
            <div class="detail-row">
                <span class="detail-label">MQTT Node:</span>
                <span class="detail-value">${node.isMqtt ? 'Yes' : 'No'}</span>
            </div>
        </div>

        <div class="detail-section">
            <h4>Location</h4>
            <div class="detail-row">
                <span class="detail-label">Latitude:</span>
                <span class="detail-value">${node.position.lat.toFixed(6)}</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Longitude:</span>
                <span class="detail-value">${node.position.lon.toFixed(6)}</span>
            </div>
            ${node.position.alt ? `
            <div class="detail-row">
                <span class="detail-label">Altitude:</span>
                <span class="detail-value">${node.position.alt.toFixed(0)}m</span>
            </div>
            ` : ''}
            <div class="detail-row">
                <a href="https://www.google.com/maps/search/?api=1&query=${node.position.lat},${node.position.lon}"
                   target="_blank" class="detail-link">
                    Open in Google Maps
                </a>
            </div>
        </div>

        <div class="detail-section">
            <h4>Connections</h4>
            ${getNodeConnections(node.id)}
        </div>

        <div class="detail-actions">
            <a href="/nodes.html" class="action-btn">View in Node List</a>
        </div>
    `;

    panel.classList.add('open');
}

// Get connections for a specific node
function getNodeConnections(nodeId) {
    if (!mapData) return '<p>No data</p>';

    const connections = mapData.connections.filter(c =>
        c.from === nodeId || c.to === nodeId
    );

    if (connections.length === 0) {
        return '<p class="no-data">No connections found</p>';
    }

    return connections.map(conn => {
        const otherId = conn.from === nodeId ? conn.to : conn.from;
        const otherNode = mapData.nodes.find(n => n.id === otherId);
        const otherName = otherNode ? otherNode.shortName : otherId.slice(-4);

        const rssiStr = conn.rssi !== null ? `${conn.rssi.toFixed(0)} dBm` : 'N/A';
        const snrStr = conn.snr !== null ? `${conn.snr.toFixed(1)} dB` : 'N/A';
        const direction = conn.from === nodeId ? '→' : '←';

        return `
            <div class="connection-item">
                <div class="connection-name">${direction} ${escapeHtml(otherName)}</div>
                <div class="connection-stats">
                    <span>RSSI: ${rssiStr}</span>
                    <span>SNR: ${snrStr}</span>
                </div>
            </div>
        `;
    }).join('');
}

// Close details panel
function closeDetailsPanel() {
    const panel = document.getElementById('details-panel');
    panel.classList.remove('open');
    selectedNode = null;
}

// Get battery color
function getBatteryColor(level) {
    if (level > 60) return '#4caf50';
    if (level > 20) return '#ffc107';
    return '#f44336';
}

// Parse UTC date string
function parseUTCDate(isoString) {
    if (!isoString) return null;
    const utcString = isoString.endsWith('Z') ? isoString : isoString + 'Z';
    return new Date(utcString);
}

// Format relative time
function formatRelativeTime(isoString) {
    const date = parseUTCDate(isoString);
    if (!date) return 'Unknown';

    const now = new Date();
    const diff = now - date;
    const minutes = Math.floor(diff / 60000);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    if (diff < 0) return 'Just now';
    if (minutes < 1) return 'Just now';
    if (minutes < 60) return `${minutes}m ago`;
    if (hours < 24) return `${hours}h ago`;
    if (days < 7) return `${days}d ago`;
    return date.toLocaleDateString();
}

// Format date/time
function formatDateTime(isoString) {
    const date = parseUTCDate(isoString);
    if (!date) return 'Unknown';
    return date.toLocaleString();
}

// Escape HTML
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
