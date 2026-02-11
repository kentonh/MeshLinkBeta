// MeshLink Coverage Map - map3.js
// Implements connection logic from connection-logic.md

const API_BASE = window.location.origin;

// Global state
let map = null;
let mapData = null;
let nodeMarkers = [];
let directConnectionLines = [];
let indirectCoverageShapes = [];
let overallCoverageHull = null;
let selectedNode = null;
let selectedShape = null;
let timeWindow = 24;

// Opacity settings
const DEFAULT_OPACITY = 0.2;
const SELECTED_OPACITY = 0.7;

// Map configuration
const MAP_CENTER = [37.6872, -97.3301]; // Wichita, Kansas
const MAP_ZOOM = 13;

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    initializeMap();
    initializeControls();
    loadMapData().then(() => {
        // Check for node parameter in URL to open details panel
        const params = new URLSearchParams(window.location.search);
        const nodeId = params.get('node');
        if (nodeId && mapData && mapData.nodes) {
            const node = mapData.nodes.find(n => n.id === nodeId);
            if (node) {
                showNodeDetails(node);
                // Center map on the node if it has a position
                if (node.position && node.position.lat && node.position.lon) {
                    map.setView([node.position.lat, node.position.lon], 15);
                }
            }
        }
    });
});

// Initialize Leaflet map
function initializeMap() {
    map = L.map('map', {
        center: MAP_CENTER,
        zoom: MAP_ZOOM,
        zoomControl: true
    });

    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        maxZoom: 19
    }).addTo(map);

    map.on('click', function() {
        deselectAllShapes();
        closeDetailsPanel();
    });
}

// Initialize controls
function initializeControls() {
    document.getElementById('time-window').addEventListener('change', (e) => {
        timeWindow = parseInt(e.target.value);
        loadMapData();
    });

    document.getElementById('refresh-btn').addEventListener('click', () => {
        loadMapData();
    });

    document.getElementById('legend-toggle').addEventListener('click', function() {
        const legend = document.getElementById('map-legend');
        const isHidden = legend.classList.toggle('collapsed');
        this.textContent = isHidden ? 'Show Legend' : 'Hide Legend';
    });

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
    document.getElementById('stat-nodes').textContent = stats.totalNodes || 0;
    document.getElementById('stat-direct').textContent = stats.directConnections || 0;
    document.getElementById('stat-indirect').textContent = stats.indirectCoverage || 0;
}

// Show/hide loading indicator
function showLoading(show) {
    const indicator = document.getElementById('loading-indicator');
    indicator.style.display = show ? 'flex' : 'none';
}

// Render the map
function renderMap() {
    if (!mapData) return;

    clearMapLayers();

    // Filter out ignored nodes
    const ignoredIds = new Set((mapData.nodes || []).filter(n => n.isIgnored).map(n => n.id));
    const nodes = (mapData.nodes || []).filter(node => !node.isIgnored);

    // Airplane nodes are shown as markers but excluded from shapes/connections
    const airplaneIds = new Set(nodes.filter(n => n.isAirplane).map(n => n.id));

    // Filter connections involving ignored or airplane nodes
    const directConnections = (mapData.directConnections || [])
        .filter(c => !ignoredIds.has(c.from) && !ignoredIds.has(c.to)
                   && !airplaneIds.has(c.from) && !airplaneIds.has(c.to));

    // Filter indirect coverage involving ignored or airplane relay nodes
    const indirectCoverage = (mapData.indirectCoverage || [])
        .filter(c => !ignoredIds.has(c.relayNodeId) && !airplaneIds.has(c.relayNodeId));

    // Create node lookup
    const nodeById = {};
    nodes.forEach(node => {
        nodeById[node.id] = node;
    });

    // Non-airplane nodes for shape calculations
    const shapeNodes = nodes.filter(n => !n.isAirplane);

    // Draw overall coverage hull first (underneath everything), excluding airplanes
    drawOverallCoverageHull(shapeNodes);

    // Draw indirect coverage shapes (under connections and nodes)
    // Shape centered on RELAY node, edges defined by SENDING nodes
    // Exclude airplane nodes from sending node sets
    indirectCoverage.forEach(coverage => {
        const relayNode = nodeById[coverage.relayNodeId];
        if (relayNode) {
            const filteredCoverage = {
                ...coverage,
                sendingNodeIds: coverage.sendingNodeIds.filter(id => !airplaneIds.has(id))
            };
            const shape = drawIndirectCoverage(relayNode, filteredCoverage, nodeById);
            if (shape) {
                indirectCoverageShapes.push(shape);
            }
        }
    });

    // Draw direct connections
    directConnections.forEach(conn => {
        const fromNode = nodeById[conn.from];
        const toNode = nodeById[conn.to];

        if (fromNode && toNode) {
            const line = drawDirectConnection(fromNode, toNode, conn);
            if (line) {
                directConnectionLines.push(line);
            }
        }
    });

    // Draw node markers on top
    nodes.forEach(node => {
        const marker = createNodeMarker(node);
        nodeMarkers.push(marker);
    });
}

// Clear all map layers
function clearMapLayers() {
    nodeMarkers.forEach(marker => marker.remove());
    directConnectionLines.forEach(line => line.remove());
    indirectCoverageShapes.forEach(shape => shape.remove());
    if (overallCoverageHull) {
        overallCoverageHull.remove();
        overallCoverageHull = null;
    }

    nodeMarkers = [];
    directConnectionLines = [];
    indirectCoverageShapes = [];
    selectedShape = null;
}

// Deselect all shapes and lines
function deselectAllShapes() {
    // Reset all direct connection lines to default opacity
    directConnectionLines.forEach(line => {
        line.setStyle({ opacity: DEFAULT_OPACITY });
    });

    // Reset all indirect coverage shapes to default opacity
    indirectCoverageShapes.forEach(shape => {
        shape.setStyle({
            opacity: DEFAULT_OPACITY,
            fillOpacity: DEFAULT_OPACITY
        });
    });

    selectedShape = null;
}

// Get node color based on last heard time
function getNodeColor(node) {
    if (!node.lastHeard) return '#9e9e9e';

    const lastHeard = parseUTCDate(node.lastHeard);
    const now = new Date();
    const ageHours = (now - lastHeard) / (1000 * 60 * 60);

    if (ageHours < 3) return '#4caf50';    // Green - online
    if (ageHours < 12) return '#8bc34a';   // Light green - recent
    if (ageHours < 168) return '#ffc107';  // Yellow - this week
    return '#f44336';                       // Red - old
}

// Get connection color based on signal quality
function getConnectionColor(rssi, snr) {
    if (rssi === null || rssi === undefined) return '#667eea'; // Purple - unknown
    if (rssi > -110 && (snr === null || snr > 0)) return '#09af0f';  // Green - good
    if (rssi > -120 && (snr === null || snr > -5)) return '#ffc107'; // Yellow - fair
    return '#f44336'; // Red - poor
}

// Create a node marker
function createNodeMarker(node) {
    let marker;

    if (node.isAirplane) {
        // Use airplane emoji marker
        const icon = L.divIcon({
            className: 'airplane-marker',
            html: '<span class="airplane-emoji">✈️</span>',
            iconSize: [28, 28],
            iconAnchor: [14, 14]
        });
        marker = L.marker([node.position.lat, node.position.lon], { icon }).addTo(map);

        // Tooltip with node name
        marker.bindTooltip(node.shortName || node.name, {
            permanent: false,
            direction: 'top',
            offset: [0, -14]
        });
    } else {
        // Standard circle marker
        const color = getNodeColor(node);
        const radius = node.battery ? 5 + (node.battery / 20) : 8;

        marker = L.circleMarker([node.position.lat, node.position.lon], {
            radius: radius,
            fillColor: color,
            color: '#ffffff',
            weight: 2,
            opacity: 1,
            fillOpacity: 0.85
        }).addTo(map);

        // Tooltip with node name
        marker.bindTooltip(node.shortName || node.name, {
            permanent: false,
            direction: 'top',
            offset: [0, -radius]
        });
    }

    // Click handler
    marker.on('click', function(e) {
        L.DomEvent.stopPropagation(e);
        deselectAllShapes();
        if (node.isAirplane) {
            // Airplane click goes straight to details panel (with hide option)
            showNodeDetails(node);
        } else {
            highlightNodeConnections(node.id);
            marker.openPopup();
        }
    });

    // Popup (non-airplane nodes get a map popup; airplanes use details panel)
    if (!node.isAirplane) {
        const popupContent = createNodePopup(node);
        marker.bindPopup(popupContent, { maxWidth: 300 });
    }

    return marker;
}

// Create popup content for a node
function createNodePopup(node) {
    const lastHeard = node.lastHeard ? formatRelativeTime(node.lastHeard) : 'Unknown';
    const batteryStr = node.battery !== null ? `${node.battery}%` : 'N/A';
    const airplaneIndicator = node.isAirplane ? ' ✈️' : '';

    return `
        <div class="popup-content">
            <div class="popup-title">${escapeHtml(node.name)}${airplaneIndicator}</div>
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
                <div class="popup-row">
                    <span class="popup-label">Direct Links:</span>
                    <span>${node.directLinkCount || 0}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Position:</span>
                    <span>${node.position.lat.toFixed(5)}, ${node.position.lon.toFixed(5)}</span>
                </div>
                ${node.isAirplane ? `
                <div class="popup-row">
                    <span class="popup-label">Status:</span>
                    <span>Airplane (high altitude)</span>
                </div>
                ` : ''}
            </div>
            <button class="popup-btn" onclick="showNodeDetails(mapData.nodes.find(n => n.id === '${node.id}'))">
                View Details
            </button>
        </div>
    `;
}

// Draw a direct connection line
function drawDirectConnection(fromNode, toNode, conn) {
    const color = getConnectionColor(conn.rssi, conn.snr);

    const line = L.polyline([
        [fromNode.position.lat, fromNode.position.lon],
        [toNode.position.lat, toNode.position.lon]
    ], {
        color: color,
        weight: 6,
        opacity: DEFAULT_OPACITY
    }).addTo(map);

    // Store connection data for highlighting
    line._connData = { from: conn.from, to: conn.to };

    // Popup with connection info
    const rssiStr = conn.rssi !== null ? `${conn.rssi.toFixed(1)} dBm` : 'N/A';
    const snrStr = conn.snr !== null ? `${conn.snr.toFixed(1)} dB` : 'N/A';
    const sourceStr = conn.source || 'packet';

    line.bindPopup(`
        <div class="popup-content">
            <div class="popup-title">${escapeHtml(fromNode.shortName)} ↔ ${escapeHtml(toNode.shortName)}</div>
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
                    <span class="popup-label">Source:</span>
                    <span>${sourceStr}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Packets:</span>
                    <span>${conn.packetCount || 0}</span>
                </div>
            </div>
        </div>
    `, { maxWidth: 250 });

    return line;
}

// Draw indirect coverage shape (convex hull)
// relayNode is included in the shape
// sendingNodes define the boundary
function drawIndirectCoverage(relayNode, coverage, nodeById) {
    const sendingNodes = coverage.sendingNodeIds
        .map(id => nodeById[id])
        .filter(n => n && n.position);

    if (sendingNodes.length === 0) return null;

    const relayLat = relayNode.position.lat;
    const relayLon = relayNode.position.lon;

    // Collect all points: relay node + all sending nodes
    const allPoints = [
        [relayLat, relayLon]
    ];
    sendingNodes.forEach(node => {
        allPoints.push([node.position.lat, node.position.lon]);
    });

    // Calculate max distance for popup
    let maxDistance = 0;
    sendingNodes.forEach(node => {
        const dist = calculateDistance(relayLat, relayLon, node.position.lat, node.position.lon);
        if (dist > maxDistance) maxDistance = dist;
    });

    // Compute convex hull of all points
    const hullPoints = convexHull(allPoints);

    if (hullPoints.length < 3) {
        // Not enough points for a polygon, skip
        return null;
    }

    const shape = L.polygon(hullPoints, {
        color: 'rgba(102, 126, 234, 0.8)',
        fillColor: 'rgba(102, 126, 234, 0.4)',
        opacity: DEFAULT_OPACITY,
        fillOpacity: DEFAULT_OPACITY,
        weight: 2,
        dashArray: '5, 10'
    }).addTo(map);

    // Store coverage data for highlighting when node is clicked
    shape._coverageData = {
        relayNodeId: coverage.relayNodeId,
        sendingNodeIds: coverage.sendingNodeIds
    };

    const nodeNames = sendingNodes.map(n => n.shortName).join(', ');

    shape.bindPopup(`
        <div class="popup-content">
            <div class="popup-title">Indirect Coverage: ${escapeHtml(relayNode.shortName)}</div>
            <div class="popup-details">
                <div class="popup-row">
                    <span class="popup-label">Relay Node:</span>
                    <span>${escapeHtml(relayNode.shortName)}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Total Nodes Reached:</span>
                    <span>${sendingNodes.length}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Nodes:</span>
                    <span>${escapeHtml(nodeNames)}</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Max Range:</span>
                    <span>${(maxDistance / 1000).toFixed(2)} km</span>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Hop Count:</span>
                    <span>2+</span>
                </div>
            </div>
        </div>
    `);

    return shape;
}

// Draw overall coverage hull encompassing all nodes
function drawOverallCoverageHull(nodes) {
    if (nodes.length < 3) return;

    // Collect all node positions
    const allPoints = nodes.map(node => [node.position.lat, node.position.lon]);

    // Compute convex hull
    const hullPoints = convexHull(allPoints);

    if (hullPoints.length < 3) return;

    overallCoverageHull = L.polygon(hullPoints, {
        color: '#888888',
        fillColor: '#888888',
        opacity: 0.2,
        fillOpacity: 0.2,
        weight: 1
    }).addTo(map);
}

// Compute convex hull of a set of points using monotone chain algorithm
// Points are [lat, lon] arrays
function convexHull(inputPoints) {
    // Make a copy to avoid modifying the original
    const points = inputPoints.map(p => [...p]);

    if (points.length < 3) return points;

    // Sort points by lat, then by lon
    points.sort((a, b) => a[0] - b[0] || a[1] - b[1]);

    // Cross product of vectors OA and OB where O is origin
    function cross(o, a, b) {
        return (a[1] - o[1]) * (b[0] - o[0]) - (a[0] - o[0]) * (b[1] - o[1]);
    }

    // Build lower hull
    const lower = [];
    for (const p of points) {
        while (lower.length >= 2 && cross(lower[lower.length - 2], lower[lower.length - 1], p) <= 0) {
            lower.pop();
        }
        lower.push(p);
    }

    // Build upper hull
    const upper = [];
    for (let i = points.length - 1; i >= 0; i--) {
        const p = points[i];
        while (upper.length >= 2 && cross(upper[upper.length - 2], upper[upper.length - 1], p) <= 0) {
            upper.pop();
        }
        upper.push(p);
    }

    // Remove last point of each half because it's repeated
    lower.pop();
    upper.pop();

    // Concatenate to form full hull
    return lower.concat(upper);
}

// Calculate distance between two points in meters (Haversine formula)
function calculateDistance(lat1, lon1, lat2, lon2) {
    const R = 6371000; // Earth's radius in meters
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon / 2) * Math.sin(dLon / 2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
}

// Show node details in side panel
function showNodeDetails(node) {
    selectedNode = node;

    // Highlight related shapes
    highlightNodeShapes(node.id);

    const panel = document.getElementById('details-panel');
    const title = document.getElementById('details-title');
    const content = document.getElementById('details-content');

    title.textContent = node.name || node.id;

    const lastHeard = node.lastHeard ? formatDateTime(node.lastHeard) : 'Unknown';
    const lastHeardRel = node.lastHeard ? formatRelativeTime(node.lastHeard) : '';

    // Get connections for this node
    const directLinks = getNodeDirectConnections(node.id);
    const indirectInfo = getNodeIndirectCoverage(node.id);

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
            ${node.isAirplane ? `
            <div class="detail-row">
                <span class="detail-label">Type:</span>
                <span class="detail-value">✈️ Airplane (high altitude)</span>
            </div>
            ` : ''}
        </div>

        <div class="detail-section">
            <h4>Battery History</h4>
            <div id="battery-chart-container" style="height: 150px; position: relative;">
                <canvas id="battery-chart"></canvas>
            </div>
        </div>

        <div class="detail-section">
            <h4>Location</h4>
            <div class="detail-row">
                <span class="detail-label">Coordinates:</span>
                <span class="detail-value">${node.position.lat.toFixed(6)}, ${node.position.lon.toFixed(6)}</span>
            </div>
            ${node.position.alt ? `
            <div class="detail-row">
                <span class="detail-label">Altitude:</span>
                <span class="detail-value">${node.position.alt.toFixed(0)}m</span>
            </div>
            ` : ''}
            <div class="detail-row">
                <a href="https://www.openstreetmap.org/?mlat=${node.position.lat}&mlon=${node.position.lon}&zoom=15"
                   target="_blank" class="detail-link">
                    Open in OpenStreetMap
                </a>
            </div>
        </div>

        <div class="detail-section">
            <h4>Direct Connections (${directLinks.length})</h4>
            ${directLinks.length > 0 ? directLinks.map(link => `
                <div class="connection-item">
                    <div class="connection-name">${escapeHtml(link.otherName)}</div>
                    <div class="connection-stats">
                        <span>RSSI: ${link.rssi !== null ? link.rssi.toFixed(0) + ' dBm' : 'N/A'}</span>
                        <span>Source: ${link.source}</span>
                    </div>
                </div>
            `).join('') : '<p class="no-data">No direct connections</p>'}
        </div>

        ${indirectInfo ? `
        <div class="detail-section">
            <h4>Indirect Coverage</h4>
            ${indirectInfo.role === 'relay' ? `
            <div class="detail-row">
                <span class="detail-label">Role:</span>
                <span class="detail-value">Relay Node</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Reaches (2+ hops):</span>
                <span class="detail-value">${indirectInfo.nodeCount} nodes</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Nodes:</span>
                <span class="detail-value">${escapeHtml(indirectInfo.nodeNames)}</span>
            </div>
            ` : `
            <div class="detail-row">
                <span class="detail-label">Role:</span>
                <span class="detail-value">Reached via relay</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Through:</span>
                <span class="detail-value">${indirectInfo.nodeCount} relay(s)</span>
            </div>
            <div class="detail-row">
                <span class="detail-label">Relays:</span>
                <span class="detail-value">${escapeHtml(indirectInfo.nodeNames)}</span>
            </div>
            `}
        </div>
        ` : ''}

        <div class="detail-actions">
            <button class="action-btn ignore-btn" onclick="toggleIgnoreNode('${node.id}')">
                ${node.isIgnored ? 'Show on Map' : 'Hide from Map'}
            </button>
            <a href="/nodes.html?node=${encodeURIComponent(node.id)}" class="action-btn secondary-btn">View Full Details</a>
        </div>
    `;

    panel.classList.add('open');

    // Load battery chart
    loadBatteryChart(node.id);
}

// Battery chart instance
let batteryChartInstance = null;

// Load and render battery chart
async function loadBatteryChart(nodeId) {
    const container = document.getElementById('battery-chart-container');
    const canvas = document.getElementById('battery-chart');

    if (!canvas) return;

    // Destroy existing chart
    if (batteryChartInstance) {
        batteryChartInstance.destroy();
        batteryChartInstance = null;
    }

    try {
        const response = await fetch(`${API_BASE}/api/nodes/${encodeURIComponent(nodeId)}/battery?days=30`);
        const data = await response.json();

        if (!data.success || data.history.length === 0) {
            container.innerHTML = '<p class="no-data">No battery data available</p>';
            return;
        }

        // Prepare chart data
        const labels = data.history.map(h => {
            const date = new Date(h.received_at_utc + 'Z');
            return date.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
        });
        const values = data.history.map(h => h.battery_level);

        // Downsample if too many points
        const maxPoints = 50;
        let chartLabels = labels;
        let chartValues = values;
        if (labels.length > maxPoints) {
            const step = Math.ceil(labels.length / maxPoints);
            chartLabels = labels.filter((_, i) => i % step === 0);
            chartValues = values.filter((_, i) => i % step === 0);
        }

        batteryChartInstance = new Chart(canvas, {
            type: 'line',
            data: {
                labels: chartLabels,
                datasets: [{
                    label: 'Battery %',
                    data: chartValues,
                    borderColor: '#667eea',
                    backgroundColor: 'rgba(102, 126, 234, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 0,
                    pointHoverRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    y: {
                        min: 0,
                        max: 100,
                        ticks: {
                            callback: v => v + '%',
                            font: { size: 10 }
                        }
                    },
                    x: {
                        ticks: {
                            maxTicksLimit: 6,
                            font: { size: 10 }
                        }
                    }
                }
            }
        });
    } catch (error) {
        console.error('Error loading battery chart:', error);
        container.innerHTML = '<p class="no-data">Failed to load battery data</p>';
    }
}

// Toggle ignore status for a node
async function toggleIgnoreNode(nodeId) {
    const node = mapData.nodes.find(n => n.id === nodeId);
    if (!node) return;

    const method = node.isIgnored ? 'DELETE' : 'POST';

    try {
        const response = await fetch(`${API_BASE}/api/nodes/${encodeURIComponent(nodeId)}/ignore`, {
            method: method
        });
        const data = await response.json();

        if (data.success) {
            // Refresh map data
            loadMapData();
            closeDetailsPanel();
        } else {
            console.error('Failed to toggle ignore status:', data.error);
        }
    } catch (error) {
        console.error('Error toggling ignore status:', error);
    }
}

// Highlight connections for a node (direct lines and indirect coverage shapes)
function highlightNodeConnections(nodeId) {
    // Highlight direct connections involving this node
    directConnectionLines.forEach(line => {
        if (line._connData && (line._connData.from === nodeId || line._connData.to === nodeId)) {
            line.setStyle({ opacity: SELECTED_OPACITY });
        }
    });

    // Highlight indirect coverage shapes involving this node
    indirectCoverageShapes.forEach(shape => {
        if (shape._coverageData) {
            const data = shape._coverageData;
            if (data.relayNodeId === nodeId || data.sendingNodeIds.includes(nodeId)) {
                shape.setStyle({
                    opacity: SELECTED_OPACITY,
                    fillOpacity: SELECTED_OPACITY
                });
            }
        }
    });
}

// Highlight shapes related to a node (used by details panel)
function highlightNodeShapes(nodeId) {
    deselectAllShapes();
    highlightNodeConnections(nodeId);

    // Also highlight indirect coverage shapes for this node (as relay or sending)
    if (mapData && mapData.indirectCoverage) {
        mapData.indirectCoverage.forEach((coverage, index) => {
            if (coverage.relayNodeId === nodeId || coverage.sendingNodeIds.includes(nodeId)) {
                if (indirectCoverageShapes[index]) {
                    indirectCoverageShapes[index].setStyle({
                        opacity: SELECTED_OPACITY,
                        fillOpacity: SELECTED_OPACITY
                    });
                }
            }
        });
    }
}

// Get direct connections for a node
function getNodeDirectConnections(nodeId) {
    if (!mapData || !mapData.directConnections) return [];

    const connections = mapData.directConnections.filter(c =>
        c.from === nodeId || c.to === nodeId
    );

    return connections.map(conn => {
        const otherId = conn.from === nodeId ? conn.to : conn.from;
        const otherNode = mapData.nodes.find(n => n.id === otherId);
        return {
            otherId: otherId,
            otherName: otherNode ? otherNode.shortName : otherId.slice(-4),
            rssi: conn.rssi,
            snr: conn.snr,
            source: conn.source || 'packet'
        };
    });
}

// Get indirect coverage info for a node (if it's a relay node)
function getNodeIndirectCoverage(nodeId) {
    if (!mapData || !mapData.indirectCoverage) return null;

    // Check if this node is a relay for indirect connections
    const asRelay = mapData.indirectCoverage.find(c => c.relayNodeId === nodeId);
    if (asRelay) {
        const sendingNames = asRelay.sendingNodeIds.map(id => {
            const node = mapData.nodes.find(n => n.id === id);
            return node ? node.shortName : id.slice(-4);
        }).join(', ');

        return {
            role: 'relay',
            nodeCount: asRelay.sendingNodeIds.length,
            nodeNames: sendingNames
        };
    }

    // Check if this node is reached indirectly through relays
    const asSource = mapData.indirectCoverage.filter(c => c.sendingNodeIds.includes(nodeId));
    if (asSource.length > 0) {
        const relayNames = asSource.map(c => {
            const node = mapData.nodes.find(n => n.id === c.relayNodeId);
            return node ? node.shortName : c.relayNodeId.slice(-4);
        }).join(', ');

        return {
            role: 'sending',
            nodeCount: asSource.length,
            nodeNames: relayNames
        };
    }

    return null;
}

// Close details panel
function closeDetailsPanel() {
    const panel = document.getElementById('details-panel');
    panel.classList.remove('open');
    selectedNode = null;
    deselectAllShapes();
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
