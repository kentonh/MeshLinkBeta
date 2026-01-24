// Telemetry Requests page JavaScript

let allRequests = [];
let currentFilter = 'all';

// Load and display telemetry requests
async function loadTelemetryRequests() {
    try {
        const limit = document.getElementById('limit-select').value;
        const response = await fetch(`/api/telemetry-requests?limit=${limit}`);
        const data = await response.json();

        if (data.success) {
            allRequests = data.requests;
            filterAndDisplay();
            updateStats(data.stats);
        } else {
            showError('Failed to load telemetry requests: ' + data.error);
        }
    } catch (error) {
        showError('Error loading telemetry requests: ' + error.message);
    }
}

// Filter and display requests
function filterAndDisplay() {
    const searchTerm = document.getElementById('search').value.toLowerCase();
    const statusFilter = document.getElementById('status-filter').value;

    const filtered = allRequests.filter(req => {
        // Status filter
        if (statusFilter !== 'all' && req.status !== statusFilter) {
            return false;
        }

        // Search filter
        if (searchTerm) {
            const nodeName = (req.to_node_name || '').toLowerCase();
            const nodeId = (req.to_node_id || '').toLowerCase();
            const relayName = (req.relay_node_name || '').toLowerCase();
            const relayId = (req.relay_node_id || '').toLowerCase();

            return nodeName.includes(searchTerm) ||
                   nodeId.includes(searchTerm) ||
                   relayName.includes(searchTerm) ||
                   relayId.includes(searchTerm);
        }

        return true;
    });

    displayRequests(filtered);
}

// Display requests in table
function displayRequests(requests) {
    const tbody = document.getElementById('telemetry-tbody');

    if (requests.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="no-data">No telemetry requests found</td></tr>';
        return;
    }

    tbody.innerHTML = requests.map(req => {
        const nodeName = req.to_node_name || req.to_node_id;

        // Status badge
        const statusClass = `status-${req.status}`;
        const statusText = req.status.charAt(0).toUpperCase() + req.status.slice(1);

        // Format timestamps
        const requestedTime = formatRelativeTime(req.requested_at_utc);
        const requestedFull = formatDateTime(req.requested_at_utc);

        let completedTime = '-';
        let completedFull = '';
        if (req.completed_at_utc) {
            completedTime = formatRelativeTime(req.completed_at_utc);
            completedFull = formatDateTime(req.completed_at_utc);
        }

        // Signal quality
        let signalDisplay = '-';
        if (req.status === 'completed' && (req.rx_snr !== null || req.rx_rssi !== null)) {
            const snrStr = req.rx_snr !== null ? `${req.rx_snr.toFixed(1)} dB` : 'N/A';
            const rssiStr = req.rx_rssi !== null ? `${req.rx_rssi} dBm` : 'N/A';
            const signalClass = getSignalClass(req.rx_rssi, req.rx_snr);
            signalDisplay = `<span class="${signalClass}">${snrStr} / ${rssiStr}</span>`;
        }

        // Relay info
        let relayDisplay = '-';
        if (req.relay_node_id && req.relay_node_name) {
            relayDisplay = `<span class="relay-info">${escapeHtml(req.relay_node_name)}</span>`;
        } else if (req.relay_node_id) {
            relayDisplay = `<span class="relay-info">${escapeHtml(req.relay_node_id)}</span>`;
        }

        // Hops
        const hopsDisplay = req.hops_away !== null ? req.hops_away : '-';

        return `
            <tr>
                <td>${req.id}</td>
                <td>
                    <a href="/nodes.html?node=${encodeURIComponent(req.to_node_id)}" class="node-link">
                        <strong>${escapeHtml(nodeName)}</strong>
                    </a>
                    <br><small>${escapeHtml(req.to_node_id)}</small>
                </td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
                <td title="${requestedFull}">${requestedTime}</td>
                <td title="${completedFull}">${completedTime}</td>
                <td>${signalDisplay}</td>
                <td>${relayDisplay}</td>
                <td class="center">${hopsDisplay}</td>
            </tr>
        `;
    }).join('');
}

// Update statistics
function updateStats(stats) {
    document.getElementById('stat-total').textContent = stats.total || 0;
    document.getElementById('stat-completed').textContent = stats.completed || 0;
    document.getElementById('stat-pending').textContent = stats.pending || 0;
    document.getElementById('stat-timeout').textContent = stats.timeout || 0;

    const successRate = stats.total > 0
        ? ((stats.completed / stats.total) * 100).toFixed(1)
        : '0';
    document.getElementById('stat-success-rate').textContent = successRate + '%';
}

// Get signal quality CSS class
function getSignalClass(rssi, snr) {
    if (rssi === null) return '';
    if (rssi > -110 && (snr === null || snr > 0)) return 'signal-good';
    if (rssi > -120 && (snr === null || snr > -5)) return 'signal-fair';
    return 'signal-poor';
}

// Format relative time
function formatRelativeTime(isoString) {
    if (!isoString) return '-';

    const utcString = isoString.endsWith('Z') ? isoString : isoString + 'Z';
    const date = new Date(utcString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffMins < 1) return 'Just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    if (diffHours < 24) return `${diffHours}h ago`;
    if (diffDays < 7) return `${diffDays}d ago`;
    return date.toLocaleDateString();
}

// Format date/time
function formatDateTime(isoString) {
    if (!isoString) return '';

    const utcString = isoString.endsWith('Z') ? isoString : isoString + 'Z';
    const date = new Date(utcString);
    return date.toLocaleString();
}

// Show error message
function showError(message) {
    const tbody = document.getElementById('telemetry-tbody');
    tbody.innerHTML = `<tr><td colspan="8" class="error">${escapeHtml(message)}</td></tr>`;
}

// Escape HTML
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Event listeners
document.getElementById('refresh-btn').addEventListener('click', loadTelemetryRequests);
document.getElementById('search').addEventListener('input', filterAndDisplay);
document.getElementById('status-filter').addEventListener('change', filterAndDisplay);
document.getElementById('limit-select').addEventListener('change', loadTelemetryRequests);

// Initial load
loadTelemetryRequests();

// Auto-refresh every 30 seconds
setInterval(loadTelemetryRequests, 30000);
