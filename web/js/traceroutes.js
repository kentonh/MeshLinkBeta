// Traceroutes page JavaScript

let allTraceroutes = [];
let currentSort = { key: 'received', direction: 'desc' };

// Load and display traceroutes
async function loadTraceroutes() {
    try {
        const limit = document.getElementById('limit-select').value;
        const response = await fetch(`/api/traceroutes?limit=${limit}`);
        const data = await response.json();

        if (data.success) {
            allTraceroutes = data.traceroutes;
            displayTraceroutes(allTraceroutes);
            updateStats();
        } else {
            showError('Failed to load traceroutes: ' + data.error);
        }
    } catch (error) {
        showError('Error loading traceroutes: ' + error.message);
    }
}

// Display traceroutes in table
function displayTraceroutes(traceroutes) {
    const tbody = document.getElementById('traceroutes-tbody');

    if (traceroutes.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="no-data">No traceroutes found</td></tr>';
        return;
    }

    // Sort traceroutes
    const dir = currentSort.direction === 'asc' ? 1 : -1;
    traceroutes.sort((a, b) => {
        let cmp;
        switch (currentSort.key) {
            case 'id':
                return (a.id - b.id) * dir;
            case 'from':
                cmp = (a.from_long_name || a.from_short_name || a.from_node_id).localeCompare(b.from_long_name || b.from_short_name || b.from_node_id);
                return cmp * dir;
            case 'to':
                cmp = (a.to_long_name || a.to_short_name || a.to_node_id || '').localeCompare(b.to_long_name || b.to_short_name || b.to_node_id || '');
                return cmp * dir;
            case 'hops':
                return (a.hop_count - b.hop_count) * dir;
            case 'signal': {
                const aSnr = (a.snr_data && a.snr_data.length > 0) ? a.snr_data.reduce((s, v) => s + v, 0) / a.snr_data.length : null;
                const bSnr = (b.snr_data && b.snr_data.length > 0) ? b.snr_data.reduce((s, v) => s + v, 0) / b.snr_data.length : null;
                if (aSnr === null && bSnr === null) return 0;
                if (aSnr === null) return 1;
                if (bSnr === null) return -1;
                return (aSnr - bSnr) * dir;
            }
            case 'received':
            default:
                return (new Date(a.received_at_utc || 0) - new Date(b.received_at_utc || 0)) * dir;
        }
    });

    tbody.innerHTML = traceroutes.map(trace => {
        const fromName = trace.from_long_name || trace.from_short_name || trace.from_node_id;
        const toName = trace.to_long_name || trace.to_short_name || trace.to_node_id || 'Unknown';

        // Format route using short names with links to node details
        const routeNames = trace.route_names || trace.route.map(id => id.slice(-4));
        const routePath = trace.route.map((nodeId, idx) => {
            const name = routeNames[idx] || nodeId.slice(-4);
            return `<a href="/nodes.html?node=${encodeURIComponent(nodeId)}" class="hop-link">${name}</a>`;
        }).join(' → ');

        // Format SNR data if available
        let signalQuality = 'N/A';
        if (trace.snr_data && trace.snr_data.length > 0) {
            const avgSnr = trace.snr_data.reduce((a, b) => a + b, 0) / trace.snr_data.length;
            const minSnr = Math.min(...trace.snr_data);
            const maxSnr = Math.max(...trace.snr_data);
            signalQuality = `Avg: ${avgSnr.toFixed(1)} dB (${minSnr.toFixed(1)} - ${maxSnr.toFixed(1)})`;
        }

        // Format timestamp (add Z suffix if missing to ensure UTC parsing)
        const utcString = trace.received_at_utc.endsWith('Z') ? trace.received_at_utc : trace.received_at_utc + 'Z';
        const receivedDate = new Date(utcString);
        const receivedTime = formatRelativeTime(receivedDate);

        return `
            <tr>
                <td><a href="/api/traceroutes/${trace.id}" target="_blank" class="trace-link">${trace.id}</a></td>
                <td><strong>${fromName}</strong><br><small>${trace.from_node_id}</small></td>
                <td><strong>${toName}</strong><br><small>${trace.to_node_id || 'N/A'}</small></td>
                <td class="center">${trace.hop_count === 0 ? 'Direct' : trace.hop_count + ' hop' + (trace.hop_count !== 1 ? 's' : '')}</td>
                <td class="route-path"><code>${routePath}</code></td>
                <td class="center">${signalQuality}</td>
                <td>${receivedTime}<br><small>${receivedDate.toLocaleString()}</small></td>
            </tr>
        `;
    }).join('');
    updateSortableHeaders();
}

// Update statistics
function updateStats() {
    document.getElementById('stat-total-traceroutes').textContent = allTraceroutes.length;

    // Count unique routes (based on from-to pairs)
    const uniqueRoutes = new Set(allTraceroutes.map(t => `${t.from_node_id}-${t.to_node_id}`));
    document.getElementById('stat-unique-routes').textContent = uniqueRoutes.size;

    // Calculate average hops
    if (allTraceroutes.length > 0) {
        const avgHops = allTraceroutes.reduce((sum, t) => sum + t.hop_count, 0) / allTraceroutes.length;
        document.getElementById('stat-avg-hops').textContent = avgHops.toFixed(1);
    } else {
        document.getElementById('stat-avg-hops').textContent = '0';
    }
}

// Search filter
function filterTraceroutes() {
    const searchTerm = document.getElementById('search').value.toLowerCase();

    const filtered = allTraceroutes.filter(trace => {
        const fromName = (trace.from_long_name || trace.from_short_name || '').toLowerCase();
        const toName = (trace.to_long_name || trace.to_short_name || '').toLowerCase();
        const fromId = trace.from_node_id.toLowerCase();
        const toId = (trace.to_node_id || '').toLowerCase();
        const route = trace.route.join(' ').toLowerCase();

        return fromName.includes(searchTerm) ||
               toName.includes(searchTerm) ||
               fromId.includes(searchTerm) ||
               toId.includes(searchTerm) ||
               route.includes(searchTerm);
    });

    displayTraceroutes(filtered);
}

// Format relative time
function formatRelativeTime(date) {
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

// Show error message
function showError(message) {
    const tbody = document.getElementById('traceroutes-tbody');
    tbody.innerHTML = `<tr><td colspan="7" class="error">${message}</td></tr>`;
}

// Sortable Headers
function initSortableHeaders() {
    document.querySelectorAll('#traceroutes-table th.sortable').forEach(th => {
        th.addEventListener('click', () => {
            const sortKey = th.dataset.sort;
            if (currentSort.key === sortKey) {
                currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.key = sortKey;
                currentSort.direction = (sortKey === 'from' || sortKey === 'to') ? 'asc' : 'desc';
            }
            filterTraceroutes();
        });
    });
}

function updateSortableHeaders() {
    document.querySelectorAll('#traceroutes-table th.sortable').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc');
        if (th.dataset.sort === currentSort.key) {
            th.classList.add(currentSort.direction === 'asc' ? 'sort-asc' : 'sort-desc');
        }
    });
}

// Event listeners
document.getElementById('refresh-btn').addEventListener('click', loadTraceroutes);
document.getElementById('search').addEventListener('input', filterTraceroutes);
document.getElementById('limit-select').addEventListener('change', loadTraceroutes);
initSortableHeaders();

// Initial load
loadTraceroutes();

// Auto-refresh every 30 seconds
setInterval(loadTraceroutes, 30000);
