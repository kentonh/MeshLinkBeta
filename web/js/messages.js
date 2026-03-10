// Messages page JavaScript

let allMessages = [];
let currentSort = { key: 'time', direction: 'desc' };

// Load and display messages
async function loadMessages() {
    try {
        const limit = document.getElementById('limit-select').value;
        const response = await fetch(`/api/messages?limit=${limit}`);
        const data = await response.json();

        if (data.success) {
            allMessages = data.messages;
            filterMessages();
            updateStats();
        } else {
            showError('Failed to load messages: ' + data.error);
        }
    } catch (error) {
        showError('Error loading messages: ' + error.message);
    }
}

// Display messages in table
function displayMessages(messages) {
    const tbody = document.getElementById('messages-tbody');

    if (messages.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="no-data">No messages found</td></tr>';
        return;
    }

    // Sort messages
    const dir = currentSort.direction === 'asc' ? 1 : -1;
    messages.sort((a, b) => {
        switch (currentSort.key) {
            case 'sender': {
                const aName = (a.sender_long_name || a.sender_short_name || a.node_id).toLowerCase();
                const bName = (b.sender_long_name || b.sender_short_name || b.node_id).toLowerCase();
                return aName.localeCompare(bName) * dir;
            }
            case 'channel':
                return ((a.channel_index || 0) - (b.channel_index || 0)) * dir;
            case 'hops':
                return ((a.hops_away || 0) - (b.hops_away || 0)) * dir;
            case 'time':
            default:
                return (new Date(a.received_at_utc || 0) - new Date(b.received_at_utc || 0)) * dir;
        }
    });

    tbody.innerHTML = messages.map(msg => {
        const senderName = msg.sender_long_name || msg.sender_short_name || msg.node_id;
        const shortId = msg.node_id ? msg.node_id.slice(-4) : '';

        // Format timestamp
        const utcString = msg.received_at_utc.endsWith('Z') ? msg.received_at_utc : msg.received_at_utc + 'Z';
        const date = new Date(utcString);
        const relTime = formatRelativeTime(date);

        // Hops display
        const hops = msg.hops_away !== null && msg.hops_away !== undefined
            ? (msg.hops_away === 0 ? 'Direct' : msg.hops_away)
            : '?';

        // Signal info
        let signal = '';
        if (msg.rx_snr !== null && msg.rx_snr !== undefined) {
            signal = `${msg.rx_snr.toFixed(1)} dB`;
        }
        if (msg.rx_rssi !== null && msg.rx_rssi !== undefined) {
            signal += (signal ? ' / ' : '') + `${msg.rx_rssi} dBm`;
        }
        if (!signal) signal = 'N/A';

        // Escape HTML in message text
        const escapedText = escapeHtml(msg.message_text);

        // Build reactions display
        let reactionsHtml = '';
        if (msg.reactions && msg.reactions.length > 0) {
            const reactionPills = msg.reactions.map(r =>
                `<span class="reaction-pill" title="${escapeHtml(r.from)}">${r.emoji}</span>`
            ).join('');
            reactionsHtml = `<div class="reactions-row">${reactionPills}</div>`;
        }

        return `
            <tr>
                <td>${relTime}<br><small>${date.toLocaleString()}</small></td>
                <td><a href="/nodes.html?node=${encodeURIComponent(msg.node_id)}" class="hop-link"><strong>${escapeHtml(senderName)}</strong></a><br><small>${shortId}</small></td>
                <td class="message-text">${escapedText}${reactionsHtml}</td>
                <td class="center">${msg.channel_index !== null ? msg.channel_index : '?'}</td>
                <td class="center">${hops}</td>
                <td class="center"><small>${signal}</small></td>
            </tr>
        `;
    }).join('');
    updateSortableHeaders();
}

// Update statistics
function updateStats() {
    document.getElementById('stat-total-messages').textContent = allMessages.length;
    const uniqueSenders = new Set(allMessages.map(m => m.node_id));
    document.getElementById('stat-unique-senders').textContent = uniqueSenders.size;
}

// Filter messages by search and channel
function filterMessages() {
    const searchTerm = document.getElementById('search').value.toLowerCase();
    const channelFilter = document.getElementById('channel-filter').value;

    const filtered = allMessages.filter(msg => {
        // Channel filter
        if (channelFilter !== '' && String(msg.channel_index) !== channelFilter) {
            return false;
        }

        // Search filter
        if (searchTerm) {
            const senderName = (msg.sender_long_name || msg.sender_short_name || '').toLowerCase();
            const nodeId = (msg.node_id || '').toLowerCase();
            const text = (msg.message_text || '').toLowerCase();

            return senderName.includes(searchTerm) ||
                   nodeId.includes(searchTerm) ||
                   text.includes(searchTerm);
        }

        return true;
    });

    displayMessages(filtered);
}

// Escape HTML to prevent XSS
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
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
    const tbody = document.getElementById('messages-tbody');
    tbody.innerHTML = `<tr><td colspan="6" class="error">${message}</td></tr>`;
}

// Sortable Headers
function initSortableHeaders() {
    document.querySelectorAll('#messages-table th.sortable').forEach(th => {
        th.addEventListener('click', () => {
            const sortKey = th.dataset.sort;
            if (currentSort.key === sortKey) {
                currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
            } else {
                currentSort.key = sortKey;
                currentSort.direction = sortKey === 'sender' ? 'asc' : 'desc';
            }
            filterMessages();
        });
    });
}

function updateSortableHeaders() {
    document.querySelectorAll('#messages-table th.sortable').forEach(th => {
        th.classList.remove('sort-asc', 'sort-desc');
        if (th.dataset.sort === currentSort.key) {
            th.classList.add(currentSort.direction === 'asc' ? 'sort-asc' : 'sort-desc');
        }
    });
}

// Event listeners
document.getElementById('refresh-btn').addEventListener('click', loadMessages);
document.getElementById('search').addEventListener('input', filterMessages);
document.getElementById('limit-select').addEventListener('change', loadMessages);
document.getElementById('channel-filter').addEventListener('change', filterMessages);
initSortableHeaders();

// Initial load
loadMessages();

// Auto-refresh every 30 seconds
setInterval(loadMessages, 30000);
