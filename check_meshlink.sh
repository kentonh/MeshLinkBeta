#!/bin/bash

# MeshLink Monitor Script
# Run via cron to ensure MeshLink stays running
# Example cron entry (every 5 minutes):
# */5 * * * * /path/to/MeshLinkBeta/check_meshlink.sh >> /path/to/MeshLinkBeta/meshlink_cron.log 2>&1

# Configuration - adjust these paths as needed
MESHLINK_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$MESHLINK_DIR/venv"
MAIN_SCRIPT="$MESHLINK_DIR/main.py"
LOG_FILE="$MESHLINK_DIR/meshlink_cron.log"
PID_FILE="$MESHLINK_DIR/meshlink.pid"

# Log with timestamp
log_msg() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Check if MeshLink is already running
is_running() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if ps -p "$PID" > /dev/null 2>&1; then
            # Verify it's actually the python process running main.py
            if ps -p "$PID" -o args= | grep -q "main.py"; then
                return 0
            fi
        fi
        # PID file exists but process not running, clean up
        rm -f "$PID_FILE"
    fi
    
    # Also check by process name as fallback - look for python main.py processes
    # and verify they're running from our directory
    for pid in $(pgrep -f "python.*main\.py" 2>/dev/null); do
        # Check if working directory matches (Linux)
        if [ -d "/proc/$pid/cwd" ]; then
            proc_cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null)
            if [ "$proc_cwd" = "$MESHLINK_DIR" ]; then
                return 0
            fi
        else
            # macOS/BSD fallback: use lsof to find working directory
            proc_cwd=$(lsof -p "$pid" 2>/dev/null | grep cwd | awk '{print $NF}')
            if [ "$proc_cwd" = "$MESHLINK_DIR" ]; then
                return 0
            fi
        fi
    done
    
    # Simple fallback: just check if any python main.py is running
    # (less strict but catches most cases)
    if pgrep -f "python main\.py$" > /dev/null 2>&1; then
        return 0
    fi
    
    return 1
}

# Start MeshLink
start_meshlink() {
    log_msg "Starting MeshLink..."
    
    # Check if venv exists
    if [ ! -d "$VENV_DIR" ]; then
        log_msg "ERROR: Virtual environment not found at $VENV_DIR"
        log_msg "Create it with: python3 -m venv $VENV_DIR"
        exit 1
    fi
    
    # Check if main.py exists
    if [ ! -f "$MAIN_SCRIPT" ]; then
        log_msg "ERROR: main.py not found at $MAIN_SCRIPT"
        exit 1
    fi
    
    # Activate venv and start MeshLink in background
    cd "$MESHLINK_DIR"
    source "$VENV_DIR/bin/activate"
    
    nohup python main.py >> "$LOG_FILE" 2>&1 &
    MESHLINK_PID=$!
    
    echo "$MESHLINK_PID" > "$PID_FILE"
    log_msg "MeshLink started with PID $MESHLINK_PID"
    
    # Verify it started successfully
    sleep 2
    if is_running; then
        log_msg "MeshLink is now running"
    else
        log_msg "ERROR: MeshLink failed to start"
        rm -f "$PID_FILE"
        exit 1
    fi
}

# Main logic
main() {
    if is_running; then
        log_msg "MeshLink is already running on PID $(cat $PID_FILE)"
        exit 0
    else
        log_msg "MeshLink is not running"
        start_meshlink
    fi
}

main