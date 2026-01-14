#!/bin/bash

# Configuration
START_PORT=8000
NUM_NODES=10
HOST="0.0.0.0"
LOG_DIR="logs"

if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
fi

echo "Starting $NUM_NODES P2P nodes..."

# Detect Python interpreter
PYTHON_CMD="python3"
if [ -f "venv/bin/python" ]; then
    PYTHON_CMD="venv/bin/python"
elif [ -f ".venv/bin/python" ]; then
    PYTHON_CMD=".venv/bin/python"
fi

echo "Using Python: $PYTHON_CMD"

for (( i=0; i<NUM_NODES; i++ ))
do
    PORT=$((START_PORT + i))
    DIR="network_data/node_data_$i"
    
    # Create data directory if it doesn't exist
    if [ ! -d "$DIR" ]; then
        mkdir -p "$DIR"
    fi
    
    echo "Starting Node $i on http://$HOST:$PORT (Data: $DIR)"
    
    # Start the node in background using nohup to persist after script exit
    # Redirect logs to a log file for each node in logs directory
    nohup $PYTHON_CMD src/main.py start-node --port $PORT --storage-dir $DIR --host $HOST > "$LOG_DIR/node_$i.log" 2>&1 &
done

echo "Starting Web UI on port 8888..."
if [ ! -d "network_data/web_ui_storage" ]; then
    mkdir -p "network_data/web_ui_storage"
fi
nohup $PYTHON_CMD src/main.py web-ui --port 8888 --storage-dir network_data/web_ui_storage > "$LOG_DIR/web_ui.log" 2>&1 &
echo "Web UI started at http://localhost:8888"

echo "Done. Use 'stop_simulation.sh' to stop all nodes."
