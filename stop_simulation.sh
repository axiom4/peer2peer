#!/bin/bash

echo "Stopping all P2P nodes..."
pkill -f "src/main.py start-node"
echo "Stopping Web UI..."
pkill -f "src/web_ui.py"
echo "All nodes stopped."
