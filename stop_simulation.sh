#!/bin/bash

echo "Stopping all P2P nodes..."
pkill -f "src/main.py start-node"
echo "All nodes stopped."
