#!/bin/bash
echo "Restarting network..."
./stop_simulation.sh
sleep 2
./start_simulation.sh
echo "Network restart complete."
