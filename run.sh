#!/bin/bash
export PYTHONUNBUFFERED=1

echo "Starting pipeline..."
python3 power_grid_pipeline.py &
PIPELINE_PID=$!

sleep 12

echo "Starting simulator..."
python3 simulate_stream.py --max-batches 20 --rows-per-batch 200 --interval 4 --anomaly-ratio 0.4

sleep 15

kill $PIPELINE_PID 2>/dev/null
wait $PIPELINE_PID 2>/dev/null

echo "Done."
