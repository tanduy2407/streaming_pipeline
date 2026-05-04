#!/bin/bash
set -e

# Start JobManager in background
/docker-entrypoint.sh jobmanager &

# Wait until Flink REST API is ready
until curl -s http://localhost:8081 >/dev/null; do
  echo "Waiting for Flink cluster..."
  sleep 2
done

echo "Flink is ready. Submitting jobs..."

# Run both jobs in parallel
flink run -py /opt/flink/app/brute_force_detection.py &
flink run -py /opt/flink/app/window_agg.py &

# Wait for both jobs (keeps container alive)
wait