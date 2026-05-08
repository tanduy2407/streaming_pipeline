#!/bin/sh

python main/normalized.py &
python main/sink_to_opensearch.py

wait