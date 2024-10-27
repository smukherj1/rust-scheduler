#/usr/bin/bash

set -eu

cargo run --release --bin worker -- -s "http://0.0.0.0:50051" -w 200 \
    --prob-build-failure=2 \
    --prob-build-heartbeat-failure=5 \
    --prob-idle-failure=5