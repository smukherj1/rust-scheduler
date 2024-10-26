#/usr/bin/bash

set -eu

cargo run --release --bin worker -- -s "http://0.0.0.0:50051" -w 200 \
    --prob-build-failure=10 \
    --prob-build-heartbeat-failure=10 \
    --prob-idle-failure=10