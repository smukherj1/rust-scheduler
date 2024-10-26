#/usr/bin/bash

set -eu

cargo run --release --bin worker -- -s "http://0.0.0.0:50051" -w 200 \
    --prob-build-failure=0 \
    --prob-build-heartbeat-failure=0 \
    --prob-idle-failure=0