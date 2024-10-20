#/usr/bin/bash

set -eu

cargo run --release --bin loadgen -- \
    -s "http://0.0.0.0:50051" -w 10 -b 2 \
    --build-dur-ms-lower-bound=500 \
    --build-dur-ms-upper-bound=5000