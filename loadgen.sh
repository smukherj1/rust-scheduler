#/usr/bin/bash

set -eu

cargo run --release --bin loadgen -- \
    -s "http://0.0.0.0:50051" -w 200 -b 2 \
    --build-dur-ms-lower-bound=5 \
    --build-dur-ms-upper-bound=10