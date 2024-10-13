#/usr/bin/bash

set -eu

cargo run --release --bin worker -- -s "http://0.0.0.0:50051" -w 10