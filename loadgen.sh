#/usr/bin/bash

set -eu

cargo run --bin loadgen -- -s "http://0.0.0.0:50051" -w 3