#/usr/bin/bash

set -eu
cargo run --release --bin scheduler -- -a "0.0.0.0:50051" -m "0.0.0.0:50052"