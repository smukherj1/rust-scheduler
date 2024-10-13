#/usr/bin/bash

set -eu
cargo run --release --bin scheduler -- -a "0.0.0.0:50051"