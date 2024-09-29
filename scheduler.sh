#/usr/bin/bash

set -eu
cargo run --bin scheduler -- -a "0.0.0.0:50051"