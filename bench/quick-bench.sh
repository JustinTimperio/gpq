#!/bin/bash

# Go
echo "Compiling and running Go program..."
go run ./go/bench.go
echo "" 

# Zig
echo "Compiling and running Zig program..."
zig run ./zig/bench.zig
echo "" 

# Rust
echo "Compiling and running Rust program..."
cd ./rust
cargo run
cd ..
echo "" 

# C++
echo "Compiling and running C++ program..."
g++ ./c++/bench.cpp 
./c++/a.out
echo "" 