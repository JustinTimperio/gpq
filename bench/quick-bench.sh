#!/bin/bash

# Go
echo "==============================" 
echo "Compiling and running Go program..."
go run ./go/bench.go
echo "" 

# Zig
echo "==============================" 
echo "Compiling and running Zig program..."
zig run ./zig/bench.zig
echo "" 

# Rust
echo "==============================" 
echo "Compiling and running Rust program..."
cd ./rust
cargo run
cd ..
echo "" 

# C++
echo "==============================" 
echo "Compiling and running C++ program..."
cd ./c++
g++ bench.cpp 
./a.out
cd ..