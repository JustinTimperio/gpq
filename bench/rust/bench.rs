use std::collections::BinaryHeap;
use std::time::Instant;

fn main() {
    let mut heap = BinaryHeap::new();

    let start = Instant::now();

    // Push 10 million integers onto the heap
    for i in 0..10_000_000 {
        heap.push(i);
    }

    let mid = Instant::now();

    // Pop 10 million integers from the heap
    while let Some(_top) = heap.pop() {}

    let end = Instant::now();

    println!("Time to insert 10 million integers: {:?}", mid.duration_since(start));
    println!("Time to remove 10 million integers: {:?}", end.duration_since(mid));
}