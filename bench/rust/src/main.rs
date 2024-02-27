use std::collections::BinaryHeap;
use std::time::Instant;
use rand::Rng;

fn main() {
    let mut heap = BinaryHeap::new();

    let start = Instant::now();
    let mut sent = 0;
    let mut received = 0;

    // Push 10 million integers onto the heap
    for i in 0..10_000_000 {
        let mut rng = rand::thread_rng();
        let p = rng.gen_range(1..101);
        heap.push(p);
        sent += 1;
    }

    let mid = Instant::now();

    // Pop 10 million integers from the heap
    while let Some(_top) = heap.pop() {
        received += 1;
    }

    let end = Instant::now();

    println!("Sent: {} Received {}", sent, received);
    println!("Time to insert 10 million integers: {:?}", mid.duration_since(start));
    println!("Time to remove 10 million integers: {:?}", end.duration_since(mid));
    println!("Total time: {:?}", end.duration_since(start));
}