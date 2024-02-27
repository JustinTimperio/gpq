#include <iostream>
#include <queue>
#include <chrono>
#include <cstdlib>
#include <ctime>

int main() {
    std::priority_queue<int> pq;

    auto start = std::chrono::high_resolution_clock::now();

    // Push 10 million integers onto the priority queue
    for (int i = 0; i < 10000000; ++i) {
        std::srand(std::time(0)); // use current time as seed for random generator
        int p = std::rand() % 101; // get a random number between 0 and 100
        pq.push(p);
    }

    auto mid = std::chrono::high_resolution_clock::now();

    // Pop 10 million integers from the priority queue
    while (!pq.empty()) {
        pq.pop();
    }

    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> diff_insert = mid - start;
    std::chrono::duration<double> diff_remove = end - mid;

    std::cout << "Time to insert 10 million integers: " << diff_insert.count() << " s\n";
    std::cout << "Time to remove 10 million integers: " << diff_remove.count() << " s\n";
    std::cout << "Total time: " << (diff_insert + diff_remove).count() << " s\n";

    return 0;
}