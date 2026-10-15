#include <cstdlib>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>
int main(int argc, char** argv) {

    int P = argc > 1 ? atoi(argv[1]) : (int)std::thread::hardware_concurrency();
    const int R = 1000;
    auto t0 = std::chrono::steady_clock::now();
    
    for (int r = 0; r < R; r++) {
        std::vector<std::thread> ts;
        for (int i = 0; i < P; i++) ts.emplace_back([] {});
        for (auto& t : ts) t.join();
    }
    
    auto t1 = std::chrono::steady_clock::now();
    printf("P=%d avg create+join per round: %.2f us\n", P,
           std::chrono::duration<double, std::micro>(t1 - t0).count() / R);

}