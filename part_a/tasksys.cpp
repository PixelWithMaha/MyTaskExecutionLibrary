#include "tasksys.h"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <algorithm>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() { 
    return "Serial"; 
}
TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {}
TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++)
        runnable->runTask(i, num_total_tasks);
}
TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads < 1 ? 1 : num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}
TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    if (num_total_tasks <= 0) return;
    int nthreads = std::min(num_threads_, num_total_tasks);

    std::atomic<int> next(0);
    auto body = [&]() {
        for (;;) {
            int id = next.fetch_add(1);
            if (id >= num_total_tasks) break;
            runnable->runTask(id, num_total_tasks);
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(nthreads - 1);
    for (int t = 1; t < nthreads; t++) threads.emplace_back(body);
    body();
    for (auto& th : threads) th.join();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads)
    : ITaskSystem(num_threads), runnable_(nullptr), total_(0), next_(0), done_(0), stop_(false) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    int n = num_threads < 1 ? 1 : num_threads;
    for (int i = 0; i < n - 1; i++)
        workers_.emplace_back(&TaskSystemParallelThreadPoolSpinning::workerLoop, this);
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    stop_.store(true);
    for (auto& th : workers_) th.join();
}

void TaskSystemParallelThreadPoolSpinning::workerLoop() {
    while (!stop_.load()) {
        if (next_.load() >= total_.load()) continue;
        IRunnable* r = nullptr;
        int id = -1, n = 0;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (next_ < total_) { id = next_++; r = runnable_; n = total_; }
        }
        if (id >= 0) {
            r->runTask(id, n);
            done_.fetch_add(1);
        }
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    if (num_total_tasks <= 0) return;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        runnable_ = runnable;
        total_ = num_total_tasks;
        next_ = 0;
        done_.store(0);
    }
    for (;;) {
        int id = -1;
        if (next_.load() < total_.load()) {
            std::lock_guard<std::mutex> lk(mtx_);
            if (next_ < total_) id = next_++;
        }
        if (id < 0) break;
        runnable->runTask(id, num_total_tasks);
        done_.fetch_add(1);
    }
    while (done_.load() < num_total_tasks) { }
    {
        std::lock_guard<std::mutex> lk(mtx_);
        total_ = 0; next_ = 0; runnable_ = nullptr;
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), nthreads_(num_threads < 1 ? 1 : num_threads),
      runnable_(nullptr), total_(0), next_(0), done_(0), stop_(false),
      gen_(0), finished_workers_(0), busy_ns_(nthreads_, 0) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    for (int i = 0; i < nthreads_; i++)
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerLoop, this, i);
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    {
        std::lock_guard<std::mutex> lk(mtx_);
        stop_ = true;
    }
    work_cv_.notify_all();
    for (auto& th : workers_) th.join();
#ifdef PROFILE
    {
        double mx = 0, sum = 0;
        for (int i = 0; i < nthreads_; i++) {
            double ms = busy_ns_[i] / 1e6;
            fprintf(stderr, "[PROFILE sleeping] thread %d busy %.2f ms\n", i, ms);
            mx = std::max(mx, ms); sum += ms;
        }
        double mean = sum / nthreads_;
        fprintf(stderr, "[PROFILE sleeping] tmax %.2f ms  tmean %.2f ms  R = %.3f\n",
                mx, mean, mean > 0 ? mx / mean : 0.0);
    }
#endif
}

#ifdef PROFILE
#define T_START() auto t0_ = std::chrono::steady_clock::now()
#define T_STOP()  busy_ns_[tid] += std::chrono::duration_cast<std::chrono::nanoseconds>( \
                      std::chrono::steady_clock::now() - t0_).count()
#else
#define T_START()
#define T_STOP()
#endif

#ifdef STATIC_ASSIGN
void TaskSystemParallelThreadPoolSleeping::workerLoop(int tid) {
    long long my_gen = 0;
    std::unique_lock<std::mutex> lk(mtx_);
    for (;;) {
        work_cv_.wait(lk, [&] { return stop_ || gen_ != my_gen; });
        if (stop_) return;
        my_gen = gen_;
        IRunnable* r = runnable_;
        int n = total_;
        lk.unlock();
        {
            T_START();
#ifdef STATIC_INTERLEAVED
            for (int id = tid; id < n; id += nthreads_) r->runTask(id, n);
#else
            int chunk = (n + nthreads_ - 1) / nthreads_;
            int begin = tid * chunk, end = std::min(n, begin + chunk);
            for (int id = begin; id < end; id++) r->runTask(id, n);
#endif
            T_STOP();
        }
        lk.lock();
        if (++finished_workers_ == nthreads_) done_cv_.notify_one();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    if (num_total_tasks <= 0) return;
    std::unique_lock<std::mutex> lk(mtx_);
    runnable_ = runnable;
    total_ = num_total_tasks;
    finished_workers_ = 0;
    gen_++;
    work_cv_.notify_all();
    done_cv_.wait(lk, [this] { return finished_workers_ == nthreads_; });
    runnable_ = nullptr;
}
#else
void TaskSystemParallelThreadPoolSleeping::workerLoop(int tid) {
    std::unique_lock<std::mutex> lk(mtx_);
    for (;;) {
        work_cv_.wait(lk, [this] { return stop_ || next_ < total_; });
        if (stop_) return;
        int id = next_++;
        IRunnable* r = runnable_;
        int n = total_;
        lk.unlock();
        {
            T_START();
            r->runTask(id, n);
            T_STOP();
        }
        lk.lock();
        if (++done_ == total_) done_cv_.notify_one();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    if (num_total_tasks <= 0) return;
    std::unique_lock<std::mutex> lk(mtx_);
    runnable_ = runnable;
    total_ = num_total_tasks;
    next_ = 0;
    done_ = 0;
    work_cv_.notify_all();
    done_cv_.wait(lk, [this] { return done_ == total_; });
    total_ = 0; next_ = 0; done_ = 0; runnable_ = nullptr;
}
#endif

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}