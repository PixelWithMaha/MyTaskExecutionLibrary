#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
================================================================
Serial task system implementation
================================================================
*/

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {}
TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
    return 0;
}

void TaskSystemSerial::sync() {
    return;
}

/*
================================================================
Parallel Task System Implementation
================================================================
*/

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {}
TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    return;
}

/*
================================================================
Parallel Thread Pool Spinning Task System Implementation
================================================================
*/

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {}
TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    return;
}

/*
================================================================
Parallel Thread Pool Sleeping Task System Implementation
================================================================
*/

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads)
    : ITaskSystem(num_threads), num_threads_(num_threads), stop_(false), next_launch_id_(0), active_tasks_count_(0) {
    for (int i = 0; i < num_threads_; ++i) {
        workers_.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerThread, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::unique_lock<std::mutex> lock(mutex_);
        stop_ = true;
    }
    cv_worker_.notify_all();
    for (std::thread &worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::workerThread() {
    while (true) {
        Launch* launch = nullptr;
        int task_id = -1;

        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_worker_.wait(lock, [this]() {
                return stop_ || !ready_queue_.empty();
            });

            if (stop_ && ready_queue_.empty()) {
                return;
            }

            launch = ready_queue_.front();
            task_id = launch->next_task_index++;

            if (launch->next_task_index == launch->num_total_tasks) {
                ready_queue_.pop();
            }
        }

        // Execute task strictly outside the lock
        launch->runnable->runTask(task_id, launch->num_total_tasks);

        {
            std::unique_lock<std::mutex> lock(mutex_);
            launch->remaining_tasks--;
            active_tasks_count_--;

            if (launch->remaining_tasks == 0) {
                TaskID completed_id = launch->id;
                completed_launches_.insert(completed_id);

                if (dependents_.count(completed_id)) {
                    for (TaskID dep_launch_id : dependents_[completed_id]) {
                        Launch* dep_launch = launches_[dep_launch_id];
                        dep_launch->unfinished_deps--;

                        if (dep_launch->unfinished_deps == 0) {
                            ready_queue_.push(dep_launch);
                            cv_worker_.notify_all();
                        }
                    }
                    dependents_.erase(completed_id);
                }

                delete launch;
                launches_.erase(completed_id);

                if (active_tasks_count_ == 0 && ready_queue_.empty()) {
                    cv_sync_.notify_all();
                }
            }
        }
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {

    std::unique_lock<std::mutex> lock(mutex_);

    TaskID launch_id = next_launch_id_++;
    Launch* launch = new Launch();
    launch->id = launch_id;
    launch->runnable = runnable;
    launch->num_total_tasks = num_total_tasks;
    launch->next_task_index = 0;
    launch->remaining_tasks = num_total_tasks;
    launch->unfinished_deps = 0;

    launches_[launch_id] = launch;

    std::unordered_set<TaskID> unique_deps(deps.begin(), deps.end());
    for (TaskID dep_id : unique_deps) {
        if (completed_launches_.find(dep_id) == completed_launches_.end()) {
            launch->unfinished_deps++;
            dependents_[dep_id].push_back(launch_id);
        }
    }

    active_tasks_count_ += num_total_tasks;

    if (launch->unfinished_deps == 0) {
        ready_queue_.push(launch);
        cv_worker_.notify_all();
    }

    return launch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_sync_.wait(lock, [this]() {
        return active_tasks_count_ == 0 && ready_queue_.empty();
    });
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    runAsyncWithDeps(runnable, num_total_tasks, {});
    sync();
}