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
    : ITaskSystem(num_threads) {
    for (int i = 0; i < num_threads; ++i) {
        workers.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerThreadLoop, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        shutdown = true;
    }
    cv_worker_.notify_all();
    for (std::thread &worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::workerThreadLoop() {
   while (true) {
        TaskLaunch* current_launch = nullptr;
        int task_id_to_run = -1;

        {
            std::unique_lock<std::mutex> lock(mutex_);

            while (!shutdown) {
         
                while (!ready_queue.empty()) {
                    TaskLaunch* candidate = ready_queue.front();
                    int task_idx = candidate->next_task_idx.fetch_add(1);

                    if (task_idx < candidate->num_total_tasks) {
                        current_launch = candidate;
                        task_id_to_run = task_idx;
                        break;
                    } else {
                        
                        ready_queue.pop();
                    }
                }

                if (current_launch != nullptr) {
                    break; 
                }

                cv_worker_.wait(lock);
            }

            if (shutdown && current_launch == nullptr) {
                return;
            }
        }

        current_launch->runnable->runTask(task_id_to_run, current_launch->num_total_tasks);

        int finished_tasks = current_launch->tasks_completed.fetch_add(1) + 1;

        if (finished_tasks == current_launch->num_total_tasks) {
            std::lock_guard<std::mutex> lock(mutex_);

            TaskID finished_id = current_launch->launch_id;
            completed_launches.insert(finished_id);

            for (TaskID dep_id : current_launch->dependents) {
                auto it = all_launches.find(dep_id);
                if (it != all_launches.end()) {
                    TaskLaunch* dep_launch = it->second;
                    dep_launch->remaining_deps--;

                    if (dep_launch->remaining_deps == 0) {
                        ready_queue.push(dep_launch);
                        cv_worker_.notify_all();
                    }
                }
            }

            all_launches.erase(finished_id);
            delete current_launch;

            total_active_launches--;

            if (total_active_launches == 0) {
                cv_sync_.notify_all();
            }
        }
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(
    IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps) {
    
        std::lock_guard<std::mutex> lock(mutex_);

    TaskID launch_id = next_launch_id++;
    TaskLaunch* launch = new TaskLaunch();
    launch->launch_id = launch_id;
    launch->runnable = runnable;
    launch->num_total_tasks = num_total_tasks;

    int unfinished_deps = 0;
    for (TaskID dep : deps) {
        if (completed_launches.find(dep) == completed_launches.end()) {
            unfinished_deps++;
            all_launches[dep]->dependents.push_back(launch_id);
        }
    }

    launch->remaining_deps = unfinished_deps;
    all_launches[launch_id] = launch;
    total_active_launches++;

    if (unfinished_deps == 0) {
        ready_queue.push(launch);
        cv_worker_.notify_all();
    }

    return launch_id;
}

void TaskSystemParallelThreadPoolSleeping::sync() {
   std::unique_lock<std::mutex> lock(mutex_);
    cv_sync_.wait(lock, [this]() {
        return total_active_launches == 0;
    });
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    std::vector<TaskID> no_deps;
    runAsyncWithDeps(runnable, num_total_tasks, no_deps);
    sync();
}