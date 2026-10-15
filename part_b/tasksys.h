#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <atomic>



class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,const std::vector<TaskID>& deps);
        void sync();
};

class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks, const std::vector<TaskID>& deps);
        void sync();

    private:
       struct TaskLaunch {
        TaskID launch_id;
        IRunnable* runnable;
        int num_total_tasks;
        std::atomic<int> next_task_idx{0};
        std::atomic<int> tasks_completed{0};
        
        int remaining_deps{0};
        std::vector<TaskID> dependents; 
    };
    
    void workerThreadLoop();
       
        std::vector<std::thread> workers;
        bool shutdown{false};

    std::mutex mutex_;
    std::condition_variable cv_worker_; 
    std::condition_variable cv_sync_;   

    TaskID next_launch_id{0};
    int total_active_launches{0};      

    std::queue<TaskLaunch*> ready_queue;
    std::unordered_map<TaskID, TaskLaunch*> all_launches;
    std::unordered_set<TaskID> completed_launches;
        

};

#endif