#include<iostream>
#include<future>
#include<mutex>
#include<condition_variable>
#include<atomic>
#include<vector>
#include<thread>
#include<queue>
class thread_pool
{
    private:
        
        std::atomic_bool done;
        std::vector<std::thread> threads;
        std::mutex mtx;
        std::queue<std::function<void()>> tasks;
        std::condition_variable cv;
        int num ;
    public:
        thread_pool(int n):done(false),num(n){
            for(int i=0;i<num;i++)
            {
                threads.emplace_back(  
                    [this](){
                        while (true)
                        {
                            std::function<void()> task;
                            {
                                std::unique_lock<std::mutex> lock(mtx);
                                cv.wait(lock,[this](){
                                    return done.load()||!tasks.empty();
                                });
                            
                                if(done.load()&&tasks.empty())
                                {
                                    return;
                                }
                                task = std::move(tasks.front());
                                tasks.pop();
                            }
                            task();
                        }
                        
                        
                    }
                );
            }
        }
        void commit(std::function<void()> task)
        {
            {
                std::unique_lock<std::mutex> lock(mtx);
                if(done.load())
                {
                    return ;
                }
                tasks.push(task);
            }
            cv.notify_one();
        }
        ~thread_pool()
        {
            done.store(true);
            cv.notify_all();
            for(std::thread &worker: threads)
            {
                if(worker.joinable())
                    {
                        worker.join();
                    }
            }
        }
};
int main()
{
    thread_pool pool(4); // 4个线程

    for (int i = 0; i < 100; ++i) {
        pool.commit([i]() {
            std::cout << "Task " << i << " is running on thread " 
                      << std::this_thread::get_id() << std::endl;
        });
    }

    return 0;
}