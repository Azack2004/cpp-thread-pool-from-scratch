#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <atomic>
#include <chrono>
class ThreadPool {
public:
    explicit ThreadPool(size_t threadSize) : stop_(false) {
        for (size_t i = 0; i < threadSize; ++i) {
            workers_.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        // 1. 获取锁
                        std::unique_lock<std::mutex> lock(this->queueMutex_);
                        
                        // 2. 等待任务或者停止信号
                        this->cv_.wait(lock, [this] {
                            return this->stop_ || !this->tasks_.empty();
                        });

                        // 3. 如果收到停止信号且队列为空，退出线程
                        if (this->stop_ && this->tasks_.empty()) {
                            return;
                        }

                        // 4. 从队列中取出任务
                        task = std::move(this->tasks_.front());
                        this->tasks_.pop();
                    }
                    // 5. 执行任务（释放锁之后再执行，避免阻塞其他线程取任务）
                    task();
                }
            });
        }
    }

    // 提交任务接口（支持返回值，使用 std::future）
    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args) 
        -> std::future<typename std::invoke_result<F, Args...>::type> {
        
        using ReturnType = typename std::invoke_result<F, Args...>::type;

        // 将任务包装为 packaged_task
        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );
        
        std::future<ReturnType> res = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            
            // 不允许在停止状态的线程池中提交新任务
            if (stop_) {
                throw std::runtime_error("enqueue on stopped ThreadPool");
            }

            tasks_.emplace([task]() { (*task)(); });
        }
        
        // 唤醒一个等待的工作线程
        cv_.notify_one();
        return res;
    }

    // 析构函数：优雅关闭
    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queueMutex_);
            stop_ = true;
        }
        
        // 唤醒所有工作线程，让它们退出
        cv_.notify_all();
        
        // 回收所有线程
        for (std::thread &worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

private:
    // 工作线程列表
    std::vector<std::thread> workers_;
    // 任务队列
    std::queue<std::function<void()>> tasks_;
    
    // 同步原语
    std::mutex queueMutex_;
    std::condition_variable cv_;
    
    // 停止标志
    std::atomic<bool> stop_;
};


int main() {
    // 创建包含 4 个线程的线程池
    ThreadPool pool(4);

    // 提交任务并获取返回值
    std::vector<std::future<int>> results;
    for (int i = 0; i < 8; ++i) {
        results.emplace_back(
            pool.enqueue([i] {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                return i * i;
            })
        );
    }

    // 获取并打印结果
    for (auto &&result : results) {
        std::cout << "Result: " << result.get() << std::endl;
    }

    return 0;
}
