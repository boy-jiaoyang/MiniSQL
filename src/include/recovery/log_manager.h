#ifndef MINISQL_LOG_MANAGER_H
#define MINISQL_LOG_MANAGER_H

#include <condition_variable>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "log_rec.h"  // ȷ��������ȷ��ͷ�ļ�

/**
 * LogManager maintains a separate thread that is awakened whenever the
 * log buffer is full or whenever a timeout happens.
 * When the thread is awakened, the log buffer's content is written into the disk log file.
 *
 * Implemented by student self
 */
using LogRecPtr = std::shared_ptr<LogRec>;

class LogManager {
public:
    LogManager(const std::string &log_file);
    ~LogManager();

    void AppendLogRecord(LogRecPtr log_rec);
    void RunFlushThread();
    void StopFlushThread();

private:
    void Flush();

    std::ofstream log_file_;
    std::mutex latch_;
    std::condition_variable cv_;
    std::thread flush_thread_;
    bool stop_thread_;
    std::vector<LogRecPtr> log_buffer_;
    size_t buffer_limit_;
    std::chrono::milliseconds timeout_;
};

#endif  // MINISQL_LOG_MANAGER_H

