#pragma once
#include "flow_event.hpp"
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>

namespace packetpipe {

/// Lock-free SPSC ring buffer with condition-variable-based blocking pop.
/// N must be a power of two. The producer and consumer must be distinct threads.
template<size_t N>
class EventQueue {
    static_assert(N > 0 && (N & (N - 1)) == 0, "N must be a power of two");

    std::array<FlowEvent, N> buffer_{};
    alignas(64) std::atomic<size_t> head_{0}; ///< consumer advances
    alignas(64) std::atomic<size_t> tail_{0}; ///< producer advances
    std::mutex              cv_mutex_;
    std::condition_variable cv_;

public:
    /// Producer side – returns false if the queue is full (no event stored).
    bool push(FlowEvent evt) noexcept {
        const size_t tail      = tail_.load(std::memory_order_relaxed);
        const size_t next_tail = (tail + 1) & (N - 1);
        if (next_tail == head_.load(std::memory_order_acquire)) {
            return false; // full
        }
        buffer_[tail] = std::move(evt);
        tail_.store(next_tail, std::memory_order_release);
        cv_.notify_one();
        return true;
    }

    /// Consumer side – non-blocking; returns nullopt if empty.
    std::optional<FlowEvent> pop() noexcept {
        const size_t head = head_.load(std::memory_order_relaxed);
        if (head == tail_.load(std::memory_order_acquire)) {
            return std::nullopt;
        }
        FlowEvent evt = std::move(buffer_[head]);
        head_.store((head + 1) & (N - 1), std::memory_order_release);
        return evt;
    }

    /// Consumer side – blocks up to `timeout` for a new item.
    std::optional<FlowEvent> pop_wait(std::chrono::milliseconds timeout) {
        if (auto evt = pop()) return evt;
        std::unique_lock<std::mutex> lk(cv_mutex_);
        cv_.wait_for(lk, timeout, [this] {
            return head_.load(std::memory_order_relaxed) !=
                   tail_.load(std::memory_order_acquire);
        });
        lk.unlock();
        return pop();
    }

    bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

    size_t size() const noexcept {
        const size_t h = head_.load(std::memory_order_acquire);
        const size_t t = tail_.load(std::memory_order_acquire);
        return (t - h + N) & (N - 1);
    }
};

constexpr size_t kDefaultEventQueueCapacity = 65536;
using DefaultEventQueue = EventQueue<kDefaultEventQueueCapacity>;

} // namespace packetpipe
