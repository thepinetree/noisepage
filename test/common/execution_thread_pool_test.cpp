#include <common/dedicated_thread_registry.h>
#include <common/execution_thread_pool.h>
#include <common/managed_pointer.h>
#include <common/shared_latch.h>
#include <common/spin_latch.h>

#include <atomic>
#include <thread>  // NOLINT
#include <vector>

#include "common/worker_pool.h"
#include "gtest/gtest.h"
#include "test_util/multithread_test_util.h"
#include "test_util/random_test_util.h"

#ifndef __APPLE__
#include <numa.h>
#include <numaif.h>
#endif

namespace terrier {

// Rather minimalistic checks for whether we reuse memory
// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, SimpleTest) {
  common::DedicatedThreadRegistry registry(DISABLED);
  const int num_threads = 1;
  std::vector<int> cpu_ids(num_threads);
  for (int i = 0; i < num_threads; i++) {
    cpu_ids[i] = i;
  }
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
  std::atomic<int> counter(0);

  int var1 = 1;

  std::promise<void> p;
  thread_pool.SubmitTask(&p, [&]() {
    var1++;
    counter.fetch_add(1);
  });

  // Wait for all the test to finish
  p.get_future().get();

  EXPECT_EQ(2, var1);
}

// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, BasicTest) {
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids(std::thread::hardware_concurrency());
  for (int i = 0; i < static_cast<int>(std::thread::hardware_concurrency()); i++) {
    cpu_ids[i] = i;
  }
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
  std::atomic<int> counter(0);

  int var1 = 1;
  int var2 = 2;
  int var3 = 3;
  int var4 = 4;
  int var5 = 5;
  std::promise<void> ps[5];
  thread_pool.SubmitTask(&ps[0], [&]() {
    var1++;
    counter.fetch_add(1);
  });
  thread_pool.SubmitTask(&ps[1], [&]() {
    var2--;
    counter.fetch_add(1);
  });
  thread_pool.SubmitTask(&ps[2], [&]() {
    var3 *= var3;
    counter.fetch_add(1);
  });
  thread_pool.SubmitTask(&ps[3], [&]() {
    var4 = var4 / var4;
    counter.fetch_add(1);
  });

  thread_pool.SubmitTask(&ps[4], [&]() {
    var5 = var5 / var5;
    counter.fetch_add(1);
  });

  for (auto &promise : ps) {
    promise.get_future().get();
  }

  EXPECT_EQ(2, var1);
  EXPECT_EQ(1, var2);
  EXPECT_EQ(9, var3);
  EXPECT_EQ(1, var4);
  EXPECT_EQ(1, var5);
}

// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, MoreTest) {
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids(std::thread::hardware_concurrency());
  for (int i = 0; i < static_cast<int>(std::thread::hardware_concurrency()); i++) {
    cpu_ids.emplace_back(i);
  }
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
  uint32_t iteration = 10;
  std::default_random_engine generator;
  std::uniform_int_distribution<uint32_t> num_thread{1, MultiThreadTestUtil::HardwareConcurrency()};
  for (uint32_t it = 0; it < iteration; it++) {
    auto workload = [] { std::this_thread::sleep_for(std::chrono::milliseconds(200)); };

    uint32_t num_threads_used = num_thread(generator);
    std::promise<void> promises[num_threads_used];
    for (auto &promise : promises) {
      thread_pool.SubmitTask(&promise, workload);
    }

    for (auto &promise : promises) {
      promise.get_future().get();
    }
  }
}

// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, NUMACorrectnessTest) {
  common::DedicatedThreadRegistry registry(DISABLED);
  int iteration = 10, num_threads = static_cast<int>(std::thread::hardware_concurrency());
  for (int _ = 0; _ < iteration; _++) {
    for (int cpu = 0; cpu < num_threads; cpu++) {
      std::vector<int> cpu_ids({cpu});
      common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry),
                                              &cpu_ids);
      std::promise<void> p;
#ifdef __APPLE__
      common::numa_region_t numa_hint UNUSED_ATTRIBUTE = common::UNSUPPORTED_NUMA_REGION;
#else
      common::numa_region_t numa_hint UNUSED_ATTRIBUTE =
          numa_available() == -1 ? common::UNSUPPORTED_NUMA_REGION
                                 : static_cast<common::numa_region_t>(static_cast<int16_t>(numa_node_of_cpu(cpu)));
#endif

      thread_pool.SubmitTask(&p, [&] {
#ifndef __APPLE__
        cpu_set_t mask;
        int result UNUSED_ATTRIBUTE = sched_getaffinity(0, sizeof(cpu_set_t), &mask);
        TERRIER_ASSERT(result == 0, "sched_getaffinity should succeed");

        uint32_t num_set = 0;
        for (uint32_t cpu_id = 0; cpu_id < sizeof(cpu_set_t) * 8; cpu_id++) {
          if (CPU_ISSET(cpu_id, &mask)) {
            TERRIER_ASSERT(
                numa_available() == -1 || static_cast<common::numa_region_t>(numa_node_of_cpu(cpu_id)) == numa_hint,
                "workload should be running on cpu on numa_hint's region");
            TERRIER_ASSERT(cpu_id == static_cast<uint32_t>(cpu), "should be running on CPU passed into thread pool");
            num_set++;
          }
        }
        TERRIER_ASSERT(num_set == 1, "should only have 1 CPU set in mask");

#endif
      });

      p.get_future().get();
    }
  }
}

// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, TaskStealingCorrectnessTest) {
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids;
  cpu_ids.emplace_back(0);
  uint32_t iteration = 10, num_threads = 1;
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
  for (uint32_t it = 0; it < iteration; it++) {
    std::atomic<uint32_t> flag1 = 0;
    auto stall_on_flag = [&] {
      flag1++;
      while (flag1 != 0) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    };

    std::promise<void> stall_promises[num_threads];
    for (uint32_t i = 0; i < num_threads; i++) {
      thread_pool.SubmitTask(&stall_promises[i], stall_on_flag);
    }

    // make sure that all threads are stalled
    while (flag1 != num_threads) std::this_thread::sleep_for(std::chrono::milliseconds(50));

#ifdef __APPLE__
    int16_t num_numa_regions = 1;
#else
    int16_t num_numa_regions = numa_available() < 0 || numa_max_node() < 0 ? 1 : numa_max_node();
#endif

    std::atomic<int16_t> order_count = 0;
    tbb::spin_mutex order_latch;
    std::map<int16_t, common::numa_region_t> numa_order;
    std::promise<void> check_promises[num_numa_regions];
    for (int16_t i = 0; i < num_numa_regions; i++) {
      auto numa_hint UNUSED_ATTRIBUTE = static_cast<common::numa_region_t>(i);
      auto workload = [&, numa_hint] {
        tbb::spin_mutex::scoped_lock l(order_latch);
        int16_t pos = order_count++;
        TERRIER_ASSERT(numa_order.find(pos) == numa_order.end(), "there should be no other node at this position");
        numa_order[pos] = numa_hint;
      };
      thread_pool.SubmitTask(&check_promises[i], workload, numa_hint);
    }

    // un-stall all tasks and make sure they finish
    flag1 = 0;
    for (auto &promise : stall_promises) {
      promise.get_future().get();
    }

    // wait for checking tasks to finish
    for (auto &promise : check_promises) {
      promise.get_future().get();
    }
    TERRIER_ASSERT(static_cast<int16_t>(numa_order.size()) == num_numa_regions,
                   "we should have as many nodes as hints");
    for (auto numa_pair UNUSED_ATTRIBUTE : numa_order) {
      TERRIER_ASSERT(numa_pair.first == static_cast<int16_t>(numa_pair.second),
                     "thread should take from queues starting at its region and continuing mod number of nodes");
    }
  }
}

// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, ContestSwitchingTestSpinLatch) {
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids;
  cpu_ids.emplace_back(0);
  uint32_t iteration = 10, num_threads = 1;
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
  for (uint32_t it = 0; it < iteration; it++) {
    std::atomic<uint32_t> flag1 = 0, flag2 = 0, flag3 = 0;
    auto stall_on_flag = [&] {
      flag1++;
      while (flag1 != 0) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    };

    std::promise<void> stall_promise;
    thread_pool.SubmitTask(&stall_promise, stall_on_flag);

    while (flag1 != num_threads) std::this_thread::sleep_for(std::chrono::milliseconds(50));

    common::SpinLatch l;
    auto get_stuck_on_latch = [&](common::PoolContext *ctx) {
      flag2++;
      common::SpinLatch::ScopedSpinLatch guard(&l, ctx);
      flag3++;
    };

    auto should_run_strait_through = [&] {
      EXPECT_EQ(flag2, 1);
      EXPECT_EQ(flag3, 0);
    };

    l.Lock();

    std::promise<void> stuck_promise, run_strait_promise;
    thread_pool.SubmitTask(&stuck_promise, get_stuck_on_latch);
    thread_pool.SubmitTask(&run_strait_promise, should_run_strait_through);

    flag1 = 0;
    stall_promise.get_future().get();
    run_strait_promise.get_future().get();

    EXPECT_EQ(flag3, 0);
    l.Unlock();
    stuck_promise.get_future().get();
    EXPECT_EQ(flag3, 1);
  }
}

// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, ContestSwitchingTestSharedLatchShared) {
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids;
  cpu_ids.emplace_back(0);
  uint32_t iteration = 10, num_threads = 1;
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
  for (uint32_t it = 0; it < iteration; it++) {
    std::atomic<uint32_t> flag1 = 0, flag2 = 0, flag3 = 0;
    auto stall_on_flag = [&] {
      flag1++;
      while (flag1 != 0) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    };

    std::promise<void> stall_promise;
    thread_pool.SubmitTask(&stall_promise, stall_on_flag);

    while (flag1 != num_threads) std::this_thread::sleep_for(std::chrono::milliseconds(50));

    common::SharedLatch l;
    auto get_stuck_on_latch = [&](common::PoolContext *ctx) {
      flag2++;
      common::SharedLatch::ScopedSharedLatch guard(&l, ctx);
      flag3++;
    };

    auto should_run_strait_through = [&] {
      EXPECT_EQ(flag2, 1);
      EXPECT_EQ(flag3, 0);
    };

    l.LockExclusive();

    std::promise<void> stuck_promise, run_strait_promise;
    thread_pool.SubmitTask(&stuck_promise, get_stuck_on_latch);
    thread_pool.SubmitTask(&run_strait_promise, should_run_strait_through);

    flag1 = 0;
    stall_promise.get_future().get();
    run_strait_promise.get_future().get();

    EXPECT_EQ(flag3, 0);
    l.Unlock();
    stuck_promise.get_future().get();
    EXPECT_EQ(flag3, 1);
  }
}

// NOLINTNEXTLINE
TEST(ExecutionThreadPoolTests, ContestSwitchingTestSharedLatchExclusive) {
  common::DedicatedThreadRegistry registry(DISABLED);
  std::vector<int> cpu_ids;
  cpu_ids.emplace_back(0);
  uint32_t iteration = 10, num_threads = 1;
  common::ExecutionThreadPool thread_pool(common::ManagedPointer<common::DedicatedThreadRegistry>(&registry), &cpu_ids);
  for (uint32_t it = 0; it < iteration; it++) {
    std::atomic<uint32_t> flag1 = 0, flag2 = 0, flag3 = 0;
    auto stall_on_flag = [&] {
      flag1++;
      while (flag1 != 0) std::this_thread::sleep_for(std::chrono::milliseconds(50));
    };

    std::promise<void> stall_promise;
    thread_pool.SubmitTask(&stall_promise, stall_on_flag);

    while (flag1 != num_threads) std::this_thread::sleep_for(std::chrono::milliseconds(50));

    common::SharedLatch l;
    auto get_stuck_on_latch = [&](common::PoolContext *ctx) {
      flag2++;
      common::SharedLatch::ScopedExclusiveLatch guard(&l, ctx);
      flag3++;
    };

    auto should_run_strait_through = [&] {
      EXPECT_EQ(flag2, 1);
      EXPECT_EQ(flag3, 0);
    };

    l.LockExclusive();

    std::promise<void> stuck_promise, run_strait_promise;
    thread_pool.SubmitTask(&stuck_promise, get_stuck_on_latch);
    thread_pool.SubmitTask(&run_strait_promise, should_run_strait_through);

    flag1 = 0;
    stall_promise.get_future().get();
    run_strait_promise.get_future().get();

    EXPECT_EQ(flag3, 0);
    l.Unlock();
    stuck_promise.get_future().get();
    EXPECT_EQ(flag3, 1);
  }
}

}  // namespace terrier
