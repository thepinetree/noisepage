#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "execution/execution_util.h"
#include "execution/vm/module.h"
#include "main/db_main.h"
#include "settings/settings_manager.h"
#include "tbb/global_control.h"
#include "test_util/tpch/workload.h"

namespace noisepage::tpch {
class TPCHBenchmark : public benchmark::Fixture {
 public:
  const bool print_exec_info_ = true;
  const double threshold_ = 0.1;
  const uint64_t min_iterations_per_query_ = 25;
  const uint64_t max_iterations_per_query_ = 25;
  const std::vector<uint8_t> threads_{1, 2, 4, 8, 16, 32, 40};
  const execution::vm::ExecutionMode mode_ = execution::vm::ExecutionMode::Interpret;

  std::unique_ptr<DBMain> db_main_;
  std::unique_ptr<tpch::Workload> tpch_workload_;

  const std::string tpch_table_root_ = "/home/dpatra/Data/NP-TPCH/SF1/";
  const std::string tpch_database_name_ = "tpch_db";

  void SetUp(const benchmark::State &state) final {
    execution::ExecutionUtil::InitTPL();

    // Set up database
    std::unordered_map<settings::Param, settings::ParamInfo> param_map;
    settings::SettingsManager::ConstructParamMap(param_map);
    auto db_main_builder = DBMain::Builder()
                               .SetUseGC(true)
                               .SetUseCatalog(true)
                               .SetUseGCThread(true)
                               .SetUseSettingsManager(true)
                               .SetSettingsParameterMap(std::move(param_map));
    db_main_ = db_main_builder.Build();

    // Set up metrics manager
    auto metrics_manager = db_main_->GetMetricsManager();
    metrics_manager->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE);
    metrics_manager->SetMetricSampleInterval(metrics::MetricsComponent::EXECUTION_PIPELINE, 0);

    // Load the TPCH tables and compile the queries
    tpch_workload_ = std::make_unique<tpch::Workload>(common::ManagedPointer<DBMain>(db_main_), tpch_database_name_,
                                                      tpch_table_root_, tpch::Workload::BenchmarkType::TPCH);
  }

  void TearDown(const benchmark::State &state) final {
    execution::ExecutionUtil::ShutdownTPL();
    // free db main here so we don't need to use the loggers anymore
    db_main_.reset();
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCHBenchmark, StabilizeBenchmark)(benchmark::State &state) {
  // Run benchmark for each query independently
  auto num_queries = tpch_workload_->GetQueryNum();

  for (auto _ : state) {
    // Overall totals
    uint64_t queries_run = 0, total_time = 0;
    for (auto thread_ct : threads_) {
      tbb::global_control c(tbb::global_control::max_allowed_parallelism, thread_ct);
      for (uint32_t i = 0; i < num_queries; i++) {
        // Single query running totals
        double old_avg = 0, avg = 0;
        double total = 0;
        uint64_t iterations = 0;
        // Iterate at least until min_iterations_per_query and at most until max_iterations_per_query and until average
        // stabilizes
        while ((iterations < min_iterations_per_query_) ||
               ((abs(avg - old_avg) > threshold_) && (iterations < max_iterations_per_query_))) {
          old_avg = avg;
          total += tpch_workload_->TimeQuery(i, mode_, thread_ct, print_exec_info_);
          iterations++;
          avg = total / iterations;
        }

        if (print_exec_info_) {
          std::cout << tpch_workload_->GetQueryName(i) << " took " << iterations
                    << " iterations with an average execution time of " << avg << std::endl;
        }

        queries_run += iterations;
        total_time += total;
      }
      state.SetIterationTime(total_time);
      state.SetItemsProcessed(queries_run);
    }
  }

  // Free the workload here so we don't need to use the loggers anymore
  tpch_workload_.reset();
}

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCHBenchmark, RuntimeBenchmark)(benchmark::State &state) {
  // Run benchmark for each query independently
  auto num_queries = tpch_workload_->GetQueryNum();

  for (auto _ : state) {
    // Overall totals
    uint64_t queries_run = 0, total_time = 0;
    for (auto thread_ct : threads_) {
      tbb::global_control c(tbb::global_control::max_allowed_parallelism, thread_ct);
      // Iterate to min_iterations_per_query
      for (uint32_t i = 0; i < num_queries; i++) {
        for (uint64_t iterations = 0; iterations < min_iterations_per_query_; iterations++) {
          total_time += tpch_workload_->TimeQuery(i, mode_, thread_ct, print_exec_info_);
          queries_run++;
        }
      }
    }
    state.SetIterationTime(total_time);
    state.SetItemsProcessed(queries_run);
  }

  // Free the workload here so we don't need to use the loggers anymore
  tpch_workload_.reset();
}

// BENCHMARK_REGISTER_F(TPCHBenchmark, StabilizeBenchmark)->Unit(benchmark::kMillisecond)->UseManualTime()->Iterations(1);
BENCHMARK_REGISTER_F(TPCHBenchmark, RuntimeBenchmark)->Unit(benchmark::kMillisecond)->UseManualTime()->Iterations(1);
}  // namespace noisepage::tpch