#include "benchmark/benchmark.h"
#include "common/scoped_timer.h"
#include "common/worker_pool.h"
#include "execution/execution_util.h"
#include "execution/vm/module.h"
#include "main/db_main.h"
#include "test_util/tpch/workload.h"
#include <tbb/task_scheduler_init.h>

tbb::task_scheduler_init s(1);

namespace terrier::tpch {
class TPCHBenchmark : public benchmark::Fixture {
 public:
  const double threshold_ = 0.1;
  const uint64_t min_iterations_ = 100;
  const uint64_t max_iterations_ = 5000;
  const execution::vm::ExecutionMode mode_ = execution::vm::ExecutionMode::Interpret;

  std::unique_ptr<DBMain> db_main_;
  std::unique_ptr<tpch::Workload> tpch_workload_;

  const std::string tpch_table_root_ = "../../../tpl_tables/tables/";
  const std::string tpch_database_name_ = "tpch_db";

  void SetUp(const benchmark::State &state) final {
    terrier::execution::ExecutionUtil::InitTPL();

    // Set up database
    auto db_main_builder = DBMain::Builder()
        .SetUseGC(true)
        .SetUseCatalog(true)
        .SetUseGCThread(true)
        .SetUseMetrics(true)
        .SetUseMetricsThread(true)
        .SetBlockStoreSize(1000000)
        .SetBlockStoreReuse(1000000)
        .SetRecordBufferSegmentSize(1000000)
        .SetRecordBufferSegmentReuse(1000000);
    db_main_ = db_main_builder.Build();

    // Set up metrics manager
    auto metrics_manager = db_main_->GetMetricsManager();
    metrics_manager->EnableMetric(metrics::MetricsComponent::EXECUTION_PIPELINE, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::GARBAGECOLLECTION, 0);
    metrics_manager->EnableMetric(metrics::MetricsComponent::LOGGING, 0);

    // Load the TPCH tables and compile the queries
    tpch_workload_ =
        std::make_unique<tpch::Workload>(common::ManagedPointer<DBMain>(db_main_), tpch_database_name_, tpch_table_root_);
  }

  void TearDown(const benchmark::State &state) final {
    terrier::execution::ExecutionUtil::ShutdownTPL();
    // free db main here so we don't need to use the loggers anymore
    db_main_.reset();
  }
};

// NOLINTNEXTLINE
BENCHMARK_DEFINE_F(TPCHBenchmark, StabilizeBenchmark)(benchmark::State &state) {
  // Run benchmark for each query independently
  auto num_queries = tpch_workload_->GetQueryNum();
  auto max_iters_per_query = max_iterations_/num_queries;
  // Register to the metrics manager
  db_main_->GetMetricsManager()->RegisterThread();

  for (uint32_t i = 0; i < num_queries; i++) {
    double avg, new_avg;
    uint64_t iterations = 0;
    auto &query = tpch_workload_->GetQueryPlan(i);
    while ((iterations < min_iterations_) || ((new_avg - avg < threshold_) && (iterations < max_iters_per_query))) {
      // Executing all the queries on by one in round robin
      auto txn = txn_manager_->BeginTransaction();
      auto accessor =
          catalog_->GetAccessor(common::ManagedPointer<transaction::TransactionContext>(txn), db_oid_, DISABLED);

      auto output_schema = std::get<1>(query_and_plan_[i])->GetOutputSchema().Get();
      // Uncomment this line and change output.cpp:90 to EXECUTION_LOG_INFO to print output
//    execution::exec::OutputPrinter printer(output_schema);
      execution::exec::NoOpResultConsumer printer;
      auto exec_ctx = execution::exec::ExecutionContext(
          db_oid_, common::ManagedPointer<transaction::TransactionContext>(txn), printer, output_schema,
          common::ManagedPointer<catalog::CatalogAccessor>(accessor), exec_settings_);

      std::vector<std::string> query_names{"Q1", "Q4", "Q5", "Q6", "Q7", "Q11", "Q18"};

      uint64_t elapsed_ms = 0;
      {
        common::ScopedTimer<std::chrono::milliseconds> timer(&elapsed_ms);
        std::get<0>(query_and_plan_[index[counter]])
            ->Run(common::ManagedPointer<execution::exec::ExecutionContext>(&exec_ctx), mode);
      }
      std::cout << query_names[index[counter]] << "," << elapsed_ms << std::endl;

      // Only execute up to query_num number of queries for this thread in round-robin
      counter = counter == query_num - 1 ? 0 : counter + 1;
      txn_manager_->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

      // Sleep to create different execution frequency patterns
      auto random_sleep_time = distribution(generator);
      std::this_thread::sleep_for(std::chrono::microseconds(random_sleep_time));

      iterations++;
    }
  }

  db_main_->GetMetricsManager()->UnregisterThread();
  // free the workload here so we don't need to use the loggers anymore
  tpch_workload_.reset();
}

BENCHMARK_REGISTER_F(TPCHBenchmark, Q1)->Unit(benchmark::kMillisecond)->UseManualTime()->Iterations(1);
}  // namespace terrier::runner

