#pragma once

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/catalog_accessor.h"
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_settings.h"
#include "execution/vm/module.h"

namespace noisepage::execution::exec {
class ExecutionContext;
}

namespace noisepage::catalog {
class Catalog;
}

namespace noisepage::transaction {
class TransactionManager;
}

namespace noisepage {
class DBMain;
}

namespace noisepage::tpch {

/**
 * Class that can load the TPCH tables, compile the TPCH queries, and execute the TPCH workload
 */
class Workload {
 public:
  enum class BenchmarkType : uint32_t { TPCH, SSB };

  Workload(common::ManagedPointer<DBMain> db_main, const std::string &db_name, const std::string &table_root,
           enum BenchmarkType type, int64_t threads);

  /**
   * Function to invoke for a single worker thread to invoke the TPCH queries
   * @param worker_id 1-indexed thread id
   * @param execution_us_per_worker max execution time for single worker
   * @param avg_interval_us interval timing
   * @param query_num number of queries to run
   * @param mode execution mode
   * */
  void Execute(int8_t worker_id, uint64_t execution_us_per_worker, uint64_t avg_interval_us, uint32_t query_num,
               execution::vm::ExecutionMode mode);

  /**
   * Function to invoke a single TPCH query and collect runtime
   * @param query_ind index of query into query_and_plan_
   * @param avg_interval_us interval timing
   * @param mode execution mode
   * @param print_output boolean flag to determine whether timing output should be printed
   * @return time taken to run query
   */
  uint64_t TimeQuery(int32_t query_ind, execution::vm::ExecutionMode mode, bool print_output = false);

  /**
   * Function to get number of queries in plan
   * @return size of query plan vector
   */
  uint32_t GetQueryNum() const { return query_and_plan_.size(); }

  /**
   * Function to get number of queries in plan
   * @return size of query plan vector
   */
  std::string GetQueryName(int32_t query_ind) const { return query_names_[query_ind]; }

 private:
  void GenerateTables(execution::exec::ExecutionContext *exec_ctx, const std::string &dir_name,
                      enum BenchmarkType type);

  void LoadQueries(const std::unique_ptr<catalog::CatalogAccessor> &accessor, enum BenchmarkType type);

  common::ManagedPointer<DBMain> db_main_;
  common::ManagedPointer<storage::BlockStore> block_store_;
  common::ManagedPointer<catalog::Catalog> catalog_;
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  catalog::db_oid_t db_oid_;
  catalog::namespace_oid_t ns_oid_;
  execution::exec::ExecutionSettings exec_settings_{};
  std::unique_ptr<catalog::CatalogAccessor> accessor_;

  std::vector<
      std::tuple<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::AbstractPlanNode>>>
      query_and_plan_;
  std::vector<std::string> query_names_;
};

}  // namespace noisepage::tpch
