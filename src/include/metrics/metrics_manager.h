#pragma once

#include <tbb/spin_mutex.h>

#include <memory>
#include <thread>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/managed_pointer.h"
#include "common/thread_context.h"
#include "metrics/abstract_raw_data.h"
#include "metrics/metrics_store.h"

namespace terrier::settings {
class Callbacks;
}

namespace terrier::metrics {

/**
 * Background thread that periodically collects data from thread level collectors
 */
class MetricsManager {
 public:
  /**
   * Aggregate metrics from all threads which have collected stats, combine with what was previously collected
   *
   * @warning this method should be called before manipulating the worker pool, especially if
   * some of the worker threads are reassigned to tasks other than execution.
   */
  void Aggregate();

  /**
   * Called by the thread to get a MetricsStore object
   */
  void RegisterThread();

  /**
   * Should be called by the thread when it is guaranteed to no longer be collecting any more metrics, otherwise,
   * segfault could happen when the unique_ptr releases the MetricsStore
   */
  void UnregisterThread();

  /**
   * @return the MetricsManager's aggregated metrics. Currently used in tests
   */
  const std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> &AggregatedMetrics() const {
    return aggregated_metrics_;
  }

  /**
   * @param component to be tested
   * @return true if metrics are enabled for this component, false otherwise
   */
  bool ComponentEnabled(const MetricsComponent component) {
    return enabled_metrics_.test(static_cast<uint8_t>(component));
  }

  /**
   * Dump aggregated metrics to CSV files.
   */
  void ToCSV() const;

  /**
   * @param component to be enabled
   * @param sample_interval the interval between recording two metrics for the component. 0 means recording every metric
   */
  void EnableMetric(const MetricsComponent component, uint32_t sample_interval) {
    tbb::spin_mutex::scoped_lock guard(latch_);
    TERRIER_ASSERT(!ComponentEnabled(component), "Metric is already enabled.");

    ResetMetric(component);

    enabled_metrics_.set(static_cast<uint8_t>(component), true);
    sample_interval_[static_cast<uint8_t>(component)] = sample_interval;
  }

  /**
   * @param component to be disabled
   */
  void DisableMetric(const MetricsComponent component) {
    tbb::spin_mutex::scoped_lock guard(latch_);
    TERRIER_ASSERT(ComponentEnabled(component), "Metric is already disabled.");
    enabled_metrics_.set(static_cast<uint8_t>(component), false);
    aggregated_metrics_[static_cast<uint8_t>(component)].reset(nullptr);
  }

 private:
  void ResetMetric(MetricsComponent component) const;

  mutable tbb::spin_mutex latch_;
  std::unordered_map<std::thread::id, std::unique_ptr<MetricsStore>> stores_map_;

  std::array<std::unique_ptr<AbstractRawData>, NUM_COMPONENTS> aggregated_metrics_;

  std::bitset<NUM_COMPONENTS> enabled_metrics_ = 0x0;

  std::array<uint32_t, NUM_COMPONENTS> sample_interval_{0x0};
};

}  // namespace terrier::metrics
