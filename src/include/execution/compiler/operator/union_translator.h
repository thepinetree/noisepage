#pragma once

#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/pipeline.h"
#include "execution/compiler/pipeline_driver.h"

namespace noisepage::planner {
class UnionPlanNode;
}  // namespace noisepage::planner

namespace noisepage::execution::compiler {

/**
 * Translator for Unions.
 */
class UnionTranslator : public OperatorTranslator, public PipelineDriver {
 public:
  /**
   * Create a translator for the given plan.
   * @param plan The plan.
   * @param compilation_context The context this translator belongs to.
   * @param pipeline The pipeline this translator is participating in.
   */
  UnionTranslator(const planner::UnionPlanNode &plan, CompilationContext *compilation_context,
                       Pipeline *pipeline);

  /**
   * Push the context through this operator to the next in the pipeline.
   * @param context The context.
   * @param function The pipeline generating function.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * Unions do not produce columns from base tables.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override {
    UNREACHABLE("Unions do not produce columns from base tables.");
  }

  /** @return Throw an error, this is serial for now. */
  util::RegionVector<ast::FieldDecl *> GetWorkerParams() const override { UNREACHABLE("Union is serial."); };

  /** @return Throw an error, this is serial for now. */
  void LaunchWork(FunctionBuilder *function, ast::Identifier work_func_name) const override {
    UNREACHABLE("Union is serial.");
  };

  bool IsCountersPassThrough() const override { return true; }

  ast::Expr *GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const override;

 private:
  Pipeline left_pipeline_;
  Pipeline right_pipeline_;
};

}  // namespace noisepage::execution::compiler
