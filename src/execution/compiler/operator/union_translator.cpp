#include "execution/compiler/operator/union_translator.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "planner/plannodes/union_plan_node.h"

namespace noisepage::execution::compiler {

// The majority of work for Unions are performed during expression
// evaluation. In the context of Unions, expressions are derived when
// requesting an output from the expression.

UnionTranslator::UnionTranslator(const planner::UnionPlanNode &plan,
                                           CompilationContext *compilation_context, Pipeline *pipeline)
    : OperatorTranslator(plan, compilation_context, pipeline, brain::ExecutionOperatingUnitType::UNION),
      left_pipeline_(this, Pipeline::Parallelism::Serial),
      right_pipeline_(this, Pipeline::Parallelism::Serial) {
    pipeline->UpdateParallelism(Pipeline::Parallelism::Serial);
    compilation_context->Prepare(*plan.GetChild(0), &left_pipeline_);
    compilation_context->Prepare(*plan.GetChild(1), &right_pipeline_);
    left_pipeline_.LinkNestedPipeline(pipeline, this);
    right_pipeline_.LinkNestedPipeline(pipeline, this);
}

void UnionTranslator::PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const {
  if(&context->GetPipeline() == &left_pipeline_
      || &context->GetPipeline() == &right_pipeline_){
    GetPipeline()->CallNestedRunPipelineFunction(context, this, function);
    return;
  }
  context->Push(function);
}

ast::Expr *UnionTranslator::GetChildOutput(WorkContext *context, uint32_t child_idx, uint32_t attr_idx) const {
  if(&context->GetPipeline() == &left_pipeline_){
    return OperatorTranslator::GetChildOutput(context, 0, attr_idx);
  }
  if(&context->GetPipeline() == &right_pipeline_){
    return OperatorTranslator::GetChildOutput(context, 1, attr_idx);
  }
  // otherwise return pipeline args?
  return context->GetPipeline().GetNestedInputArg(attr_idx);
//  return context
}

}  // namespace noisepage::execution::compiler
