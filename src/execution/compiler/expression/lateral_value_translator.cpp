#include "execution/compiler/expression/lateral_value_translator.h"

#include "execution/compiler/compilation_context.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/lateral_value_expression.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/output_schema.h"

namespace noisepage::execution::compiler {

LateralValueTranslator::LateralValueTranslator(const parser::LateralValueExpression &expr,
                                               CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context),
      compilation_context_(compilation_context) {
//  const auto &lateral_expr = GetExpressionAs<parser::LateralValueExpression>();
//  auto source_translator = compilation_context_->LookupTranslator(*lateral_expr.GetSourcePlan());
////  source_translator->RegisterNeedValue(compilation_context->GetCurrentOp(), lateral_expr.GetColumnOid().UnderlyingValue());
//  NOISEPAGE_ASSERT(source_translator->GetPipeline() == compilation_context->GetCurrentOp()->GetPipeline(),
//                   "Can't call lateral values from a different pipeline yet");
}

ast::Expr *LateralValueTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  const auto &lateral_expr = GetExpressionAs<parser::LateralValueExpression>();
  auto source_translator = compilation_context_->LookupTranslator(*lateral_expr.GetSourcePlan());
  NOISEPAGE_ASSERT(source_translator->GetPipeline() == &ctx->GetPipeline(), "Can't call lateral values from a different pipeline yet");

  return source_translator->GetChildOutput(ctx, 0, lateral_expr.GetColumnOid().UnderlyingValue());
}

}  // namespace noisepage::execution::compiler
