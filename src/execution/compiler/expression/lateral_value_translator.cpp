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
      compilation_context_(compilation_context) {}

ast::Expr *LateralValueTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  const auto &derived_expr = GetExpressionAs<parser::LateralValueExpression>();
  return compilation_context_->LookupTranslator(*derived_expr.GetSourcePlan())
      ->GetChildOutput(ctx, 0, derived_expr.GetColumnOid().UnderlyingValue());
}

}  // namespace noisepage::execution::compiler
