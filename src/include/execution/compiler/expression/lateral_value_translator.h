#pragma once

#include "execution/compiler/expression/expression_translator.h"

namespace noisepage::parser {
class LateralValueExpression;
}  // namespace noisepage::parser

namespace noisepage::execution::compiler {

/**
 * A translator for derived value expressions.
 */
class LateralValueTranslator : public ExpressionTranslator {
 public:
  /**
   * Create a translator for the given derived value.
   * @param expr The expression to translate.
   * @param compilation_context The context in which translation occurs.
   */
  LateralValueTranslator(const parser::LateralValueExpression &expr, CompilationContext *compilation_context);

  /**
   * Derive the value of the expression.
   * @param ctx The context containing collected subexpressions.
   * @param provider A provider for specific column values.
   * @return The value of the expression.
   */
  ast::Expr *DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const override;

 private:
  const CompilationContext *compilation_context_;
};

}  // namespace noisepage::execution::compiler
