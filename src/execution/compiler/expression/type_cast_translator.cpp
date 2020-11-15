#include "execution/compiler/expression/type_cast_translator.h"

#include "common/error/exception.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/work_context.h"
#include "parser/expression/type_cast_expression.h"

namespace noisepage::execution::compiler {

TypeCastTranslator::TypeCastTranslator(const parser::TypeCastExpression &expr, CompilationContext *compilation_context)
    : ExpressionTranslator(expr, compilation_context) {
  compilation_context->Prepare(*expr.GetChild(0));
}

ast::Expr *TypeCastTranslator::DeriveValue(WorkContext *ctx, const ColumnValueProvider *provider) const {
  auto *codegen = GetCodeGen();
  auto input = ctx->DeriveValue(*GetExpression().GetChild(0), provider);
  auto expr = GetExpressionAs<parser::TypeCastExpression>();
  return codegen->PtrCast(codegen->TplType(sql::GetTypeId(expr.GetReturnValueType())), input);
}

}  // namespace noisepage::execution::compiler
