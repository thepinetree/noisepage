#include "common/error/exception.h"

#include "binder/bind_node_visitor.h"

#include "execution/ast/ast.h"
#include "execution/ast/ast_clone.h"
#include "execution/exec/execution_settings.h"
#include "execution/compiler/executable_query.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/if.h"
#include "execution/compiler/loop.h"

#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/statistics/stats_storage.h"
#include "catalog/catalog_accessor.h"

#include "traffic_cop/traffic_cop_util.h"

#include "parser/expression/constant_value_expression.h"
#include "parser/postgresparser.h"
#include "parser/udf/udf_codegen.h"
#include "parser/udf/ast_nodes.h"

#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::parser::udf{

UDFCodegen::UDFCodegen(catalog::CatalogAccessor *accessor, FunctionBuilder *fb,
    parser::udf::UDFASTContext *udf_ast_context, CodeGen *codegen, catalog::db_oid_t db_oid)
    : accessor_{accessor}, fb_{fb}, udf_ast_context_{udf_ast_context}, codegen_{codegen},
      aux_decls_(codegen->GetAstContext()->GetRegion()), db_oid_{db_oid} {
  for(size_t i = 0;fb->GetParameterByPosition(i) != nullptr;i++){
    auto param = fb->GetParameterByPosition(i);
    const auto &name = param->As<execution::ast::IdentifierExpr>()->Name();
    str_to_ident_.emplace(name.GetString(),
                          name);
  }
}

const char *UDFCodegen::GetReturnParamString() {
  return "return_val";
}

  void UDFCodegen::GenerateUDF(AbstractAST *ast) { ast->Accept(this); }

void UDFCodegen::Visit(AbstractAST *ast) {
  UNREACHABLE("Not implemented");
}

void UDFCodegen::Visit(DynamicSQLStmtAST *ast) {
  UNREACHABLE("Not implemented");
}

catalog::type_oid_t UDFCodegen::GetCatalogTypeOidFromSQLType(execution::ast::BuiltinType::Kind type){
  switch(type){
    case execution::ast::BuiltinType::Kind::Integer: {
      return accessor_->GetTypeOidFromTypeId(type::TypeId::INTEGER);
    }
    case execution::ast::BuiltinType::Kind::Boolean: {
      return accessor_->GetTypeOidFromTypeId(type::TypeId::BOOLEAN);
    }
    default:
      return accessor_->GetTypeOidFromTypeId(type::TypeId::INVALID);
      TERRIER_ASSERT(false, "Unsupported param type");
  }
}


void UDFCodegen::Visit(CallExprAST *ast) {
//  UNREACHABLE("Not implemented");
  auto &args = ast->args;
  std::vector<execution::ast::Expr*> args_ast;
  std::vector<execution::ast::Expr*> args_ast_region_vec;
  std::vector<catalog::type_oid_t> arg_types;

  for(auto &arg : args){
    arg->Accept(this);
    args_ast.push_back(dst_);
    args_ast_region_vec.push_back(dst_);
    auto *builtin = dst_->GetType()->SafeAs<execution::ast::BuiltinType>();
    TERRIER_ASSERT(builtin != nullptr, "Not builtin parameter");
    TERRIER_ASSERT(builtin->IsSqlValueType(), "Param is not SQL Value Type");
    arg_types.push_back(GetCatalogTypeOidFromSQLType(builtin->GetKind()));
  }
  auto proc_oid = accessor_->GetProcOid(ast->callee, arg_types);
  TERRIER_ASSERT(proc_oid != catalog::INVALID_PROC_OID, "Invalid call");
  auto context = accessor_->GetProcCtxPtr(proc_oid);
  if(context->IsBuiltin()){
    fb_->Append(codegen_->MakeStmt(codegen_->CallBuiltin(context->GetBuiltin(), std::move(args_ast))));
  }else{
    auto it = str_to_ident_.find(ast->callee);
    execution::ast::Identifier ident_expr;
    if(it != str_to_ident_.end()){
      ident_expr = it->second;
    }else{
      auto file = reinterpret_cast<execution::ast::File*>(execution::ast::AstClone::
          Clone(context->GetFile(), codegen_->GetAstContext()->GetNodeFactory(), "", context->GetASTContext(),
          codegen_->GetAstContext().Get()));
      for(auto decl : file->Declarations()){
        aux_decls_.push_back(decl);
      }
      ident_expr = codegen_->MakeFreshIdentifier(
          file->Declarations().back()->Name().GetString());
      str_to_ident_[file->Declarations().back()->Name().GetString()] = ident_expr;
    }
    fb_->Append(codegen_->MakeStmt(codegen_->Call(
        ident_expr, args_ast_region_vec)));
  }
//    fb_->Append(codegen_->Call)
  }


void UDFCodegen::Visit(StmtAST *ast) {
  UNREACHABLE("Not implemented");
}

void UDFCodegen::Visit(ExprAST *ast) {
  UNREACHABLE("Not implemented");
}

void UDFCodegen::Visit(DeclStmtAST *ast) {
    if(ast->name == "*internal*"){
      return;
    }
    execution::ast::Identifier ident = codegen_->MakeFreshIdentifier(ast->name);
    str_to_ident_.emplace(ast->name, ident);
    if(ast->initial != nullptr) {
//      Visit(ast->initial.get());
      ast->initial->Accept(this);
    }
  fb_->Append(codegen_->DeclareVar(ident, codegen_->TplType(execution::sql::GetTypeId(ast->type)), dst_));
}

void UDFCodegen::Visit(FunctionAST *ast) {
  for(size_t i = 0;i < ast->param_types_.size();i++){
//    auto param_type = codegen_->TplType(ast->param_types_[i]);
    str_to_ident_.emplace(ast->param_names_[i], codegen_->MakeFreshIdentifier("udf"));
  }
  ast->body.get()->Accept(this);
}

void UDFCodegen::Visit(VariableExprAST *ast) {
    auto it = str_to_ident_.find(ast->name);
    TERRIER_ASSERT(it != str_to_ident_.end(), "variable not declared");
  dst_ = codegen_->MakeExpr(it->second);
  }

void UDFCodegen::Visit(ValueExprAST *ast) {
  auto val = common::ManagedPointer(ast->value_).CastManagedPointerTo<parser::ConstantValueExpression>();
  auto type_id = execution::sql::GetTypeId(val->GetReturnValueType());
  switch (type_id) {
    case execution::sql::TypeId::Boolean:
      dst_ = codegen_->BoolToSql(val->GetBoolVal().val_);
      break;
    case execution::sql::TypeId::TinyInt:
    case execution::sql::TypeId::SmallInt:
    case execution::sql::TypeId::Integer:
    case execution::sql::TypeId::BigInt:
      dst_ = codegen_->IntToSql(val->GetInteger().val_);
      break;
    case execution::sql::TypeId::Float:
    case execution::sql::TypeId::Double:
      dst_ = codegen_->FloatToSql(val->GetReal().val_);
    case execution::sql::TypeId::Date:
      dst_ = codegen_->DateToSql(val->GetDateVal().val_);
      break;
    case execution::sql::TypeId::Timestamp:
      dst_ = codegen_->TimestampToSql(val->GetTimestampVal().val_);
      break;
    case execution::sql::TypeId::Varchar:
      dst_ = codegen_->StringToSql(val->GetStringVal().StringView());
      break;
    default:
      throw NOT_IMPLEMENTED_EXCEPTION("Unsupported type in UDF codegen");
  }
}

void UDFCodegen::Visit(AssignStmtAST *ast) {
  reinterpret_cast<AbstractAST*>(ast->rhs.get())->Accept(this);
  auto rhs_expr = dst_;

  type::TypeId left_type;
  udf_ast_context_->GetVariableType(ast->lhs->name, &left_type);


  auto it = str_to_ident_.find(ast->lhs->name);
  TERRIER_ASSERT(it != str_to_ident_.end(), "Variable not found");
  auto left_codegen_ident = it->second;

  auto *left_expr = codegen_->MakeExpr(left_codegen_ident);

//  auto right_type = rhs_expr->GetType()->GetTypeId();

  if (left_type == type::TypeId::VARCHAR) {
//    llvm::Value *l_val = nullptr, *l_len = nullptr, *l_null = nullptr;
//    left_codegen_val.ValuesForMaterialization(*codegen_, l_val, l_len, l_null);
//
//    llvm::Value *r_val = nullptr, *r_len = nullptr, *r_null = nullptr;
//    right_codegen_val.ValuesForMaterialization(*codegen_, r_val, r_len, r_null);
//
//    (*codegen_)->CreateStore(r_val, l_val);
//    (*codegen_)->CreateStore(r_len, l_len);

//    return;
  }

//  if (right_type != left_type) {
//    // TODO[Siva]: Need to check that they can be casted in semantic analysis
//    rhs_expr = codegen_->Cast.CastTo(
//        *codegen_,
//        codegen::type::Type(ast->lhs->GetVarType(udf_context_), false));
//  }

//  (*codegen_)-/CreateStore(right_codegen_val.GetValue(), left_val);
  fb_->Append(codegen_->Assign(left_expr, rhs_expr));
}

void UDFCodegen::Visit(BinaryExprAST *ast) {
    execution::parsing::Token::Type op_token;
    bool compare = false;
    switch(ast->op){
      case terrier::parser::ExpressionType::OPERATOR_DIVIDE:
        op_token = execution::parsing::Token::Type::SLASH;
        break;
      case terrier::parser::ExpressionType::OPERATOR_PLUS:
        op_token = execution::parsing::Token::Type::PLUS;
        break;
      case terrier::parser::ExpressionType::OPERATOR_MINUS:
        op_token = execution::parsing::Token::Type::MINUS;
        break;
      case terrier::parser::ExpressionType::OPERATOR_MULTIPLY:
        op_token = execution::parsing::Token::Type::STAR;
        break;
      case terrier::parser::ExpressionType::OPERATOR_MOD:
        op_token = execution::parsing::Token::Type::PERCENT;
        break;
      case terrier::parser::ExpressionType::CONJUNCTION_OR:
        op_token = execution::parsing::Token::Type::OR;
        break;
      case terrier::parser::ExpressionType::CONJUNCTION_AND:
        op_token = execution::parsing::Token::Type::AND;
        break;
      case terrier::parser::ExpressionType::COMPARE_GREATER_THAN:
        compare = true;
        op_token = execution::parsing::Token::Type::GREATER;
        break;
      case terrier::parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO:
        compare = true;
        op_token = execution::parsing::Token::Type::GREATER_EQUAL;
        break;
      case terrier::parser::ExpressionType::COMPARE_LESS_THAN_OR_EQUAL_TO:
        compare = true;
        op_token = execution::parsing::Token::Type::LESS_EQUAL;
        break;
      case terrier::parser::ExpressionType::COMPARE_LESS_THAN:
        compare = true;
        op_token = execution::parsing::Token::Type::LESS;
        break;
      default:
        // TODO(tanujnay112): figure out concatenation operation from expressions?
        UNREACHABLE("Unsupported expression");
    }
    ast->lhs->Accept(this);
    auto lhs_expr = dst_;

    ast->rhs->Accept(this);
    auto rhs_expr = dst_;
    if(compare){
      dst_ = codegen_->Compare(op_token, lhs_expr, rhs_expr);
    }else {
      dst_ = codegen_->BinaryOp(op_token, lhs_expr, rhs_expr);
    }
}

void UDFCodegen::Visit(IfStmtAST *ast) {
  ast->cond_expr->Accept(this);
  auto cond = dst_;

  If branch(fb_, cond);
  ast->then_stmt->Accept(this);
  if(ast->else_stmt != nullptr) {
    branch.Else();
    ast->else_stmt->Accept(this);
  }
  branch.EndIf();
}

void UDFCodegen::Visit(SeqStmtAST *ast) {
  for(auto &stmt : ast->stmts){
    stmt->Accept(this);
  }
}

void UDFCodegen::Visit(WhileStmtAST *ast) {
  ast->cond_expr->Accept(this);
  auto cond = dst_;
//  cond = codegen_->Compare(execution::parsing::Token::Type::EQUAL_EQUAL, cond, )
  cond = codegen_->CallBuiltin(execution::ast::Builtin::SqlToBool, {cond});
  Loop loop(fb_, cond);
  ast->body_stmt->Accept(this);
  loop.EndLoop();
}

void UDFCodegen::Visit(RetStmtAST *ast) {
  ast->expr->Accept(reinterpret_cast<ASTNodeVisitor*>(this));
//  auto iter = str_to_ident_.find(std::string(GetReturnParamString()));
//  TERRIER_ASSERT(iter != str_to_ident_.end(), "Return param not found");
//  auto ret_expr = codegen_->MakeExpr(iter->second);
//  fb_->Append(codegen_->Assign(ret_expr, dst_));
  auto ret_expr = dst_;
  fb_->Append(codegen_->Return(ret_expr));
}

void UDFCodegen::Visit(SQLStmtAST *ast) {
  needs_exec_ctx_ = true;
  const auto query = common::ManagedPointer(ast->query);
  auto stats = optimizer::StatsStorage();
  std::unique_ptr<planner::AbstractPlanNode> plan = trafficcop::TrafficCopUtil::Optimize(accessor_->GetTxn(),
                                                   common::ManagedPointer(accessor_), query, db_oid_, common::ManagedPointer(&stats),
                                                                                         std::make_unique<optimizer::TrivialCostModel>(), 1000000);
  // make lambda that just writes into this
  auto count_var = str_to_ident_.find(ast->var_name)->second;
  auto lam_var = codegen_->MakeFreshIdentifier("lamb");
  TERRIER_ASSERT(plan->GetOutputSchema()->GetColumns().size() == 1, "Can't support non scalars yet!");

  execution::util::RegionVector<execution::ast::FieldDecl *> params(codegen_->GetAstContext()->GetRegion());
  auto input_param = codegen_->MakeFreshIdentifier("input");
  params.push_back(codegen_->MakeField(input_param, codegen_->TplType(execution::sql::GetTypeId(plan->GetOutputSchema()
                                                                          ->GetColumn(0).GetType()))));
  execution::ast::LambdaExpr *lambda_expr;
  FunctionBuilder fn(codegen_, std::move(params), codegen_->BuiltinType(execution::ast::BuiltinType::Nil));
  {
    fn.Append(codegen_->Assign(codegen_->MakeExpr(count_var),
                               codegen_->MakeExpr(input_param)));
  }
  lambda_expr = fn.FinishLambda();
  lambda_expr->SetName(lam_var);

  fb_->Append(codegen_->DeclareVar(lam_var, codegen_->LambdaType(lambda_expr->GetFunctionLitExpr()->TypeRepr()),
                                     lambda_expr));

  execution::exec::ExecutionSettings exec_settings{};
  const std::string dummy_query = "";
  auto exec_query = execution::compiler::CompilationContext::Compile(
      *plan, exec_settings, accessor_,
      execution::compiler::CompilationMode::OneShot,
      common::ManagedPointer<const std::string>(&dummy_query), lambda_expr, codegen_->GetAstContext());
  auto fns = exec_query->GetFunctions();
  auto decls = exec_query->GetDecls();

  aux_decls_.insert(aux_decls_.end(), decls.begin(), decls.end());

  // make query state
  auto query_state = codegen_->MakeFreshIdentifier("query_state");
  fb_->Append(codegen_->DeclareVarNoInit(query_state,
                             codegen_->MakeExpr(exec_query->GetQueryStateType()->Name())));
  fb_->Append(codegen_->Assign(codegen_->AccessStructMember(codegen_->MakeExpr(query_state), codegen_->MakeIdentifier("execCtx")),
                               fb_->GetParameterByPosition(0)));
  // set its execution context to whatever exec context was passed in here

  for(auto &sub_fn : fns){
//    aux_decls_.push_back(c)
    if(sub_fn.find("Run") != std::string::npos) {
      fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(sub_fn),
                                 {codegen_->AddressOf(query_state), codegen_->MakeExpr(lam_var)}));
    }else{
      fb_->Append(codegen_->Call(codegen_->GetAstContext()->GetIdentifier(sub_fn),
                                 {codegen_->AddressOf(query_state)}));
    }
  }

  return;
}

}


//
//#include "catalog/catalog.h"t
//#include "codegen/buffering_consumer.h"
//#include "codegen/lang/if.h"
//#include "codegen/lang/loop.h"
//#include "codegen/proxy/udf_util_proxy.h"
//#include "codegen/proxy/string_functions_proxy.h"
//#include "codegen/query.h"
//#include "codegen/query_cache.h"
//#include "codegen/query_compiler.h"
//#include "codegen/type/decimal_type.h"
//#include "codegen/type/integer_type.h"
//#include "codegen/type/type.h"
//#include "codegen/value.h"
//#include "concurrency/transaction_manager_factory.h"
//#include "executor/executor_context.h"
//#include "executor/executors.h"
//#include "optimizer/optimizer.h"
//#include "parser/postgresparser.h"
//#include "traffic_cop/traffic_cop.h"
//#include "parser/udf/ast_nodes.h"
//#include "udf/udf_util.h"
//
//namespace peloton {
//namespace udf {
//UDFCodegen::UDFCodegen(codegen::CodeGen *codegen, codegen::FunctionBuilder *fb,
//                       UDFContext *udf_context)
//    : codegen_(codegen), fb_(fb), udf_context_(udf_context), dst_(nullptr){};
//
//void UDFCodegen::GenerateUDF(AbstractAST *ast) { ast->Accept(this); }
//
//void UDFCodegen::Visit(ValueExprAST *ast) {
//  switch (ast->value_.GetTypeId()) {
//    case type::TypeId::INTEGER: {
//      *dst_ = codegen::Value(codegen::type::Type(type::TypeId::INTEGER, false),
//                             codegen_->Const32(ast->value_.GetAs<int>()));
//      break;
//    }
//    case type::TypeId::DECIMAL: {
//      *dst_ = peloton::codegen::Value(
//          peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
//          codegen_->ConstDouble(ast->value_.GetAs<double>()));
//      break;
//    }
//    case type::TypeId::VARCHAR: {
//      auto *val = codegen_->ConstStringPtr(ast->value_.ToString());
//      auto *len = codegen_->Const32(ast->value_.GetLength());
//      *dst_ = peloton::codegen::Value(
//          peloton::codegen::type::Type(type::TypeId::VARCHAR, false), val, len);
//      break;
//    }
//    default:
//      throw Exception("ValueExprAST::Codegen : Expression type not supported");
//  }
//}
//
//void UDFCodegen::Visit(VariableExprAST *ast) {
//  llvm::Value *val = fb_->GetArgumentByName(ast->name);
//  type::TypeId type = udf_context_->GetVariableType(ast->name);
//
//  if (val != nullptr) {
//    if (type == type::TypeId::VARCHAR) {
//      // read in from the StrwithLen
//      auto *str_with_len_type = peloton::codegen::StrWithLenProxy::GetType(
//                                    *codegen_);
//
//      std::vector<llvm::Value*> indices(2);
//      indices[0] = codegen_->Const32(0);
//      indices[1] = codegen_->Const32(0);
//
//      auto *str_val = (*codegen_)->CreateLoad((*codegen_)->CreateGEP(
//          str_with_len_type, val, indices, "str_ptr"));
//
//      indices[1] = codegen_->Const32(1);
//      auto *len_val = (*codegen_)->CreateLoad((*codegen_)->CreateGEP(
//          str_with_len_type, val,indices, "str_len"));
//
//      *dst_ = peloton::codegen::Value(
//          peloton::codegen::type::Type(type::TypeId::VARCHAR, false), str_val,
//          len_val);
//    } else {
//      *dst_ = codegen::Value(codegen::type::Type(type, false), val);
//      return;
//    }
//  } else {
//    if (type == type::TypeId::VARCHAR) {
//      auto alloc_val = udf_context_->GetAllocValue(ast->name);
//      llvm::Value *val = nullptr, *len = nullptr, *null = nullptr;
//      alloc_val.ValuesForMaterialization(*codegen_, val, len, null);
//
//      auto *index = codegen_->Const32(0);
//      auto *str_val = (*codegen_)->CreateLoad(
//          (*codegen_)->CreateInBoundsGEP(codegen_->CharPtrType(), val, index));
//      auto *len_val = (*codegen_)->CreateLoad(
//          (*codegen_)->CreateInBoundsGEP(codegen_->Int32Type(), len, index));
//
//      *dst_ = peloton::codegen::Value(
//          peloton::codegen::type::Type(type::TypeId::VARCHAR, false), str_val,
//          len_val);
//    } else {
//      // Assuming each variable is defined
//      auto *alloc_val = (udf_context_->GetAllocValue(ast->name)).GetValue();
//      *dst_ = codegen::Value(codegen::type::Type(type, false),
//                             (*codegen_)->CreateLoad(alloc_val));
//      return;
//    }
//  }
//}
//
//void UDFCodegen::Visit(BinaryExprAST *ast) {
//  auto *ret_dst = dst_;
//  codegen::Value left;
//  dst_ = &left;
//  ast->lhs->Accept(this);
//  codegen::Value right;
//  dst_ = &right;
//  ast->rhs->Accept(this);
//  // TODO(boweic): Should not be nullptr;
//  if (left.GetValue() == nullptr || right.GetValue() == nullptr) {
//    *ret_dst = codegen::Value();
//    return;
//  }
//  switch (ast->op) {
//    case ExpressionType::OPERATOR_PLUS: {
//      *ret_dst = left.Add(*codegen_, right);
//      return;
//    }
//    case ExpressionType::OPERATOR_MINUS: {
//      *ret_dst = left.Sub(*codegen_, right);
//      return;
//    }
//    case ExpressionType::OPERATOR_MULTIPLY: {
//      *ret_dst = left.Mul(*codegen_, right);
//      return;
//    }
//    case ExpressionType::OPERATOR_DIVIDE: {
//      *ret_dst = left.Div(*codegen_, right);
//      return;
//    }
//    case ExpressionType::COMPARE_LESSTHAN: {
//      auto val = left.CompareLt(*codegen_, right);
//      // TODO(boweic): support boolean type
//      *ret_dst = val.CastTo(*codegen_,
//                            codegen::type::Type(type::TypeId::DECIMAL, false));
//      return;
//    }
//    case ExpressionType::COMPARE_GREATERTHAN: {
//      auto val = left.CompareGt(*codegen_, right);
//      *ret_dst = val.CastTo(*codegen_,
//                            codegen::type::Type(type::TypeId::DECIMAL, false));
//      return;
//    }
//    case ExpressionType::COMPARE_EQUAL: {
//      auto val = left.CompareEq(*codegen_, right);
//      *ret_dst = val.CastTo(*codegen_,
//                            codegen::type::Type(type::TypeId::DECIMAL, false));
//      return;
//    }
//    default:
//      throw Exception("BinaryExprAST : Operator not supported");
//  }
//}
//
//void UDFCodegen::Visit(CallExprAST *ast) {
//  std::vector<llvm::Value *> args_val;
//  std::vector<type::TypeId> args_type;
//  auto *ret_dst = dst_;
//  // Codegen type needed to retrieve built-in functions
//  // TODO(boweic): Use a uniform API for UDF and built-in so code don't get
//  // super ugly
//  std::vector<codegen::Value> args_codegen_val;
//  for (unsigned i = 0, size = ast->args.size(); i != size; ++i) {
//    codegen::Value arg_val;
//    dst_ = &arg_val;
//    ast->args[i]->Accept(this);
//    args_val.push_back(arg_val.GetValue());
//    // TODO(boweic): Handle type missmatch in typechecking phase
//    args_type.push_back(arg_val.GetType().type_id);
//    args_codegen_val.push_back(arg_val);
//  }
//
//  // Check if present in the current code context
//  // Else, check the catalog and get it
//  llvm::Function *callee_func;
//  type::TypeId return_type = type::TypeId::INVALID;
//  if (ast->callee == udf_context_->GetFunctionName()) {
//    // Recursive function call
//    callee_func = fb_->GetFunction();
//    return_type = udf_context_->GetFunctionReturnType();
//  } else {
//    // Check and set the function ptr
//    // TODO(boweic): Visit the catalog using the interface that is protected by
//    // transaction
//    const catalog::FunctionData &func_data =
//        catalog::Catalog::GetInstance()->GetFunction(ast->callee, args_type);
//    if (func_data.is_udf_) {
//      return_type = func_data.return_type_;
//      llvm::Type *ret_type =
//          UDFUtil::GetCodegenType(func_data.return_type_, *codegen_);
//      std::vector<llvm::Type *> llvm_args;
//      for (const auto &arg_type : args_type) {
//        llvm_args.push_back(UDFUtil::GetCodegenType(arg_type, *codegen_));
//      }
//      auto *fn_type = llvm::FunctionType::get(ret_type, llvm_args, false);
//      callee_func = llvm::Function::Create(
//          fn_type, llvm::Function::ExternalLinkage, ast->callee,
//          &(codegen_->GetCodeContext().GetModule()));
//      codegen_->GetCodeContext().RegisterExternalFunction(
//          callee_func, func_data.func_context_->GetRawFunctionPointer(
//                           func_data.func_context_->GetUDF()));
//    } else {
//      codegen::type::TypeSystem::InvocationContext ctx{
//          .on_error = OnError::Exception, .executor_context = nullptr};
//      OperatorId operator_id = func_data.func_.op_id;
//      if (ast->args.size() == 1) {
//        auto *unary_op = codegen::type::TypeSystem::GetUnaryOperator(
//            operator_id, args_codegen_val[0].GetType());
//        *ret_dst = unary_op->Eval(*codegen_, args_codegen_val[0], ctx);
//        PL_ASSERT(unary_op != nullptr);
//        return;
//      } else if (ast->args.size() == 2) {
//        codegen::type::Type left_type = args_codegen_val[0].GetType(),
//                            right_type = args_codegen_val[1].GetType();
//        auto *binary_op = codegen::type::TypeSystem::GetBinaryOperator(
//            operator_id, left_type, right_type, left_type, right_type);
//        *ret_dst = binary_op->Eval(
//            *codegen_, args_codegen_val[0].CastTo(*codegen_, left_type),
//            args_codegen_val[1].CastTo(*codegen_, right_type), ctx);
//        PL_ASSERT(binary_op != nullptr);
//        return;
//      } else {
//        std::vector<codegen::type::Type> args_codegen_type;
//        for (const auto &val : args_codegen_val) {
//          args_codegen_type.push_back(val.GetType());
//        }
//        auto *nary_op = codegen::type::TypeSystem::GetNaryOperator(
//            operator_id, args_codegen_type);
//        PL_ASSERT(nary_op != nullptr);
//        *ret_dst = nary_op->Eval(*codegen_, args_codegen_val, ctx);
//        return;
//      }
//    }
//  }
//
//  // TODO(boweic): Throw an exception?
//  if (callee_func == nullptr) {
//    return;  // LogErrorV("Unknown function referenced");
//  }
//
//  // TODO(boweic): Do this in typechecking
//  if (callee_func->arg_size() != ast->args.size()) {
//    return;  // LogErrorV("Incorrect # arguments passed");
//  }
//
//  auto *call_ret = codegen_->CallFunc(callee_func, args_val);
//
//  // TODO(boweic): Maybe wrap this logic as a helper function since it could be
//  // reused
//  switch (return_type) {
//    case type::TypeId::DECIMAL: {
//      *ret_dst = codegen::Value{codegen::type::Decimal::Instance(), call_ret};
//      break;
//    }
//    case type::TypeId::INTEGER: {
//      *ret_dst = codegen::Value{codegen::type::Integer::Instance(), call_ret};
//      break;
//    }
//    default: {
//      throw Exception("CallExpr::Codegen : Return type not supported");
//    }
//  }
//}
//
//void UDFCodegen::Visit(SeqStmtAST *ast) {
//  for (uint32_t i = 0; i < ast->stmts.size(); i++) {
//    // If already return in the current block, don't continue to generate
//    if (codegen_->IsTerminated()) {
//      break;
//    }
//    ast->stmts[i]->Accept(this);
//  }
//}
//
//void UDFCodegen::Visit(DeclStmtAST *ast) {
//  switch (ast->type) {
//    // TODO[Siva]: This is a hack, this is a pointer to type, not type
//    // TODO[Siva]: Replace with this a function that returns llvm::Type from
//    // type::TypeID
//    case type::TypeId::INTEGER: {
//      // TODO[Siva]: 32 / 64 bit handling??
//      auto alloc_val = peloton::codegen::Value(
//          peloton::codegen::type::Type(type::TypeId::INTEGER, false),
//          codegen_->AllocateVariable(codegen_->Int32Type(), ast->name));
//      udf_context_->SetAllocValue(ast->name, alloc_val);
//      break;
//    }
//    case type::TypeId::DECIMAL: {
//      auto alloc_val = peloton::codegen::Value(
//          peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
//          codegen_->AllocateVariable(codegen_->DoubleType(), ast->name));
//      udf_context_->SetAllocValue(ast->name, alloc_val);
//      break;
//    }
//    case type::TypeId::VARCHAR: {
//      auto alloc_val = peloton::codegen::Value(
//          peloton::codegen::type::Type(type::TypeId::VARCHAR, false),
//          codegen_->AllocateVariable(codegen_->CharPtrType(),
//                                     ast->name + "_ptr"),
//          codegen_->AllocateVariable(codegen_->Int32Type(),
//                                     ast->name + "_len"));
//      udf_context_->SetAllocValue(ast->name, alloc_val);
//      break;
//    }
//    default: {
//      // TODO[Siva]: Should throw an excpetion, but need to figure out "found"
//      // and other internal types first.
//    }
//  }
//}
//
//void UDFCodegen::Visit(IfStmtAST *ast) {
//  auto compare_value = peloton::codegen::Value(
//      peloton::codegen::type::Type(type::TypeId::DECIMAL, false),
//      codegen_->ConstDouble(1.0));
//
//  peloton::codegen::Value cond_expr_value;
//  dst_ = &cond_expr_value;
//  ast->cond_expr->Accept(this);
//
//  // Codegen If condition expression
//  codegen::lang::If entry_cond{
//      *codegen_, cond_expr_value.CompareEq(*codegen_, compare_value),
//      "entry_cond"};
//  {
//    // Codegen the then statements
//    ast->then_stmt->Accept(this);
//  }
//  entry_cond.ElseBlock("multipleValue");
//  {
//    // codegen the else statements
//    ast->else_stmt->Accept(this);
//  }
//  entry_cond.EndIf();
//
//  return;
//}
//
//void UDFCodegen::Visit(WhileStmtAST *ast) {
//  // TODO(boweic): Use boolean when supported
//  auto compare_value =
//      codegen::Value(codegen::type::Type(type::TypeId::DECIMAL, false),
//                     codegen_->ConstDouble(1.0));
//
//  peloton::codegen::Value cond_expr_value;
//  dst_ = &cond_expr_value;
//  ast->cond_expr->Accept(this);
//
//  // Codegen If condition expression
//  codegen::lang::Loop loop{
//      *codegen_,
//      cond_expr_value.CompareEq(*codegen_, compare_value).GetValue(),
//      {}};
//  {
//    ast->body_stmt->Accept(this);
//    // TODO(boweic): Use boolean when supported
//    auto compare_value = peloton::codegen::Value(
//        codegen::type::Type(type::TypeId::DECIMAL, false),
//        codegen_->ConstDouble(1.0));
//    codegen::Value cond_expr_value;
//    codegen::Value cond_var;
//    if (!codegen_->IsTerminated()) {
//      dst_ = &cond_expr_value;
//      ast->cond_expr->Accept(this);
//      cond_var = cond_expr_value.CompareEq(*codegen_, compare_value);
//    }
//    loop.LoopEnd(cond_var.GetValue(), {});
//  }
//
//  return;
//}
//
//void UDFCodegen::Visit(RetStmtAST *ast) {
//  // TODO[Siva]: Handle void properly
//  if (ast->expr == nullptr) {
//    // TODO(boweic): We should deduce type in typechecking phase and create a
//    // default value for that type, or find a way to get around llvm basic
//    // block
//    // without return
//    codegen::Value value = peloton::codegen::Value(
//        peloton::codegen::type::Type(udf_context_->GetFunctionReturnType(),
//                                     false),
//        codegen_->ConstDouble(0));
//    (*codegen_)->CreateRet(value.GetValue());
//  } else {
//    codegen::Value expr_ret_val;
//    dst_ = &expr_ret_val;
//    ast->expr->Accept(this);
//
//    if (expr_ret_val.GetType() !=
//        peloton::codegen::type::Type(udf_context_->GetFunctionReturnType(),
//                                     false)) {
//      expr_ret_val = expr_ret_val.CastTo(
//          *codegen_,
//          peloton::codegen::type::Type(udf_context_->GetFunctionReturnType(), false));
//    }
//
//    if(expr_ret_val.GetType() ==
//          peloton::codegen::type::Type(type::TypeId::VARCHAR, false)) {
//      auto *str_with_len_type = peloton::codegen::StrWithLenProxy::GetType(
//                                    *codegen_);
//      llvm::Value *agg_val = codegen_->AllocateVariable(str_with_len_type,
//                                    "return_val");
//
//      std::vector<llvm::Value*> indices(2);
//      indices[0] = codegen_->Const32(0);
//      indices[1] = codegen_->Const32(0);
//
//      auto *str_ptr = (*codegen_)->CreateGEP(str_with_len_type, agg_val,
//                                             indices, "str_ptr");
//
//      indices[1] = codegen_->Const32(1);
//      auto *str_len = (*codegen_)->CreateGEP(str_with_len_type, agg_val,
//                                             indices, "str_len");
//
//      (*codegen_)->CreateStore(expr_ret_val.GetValue(), str_ptr);
//      (*codegen_)->CreateStore(expr_ret_val.GetLength(), str_len);
//      agg_val = (*codegen_)->CreateLoad(agg_val);
//      (*codegen_)->CreateRet(agg_val);
//    } else {
//      (*codegen_)->CreateRet(expr_ret_val.GetValue());
//    }
//  }
//}
//
//void UDFCodegen::Visit(AssignStmtAST *ast) {
//  codegen::Value right_codegen_val;
//  dst_ = &right_codegen_val;
//  ast->rhs->Accept(this);
//
//  auto left_codegen_val = ast->lhs->GetAllocValue(udf_context_);
//
//  auto *left_val = left_codegen_val.GetValue();
//
//  auto right_type = right_codegen_val.GetType();
//
//  auto left_type_id = ast->lhs->GetVarType(udf_context_);
//
//  auto left_type = codegen::type::Type(left_type_id, false);
//
//  if (left_type_id == type::TypeId::VARCHAR) {
//    llvm::Value *l_val = nullptr, *l_len = nullptr, *l_null = nullptr;
//    left_codegen_val.ValuesForMaterialization(*codegen_, l_val, l_len, l_null);
//
//    llvm::Value *r_val = nullptr, *r_len = nullptr, *r_null = nullptr;
//    right_codegen_val.ValuesForMaterialization(*codegen_, r_val, r_len, r_null);
//
//    (*codegen_)->CreateStore(r_val, l_val);
//    (*codegen_)->CreateStore(r_len, l_len);
//
//    return;
//  }
//
//  if (right_type != left_type) {
//    // TODO[Siva]: Need to check that they can be casted in semantic analysis
//    right_codegen_val = right_codegen_val.CastTo(
//        *codegen_,
//        codegen::type::Type(ast->lhs->GetVarType(udf_context_), false));
//  }
//
//  (*codegen_)->CreateStore(right_codegen_val.GetValue(), left_val);
//}
//
//void UDFCodegen::Visit(SQLStmtAST *ast) {
//  auto *val = codegen_->ConstStringPtr(ast->query);
//  auto *len = codegen_->Const32(ast->query.size());
//  auto left = udf_context_->GetAllocValue(ast->var_name);
//
//  // auto &code_context = codegen_->GetCodeContext();
//
//  // codegen::FunctionBuilder temp_fun{
//  //     code_context, "temp_fun_1", codegen_->DoubleType(), {}};
//  // {
//  //   temp_fun.ReturnAndFinish(codegen_->ConstDouble(12.0));
//  // }
//
//  // auto *right_val = codegen_->CallFunc(temp_fun.GetFunction(), {});
//  // (*codegen_)->CreateStore(right_val, left.GetValue());
//
//  codegen_->Call(codegen::UDFUtilProxy::ExecuteSQLHelper,
//                 {val, len, left.GetValue()});
//  return;
//}
//
//void UDFCodegen::Visit(DynamicSQLStmtAST *ast) {
//  codegen::Value query;
//  dst_ = &query;
//  ast->query->Accept(this);
//  auto left = udf_context_->GetAllocValue(ast->var_name);
//  codegen_->Call(codegen::UDFUtilProxy::ExecuteSQLHelper,
//                 {query.GetValue(), query.GetLength(), left.GetValue()});
//  return;
//}
//
//}  // namespace udf
//}  // namespace peloton
