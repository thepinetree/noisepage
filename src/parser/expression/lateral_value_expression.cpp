#include "parser/expression/lateral_value_expression.h"

#include "binder/sql_node_visitor.h"
#include "common/hash_util.h"
#include "common/json.h"

namespace noisepage::parser {

std::unique_ptr<AbstractExpression> LateralValueExpression::Copy() const {
  auto expr = std::make_unique<LateralValueExpression>(GetTableOid(), GetColumnOid(), GetReturnValueType(), GetSourcePlan(), waiting_vec_);
  expr->SetMutableStateForCopy(*this);
  expr->SetDatabaseOID(this->database_oid_);
  expr->SetTableOID(this->table_oid_);
  expr->SetColumnOID(this->column_oid_);
  return expr;
}

common::hash_t LateralValueExpression::Hash() const {
  common::hash_t hash = common::HashUtil::Hash(GetExpressionType());
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(GetReturnValueType()));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(database_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(table_oid_));
  hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(column_oid_));
  hash = common::HashUtil::CombineHashes(hash, parser::AliasType::HashKey()(alias_));
  return hash;
}

bool LateralValueExpression::operator==(const AbstractExpression &rhs) const {
  if (GetExpressionType() != rhs.GetExpressionType()) return false;
  if (GetReturnValueType() != rhs.GetReturnValueType()) return false;

  auto const &other = dynamic_cast<const LateralValueExpression &>(rhs);
  if (GetColumnOid() != other.GetColumnOid()) return false;
  if (GetTableOid() != other.GetTableOid()) return false;
  if (!(GetAlias() == rhs.GetAlias())) return false;
  return GetDatabaseOid() == other.GetDatabaseOid();
}

//void LateralValueExpression::DeriveExpressionName() {
//  if (!(this->GetAlias().Empty()))
//    this->SetExpressionName(this->GetAlias().GetName());
//  else
//    this->SetExpressionName(column_name_);
//}

void LateralValueExpression::Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) {
  v->Visit(common::ManagedPointer(this));
}

nlohmann::json LateralValueExpression::ToJson() const {
  nlohmann::json j = AbstractExpression::ToJson();
  j["database_oid"] = database_oid_;
  j["table_oid"] = table_oid_;
  j["column_oid"] = column_oid_;
  return j;
}

std::vector<std::unique_ptr<AbstractExpression>> LateralValueExpression::FromJson(const nlohmann::json &j) {
  std::vector<std::unique_ptr<AbstractExpression>> exprs;
  auto e1 = AbstractExpression::FromJson(j);
  exprs.insert(exprs.end(), std::make_move_iterator(e1.begin()), std::make_move_iterator(e1.end()));
  database_oid_ = j.at("database_oid").get<catalog::db_oid_t>();
  table_oid_ = j.at("table_oid").get<catalog::table_oid_t>();
  column_oid_ = j.at("column_oid").get<catalog::col_oid_t>();
  return exprs;
}

DEFINE_JSON_BODY_DECLARATIONS(LateralValueExpression);

}  // namespace noisepage::parser
