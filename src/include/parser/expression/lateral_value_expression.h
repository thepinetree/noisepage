#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "parser/expression/abstract_expression.h"


namespace noisepage::planner {
class AbstractPlanNode;
}  // namespace noisepage::planner

namespace noisepage::optimizer {
class PlanGenerator;
}  // namespace noisepage::planner

namespace noisepage::parser {

/**
 * LateralValueExpression represents a reference to a column.
 */
class LateralValueExpression : public AbstractExpression {
  // PlanGenerator creates LateralValueExpressions and will
  // need to set the bound oids
  friend class noisepage::optimizer::OptimizerUtil;

 public:
//  /**
//   * @param database_oid database OID
//   * @param table_oid table OID
//   * @param column_oid column OID
//   */
//  LateralValueExpression(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, catalog::col_oid_t column_oid,
//                         common::ManagedPointer<planner::AbstractPlanNode> source_plan)
//      : AbstractExpression(ExpressionType::LATERAL_VALUE, type::TypeId::INVALID, {}),
//        database_oid_(database_oid),
//        table_oid_(table_oid),
//        column_oid_(column_oid),
//        source_plan_(source_plan){}

  /**
   * @param table_oid OID of the table.
   * @param column_oid OID of the column.
   * @param type Type of the column.
   */
  LateralValueExpression(catalog::table_oid_t table_oid, catalog::col_oid_t column_oid, type::TypeId type,
                         common::ManagedPointer<planner::AbstractPlanNode> source_plan,
                         std::vector<common::ManagedPointer<parser::LateralValueExpression>> &waiting_vec)
      : AbstractExpression(ExpressionType::LATERAL_VALUE, type, {}), table_oid_(table_oid), column_oid_(column_oid),
        source_plan_(source_plan), waiting_vec_(waiting_vec) {
    waiting_vec.push_back(common::ManagedPointer(this));
  }
//
//  /**
//   * This constructor is used to construct abstract value expressions used by CTEs
//   * for logical derived get below it to reference aliases.
//   * @param table_name Name of the table
//   * @param col_name name of the column.
//   * @param type Type of the column.
//   * @param alias Alias of the column this is referencing
//   * @param column_oid Oid of the column (it should be a temp oid in this case)
//   */
//  LateralValueExpression(type::TypeId type, AliasType alias, catalog::col_oid_t column_oid, common::ManagedPointer<planner::AbstractPlanNode> source_plan)
//      : AbstractExpression(ExpressionType::LATERAL_VALUE, type, std::move(alias), {}),
//        column_oid_(column_oid), source_plan_(source_plan) {}
//
//  /**
//   * @param table_name table name
//   * @param col_name column name
//   * @param database_oid database OID
//   * @param table_oid table OID
//   * @param column_oid column OID
//   * @param type Type of the column.
//   */
//  LateralValueExpression(catalog::db_oid_t database_oid, catalog::table_oid_t table_oid, catalog::col_oid_t column_oid,
//                         type::TypeId type, common::ManagedPointer<planner::AbstractPlanNode> source_plan)
//      : AbstractExpression(ExpressionType::LATERAL_VALUE, type, {}),
//        database_oid_(database_oid),
//        table_oid_(table_oid),
//        column_oid_(column_oid),
//        source_plan_(source_plan) {}

  /** Default constructor for deserialization. */
//  LateralValueExpression() = default;

  /** @return table name */
//  std::string GetTableName() const { return table_name_; }
//
//  /** @return column name */
//  std::string GetColumnName() const { return column_name_; }

  /** @return database oid */
  catalog::db_oid_t GetDatabaseOid() const { return database_oid_; }

  /** @return table oid */
  catalog::table_oid_t GetTableOid() const { return table_oid_; }

  /** @return column oid */
  catalog::col_oid_t GetColumnOid() const { return column_oid_; }

  common::ManagedPointer<planner::AbstractPlanNode> GetSourcePlan() const { return source_plan_; }

//  /**
//   * Get Column Full Name [tbl].[col]
//   */
//  std::string GetFullName() const {
//    if (!table_name_.empty()) {
//      return table_name_ + "." + column_name_;
//    }
//
//    return column_name_;
//  }

  /**
   * Copies this LateralValueExpression
   * @returns copy of this
   */
  std::unique_ptr<AbstractExpression> Copy() const override;

  /**
   * Copies this LateralValueExpression with new children
   * @param children new children
   * @returns copy of this with new children
   */
  std::unique_ptr<AbstractExpression> CopyWithChildren(
      std::vector<std::unique_ptr<AbstractExpression>> &&children) const override {
    NOISEPAGE_ASSERT(children.empty(), "LateralValueExpression should have no children");
    return Copy();
  }

  /**
   * Hashes the current ColumnValue expression.
   */
  common::hash_t Hash() const override;

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two expressions are logically equal
   */
  bool operator==(const AbstractExpression &rhs) const override;

  /**
   * Walks the expression trees and generate the correct expression name
   */
//  void DeriveExpressionName() override;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override;

  /**
   * @return expression serialized to json
   */
  nlohmann::json ToJson() const override;

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  friend optimizer::PlanGenerator;
  /** @param database_oid Database OID to be assigned to this expression */
  void SetDatabaseOID(catalog::db_oid_t database_oid) { database_oid_ = database_oid; }
  /** @param table_oid Table OID to be assigned to this expression */
  void SetTableOID(catalog::table_oid_t table_oid) { table_oid_ = table_oid; }
  /** @param column_oid Column OID to be assigned to this expression */
  void SetColumnOID(catalog::col_oid_t column_oid) { column_oid_ = column_oid; }

  /** OID of the database */
  catalog::db_oid_t database_oid_ = catalog::INVALID_DATABASE_OID;

  /** OID of the table */
  catalog::table_oid_t table_oid_ = catalog::INVALID_TABLE_OID;

  /** OID of the column */
  catalog::col_oid_t column_oid_ = catalog::INVALID_COLUMN_OID;

  common::ManagedPointer<planner::AbstractPlanNode> source_plan_;

  // TODO(tanujnay112) I hate this but I need to figure out a better way to maintain source_plan_
  // if I don't have this then in between me adding myself to a LateralWaitersSet map and source_plan_
  // being later resolved Copy could be called on me, making the source_plan_ resolution useless
  std::vector<common::ManagedPointer<parser::LateralValueExpression>> &waiting_vec_;
};

DEFINE_JSON_HEADER_DECLARATIONS(LateralValueExpression);

}  // namespace noisepage::parser
