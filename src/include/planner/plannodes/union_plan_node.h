#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/schema.h"
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/plan_visitor.h"

namespace noisepage::planner {

/**
 * Plan node for a union operator
 */
class UnionPlanNode : public AbstractPlanNode {
 public:
  /**
   * Builder for limit plan node
   */
  class Builder : public AbstractPlanNode::Builder<Builder> {
   public:
    Builder() = default;

    /**
     * Don't allow builder to be copied or moved
     */
    DISALLOW_COPY_AND_MOVE(Builder);

    /**
     * Build the limit plan node
     * @return plan node
     */
    std::unique_ptr<UnionPlanNode> Build() {
      return std::unique_ptr<UnionPlanNode>(
          new UnionPlanNode(std::move(children_), std::move(output_schema_)));
    }

  };

 private:
  /**
   * @param children child plan nodes
   * @param output_schema Schema representing the structure of the output of this plan node
   */
  UnionPlanNode(std::vector<std::unique_ptr<AbstractPlanNode>> &&children, std::unique_ptr<OutputSchema> output_schema)
      : AbstractPlanNode(std::move(children), std::move(output_schema)) {}

 public:
  /**
   * Constructor used for JSON serialization
   */
  UnionPlanNode() = default;

  DISALLOW_COPY_AND_MOVE(UnionPlanNode)

  /**
   * @return the type of this plan node
   */
  PlanNodeType GetPlanNodeType() const override { return PlanNodeType::UNION; }

  /**
   * @return the hashed value of this plan node
   */
  common::hash_t Hash() const override { return AbstractPlanNode::Hash(); }

  bool operator==(const AbstractPlanNode &rhs) const override { return rhs.GetPlanNodeType() == GetPlanNodeType(); };

  void Accept(common::ManagedPointer<PlanVisitor> v) const override { v->Visit(this); }
};

DEFINE_JSON_HEADER_DECLARATIONS(UnionPlanNode);

}  // namespace noisepage::planner
