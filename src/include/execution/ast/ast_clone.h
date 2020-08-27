#pragma once
#include <string>
#include "execution/ast/ast_node_factory.h"
#include "execution/ast/context.h"

namespace terrier::execution::ast {

class AstNode;

/**
 * Class to dump the AST to a string.
 */
class AstClone {
 public:
  /**
   * Clones an ASTNode and its descendants
   * @param node node to dump
   * @return output string
   */
  static AstNode *Clone(AstNode *node, AstNodeFactory *factory, std::string prefix,
                        Context *old_context, Context *new_context);
};

}  // namespace terrier::execution::ast
