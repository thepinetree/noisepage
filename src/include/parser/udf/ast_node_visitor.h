#pragma once

namespace terrier::parser::udf {
class AbstractAST;
class StmtAST;
class ExprAST;
class ValueExprAST;
class VariableExprAST;
class BinaryExprAST;
class CallExprAST;
class SeqStmtAST;
class DeclStmtAST;
class IfStmtAST;
class WhileStmtAST;
class RetStmtAST;
class AssignStmtAST;
class SQLStmtAST;
class DynamicSQLStmtAST;
class FunctionAST;

class ASTNodeVisitor {
 public:
  virtual ~ASTNodeVisitor(){};

  virtual void Visit(AbstractAST *){};
  virtual void Visit(StmtAST *){};
  virtual void Visit(ExprAST *){};
  virtual void Visit(FunctionAST *){};
  virtual void Visit(ValueExprAST *){};
  virtual void Visit(VariableExprAST *){};
  virtual void Visit(BinaryExprAST *){};
  virtual void Visit(CallExprAST *){};
  virtual void Visit(SeqStmtAST *){};
  virtual void Visit(DeclStmtAST *){};
  virtual void Visit(IfStmtAST *){};
  virtual void Visit(WhileStmtAST *){};
  virtual void Visit(RetStmtAST *){};
  virtual void Visit(AssignStmtAST *){};
  virtual void Visit(SQLStmtAST *){};
  virtual void Visit(DynamicSQLStmtAST *){};
};
}  // namespace terrier::parser::udf
