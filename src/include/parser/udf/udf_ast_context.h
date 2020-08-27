#pragma once

#include "type/type_id.h"

namespace terrier::parser::udf {
class UDFASTContext {
 public:
  UDFASTContext() {}

  void SetVariableType(std::string &var, type::TypeId type) {
    symbol_table_[var] = type;
  }

  bool GetVariableType(const std::string &var, type::TypeId *type) {
    auto it = symbol_table_.find(var);
    if(it == symbol_table_.end()){
      return false;
    }
    if(type != nullptr){
      *type = it->second;
    }
    return true;
  }

  void AddVariable(std::string name) { local_variables_.push_back(name); }

  const std::string &GetVariableAtIndex(int index) {
    TERRIER_ASSERT(local_variables_.size() >= index, "Bad var");
    return local_variables_[index-1];
  }

 private:
  std::unordered_map<std::string, type::TypeId> symbol_table_;
  std::vector<std::string> local_variables_;
};

}  // namespace terrier::parser::udf
