//#include "parser/udf/ast_nodes.h"
//
//#include "catalog/catalog.h"
//#include "codegen/lang/if.h"
//#include "codegen/lang/loop.h"
//#include "codegen/type/decimal_type.h"
//#include "codegen/type/integer_type.h"
//#include "codegen/type/type.h"
//#include "parser/udf/udf_codegen.h"
//#include "udf/udf_util.h"
//
//namespace terrier::parser::udf {
//
////// Codegen for FunctionAST
////llvm::Function *FunctionAST::Codegen(peloton::codegen::CodeGen &codegen,
////                                     peloton::codegen::FunctionBuilder &fb,
////                                     UDFContext *udf_context) {
////  UDFCodegen generator(&codegen, &fb, udf_context);
////  generator.GenerateUDF(body.get());
////  fb.Finish();
////  return fb.GetFunction();
////}
//
//}  // namespace terrier::parser::udf