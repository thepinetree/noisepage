struct output_struct {
  col1: Integer
}

fun main(exec_ctx: *ExecutionContext) -> int32 {
  // Initialize CTE Scan Iterator

  var output_buffer = @resultBufferNew(exec_ctx)

  var TEMP_OID_MASK: uint32 = 2147483647 + 1                       // 2^31
  var col_types: [5]uint32
  col_types[0] = 4
  col_types[1] = 4
  col_types[2] = 4
  col_types[3] = 4
  col_types[4] = 1

  var out : *output_struct

  var temp_col_oids: [5]uint32
  temp_col_oids[0] = TEMP_OID_MASK | 1 // res
  temp_col_oids[1] = TEMP_OID_MASK | 2 // result
  temp_col_oids[2] = TEMP_OID_MASK | 3 // s
  temp_col_oids[3] = TEMP_OID_MASK | 4 // x
  temp_col_oids[4] = TEMP_OID_MASK | 5 // rec?


  var cte_scan: IndCteScanIterator
  @indCteScanInit(&cte_scan, exec_ctx, TEMP_OID_MASK, temp_col_oids, col_types, false)

  //var table_oid : uint32
  //table_oid = @testCatalogLookup(exec_ctx, "test_1", "")
  //var col_oids: [1]uint32
  //col_oids[0] = @testCatalogLookup(exec_ctx, "test_1", "colA")

  var out_tvi: TableVectorIterator

  //for (@tableIterInit(&out_tvi, exec_ctx, table_oid, col_oids); @tableIterAdvance(&out_tvi); ) {
  // var out_vpi = @tableIterGetVPI(&out_tvi)
  // for (; @vpiHasNext(out_vpi); @vpiAdvance(out_vpi)) {
  //   var x = @vpiGetInt(out_vpi, 0)


  var insert_pr = @indCteScanGetInsertTempTablePR(&cte_scan)
  var integer_ins : Integer
  integer_ins = @initSqlNull(&integer_ins)
  @prSetInt(insert_pr, 0, &integer_ins)
  integer_ins = @intToSql(0)
  @prSetInt(insert_pr, 1, &integer_ins)
  integer_ins = @intToSql(0)
  @prSetInt(insert_pr, 2, &integer_ins)
  integer_ins = @intToSql(50000000)
  @prSetInt(insert_pr, 3, &integer_ins)

  var bool_ins = @boolToSql(true)
  @prSetBool(insert_pr, 4, &bool_ins)
  var base_insert_temp_table_slot = @indCteScanTableInsert(&cte_scan)


  for(@indCteScanAccumulate(&cte_scan)){
    var cte = @indCteScanGetReadCte(&cte_scan)
    var tvi : TableVectorIterator
    @tableIterInit(&tvi, exec_ctx, TEMP_OID_MASK, temp_col_oids)
    for(@tableIterAdvance(&tvi)){
      var vpi = @tableIterGetVPI(&tvi)
      for(; @vpiHasNext(vpi); @vpiAdvance(vpi)){
      // FROM run AS "run"("rec?", "res", "result", "s", "x"),
        var res = @vpiGetInt(vpi, 0)
        var result = @vpiGetInt(vpi, 1)
        var s = @vpiGetInt(vpi, 2)
        var x = @vpiGetInt(vpi, 3)
        var rec = @vpiGetBool(vpi, 4)

        if(@sqlToBool(rec)){
        insert_pr = @indCteScanGetInsertTempTablePR(&cte_scan)

        if(@sqlToBool(s <= x)){
          var sql_int : Integer
          sql_int = @initSqlNull(&sql_int)
          @prSetInt(insert_pr, 0, &sql_int)
          sql_int = result + s
          @prSetInt(insert_pr, 1, &sql_int)
          sql_int = s + 1
          @prSetInt(insert_pr, 2, &sql_int)
          sql_int = x
          @prSetInt(insert_pr, 3, &sql_int)

          var sql_bool : Boolean
          sql_bool = @boolToSql(true)
          @prSetBool(insert_pr, 4, &sql_bool)
        } else {
         var sql_int : Integer
         sql_int = @initSqlNull(&sql_int)
         @prSetInt(insert_pr, 0, &sql_int)
         sql_int = result
         @prSetInt(insert_pr, 1, &sql_int)
         sql_int = s
         @prSetInt(insert_pr, 2, &sql_int)
         sql_int = x
         @prSetInt(insert_pr, 3, &sql_int)
         var sql_bool : Boolean
         sql_bool = @boolToSql(false)
         @prSetBool(insert_pr, 4, &sql_bool)
        }

        var ind_insert_temp_table_slot = @indCteScanTableInsert(&cte_scan)
        }
      }
      @vpiReset(vpi)
    }
    @tableIterClose(&tvi)
  }

  var ret = 0
  var tvi: TableVectorIterator
  @tableIterInit(&tvi, exec_ctx, TEMP_OID_MASK, temp_col_oids)
  for (@tableIterAdvance(&tvi)) {
    var vpi = @tableIterGetVPI(&tvi)
    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
      var resres = @vpiGetInt(vpi, 1)
      var recrec = @vpiGetBool(vpi, 4)
      //if(@sqlToBool(recrec)){
        out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        out.col1 = resres
      //}
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
  @indCteScanFree(&cte_scan)

  //}
  // @vpiReset(out_vpi)
  //}
  //@tableIterClose(&out_tvi)

  @resultBufferFinalize(output_buffer)
  @resultBufferFree(output_buffer)
  return ret
}
