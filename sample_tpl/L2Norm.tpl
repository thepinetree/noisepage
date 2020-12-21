struct output_struct {
  col1: Integer
  col2: Real
}

struct AggPayload {
    agg_term_attr0: IntegerSumAggregate
}
struct AggValues {
    agg_term_attr0: Integer
}
struct QueryState {
    execCtx: *ExecutionContext
    aggs   : AggPayload
}
struct PQuery22_State {
}

fun Query2_Init(queryState: *QueryState) -> nil {
    return
}

fun Query2_Pipeline2_InitPipelineState(queryState: *QueryState, pipelineState: *PQuery22_State) -> nil {
    return
}

fun Query2_Pipeline2_TearDownPipelineState(queryState: *QueryState, pipelineState: *PQuery22_State) -> nil {
    return
}

fun main(exec_ctx: *ExecutionContext) -> int32 {
  // Initialize CTE Scan Iterator

  var output_buffer = @resultBufferNew(exec_ctx)

  var TEMP_OID_MASK: uint32 = 2147483647 + 1                       // 2^31
  var col_types: [9]uint32
  col_types[0] = 4
  col_types[1] = 4
  col_types[2] = 4
  col_types[3] = 4
  col_types[4] = 4
  col_types[5] = 4
  col_types[6] = 4
  col_types[7] = 1
  col_types[8] = 1

  var out : *output_struct

  var temp_col_oids: [9]uint32
  temp_col_oids[0] = TEMP_OID_MASK | 1 // res
  temp_col_oids[1] = TEMP_OID_MASK | 2 // elem
  temp_col_oids[2] = TEMP_OID_MASK | 3 // i
  temp_col_oids[3] = TEMP_OID_MASK | 4 // j
  temp_col_oids[4] = TEMP_OID_MASK | 5 // n
  temp_col_oids[5] = TEMP_OID_MASK | 6 // row
  temp_col_oids[6] = TEMP_OID_MASK | 7 // sum
  temp_col_oids[7] = TEMP_OID_MASK | 8 // label
  temp_col_oids[8] = TEMP_OID_MASK | 9 // rec?


  var cte_scan: IndCteScanIterator
  @indCteScanInit(&cte_scan, exec_ctx, TEMP_OID_MASK, temp_col_oids, col_types, false)

  var out_tvi: TableVectorIterator

  var insert_pr = @indCteScanGetInsertTempTablePR(&cte_scan)
  var integer_ins : Integer
  var init_n = @intToSql(200)
  integer_ins = @initSqlNull(&integer_ins)
  var null_int = integer_ins
  @prSetIntNull(insert_pr, 0, null_int)
  @prSetIntNull(insert_pr, 1, null_int)
  integer_ins = @intToSql(1)
  @prSetIntNull(insert_pr, 2, integer_ins)
  @prSetIntNull(insert_pr, 3, null_int)
  integer_ins = init_n
  @prSetIntNull(insert_pr, 4, integer_ins)
  @prSetIntNull(insert_pr, 5, null_int)
  integer_ins = @intToSql(0)
  @prSetIntNull(insert_pr, 6, integer_ins)

  var bool_ins = @boolToSql(false)
  @prSetBool(insert_pr, 7, bool_ins)
  bool_ins = @boolToSql(true)
  @prSetBool(insert_pr, 8, bool_ins)
  var base_insert_temp_table_slot = @indCteScanTableInsert(&cte_scan)

  var table_id_1 = @testCatalogLookup(exec_ctx, "matrix_1", "")
  var table_id_2 = @testCatalogLookup(exec_ctx, "matrix_2", "")


  for(@indCteScanAccumulate(&cte_scan)){
    var cte = @indCteScanGetReadCte(&cte_scan)
    var tvi : TableVectorIterator
    @tableIterInit(&tvi, exec_ctx, TEMP_OID_MASK, temp_col_oids)
    for(@tableIterAdvance(&tvi)){
      var vpi = @tableIterGetVPI(&tvi)
      for(; @vpiHasNext(vpi); @vpiAdvance(vpi)){
      // FROM run AS "run"("rec?", "res", "result", "s", "x"),
        var res = @vpiGetIntNull(vpi, 0)
        var elem = @vpiGetIntNull(vpi, 1)
        var i = @vpiGetIntNull(vpi, 2)
        var j = @vpiGetIntNull(vpi, 3)
        var n = @vpiGetIntNull(vpi, 4)
        var row = @vpiGetIntNull(vpi, 5)
        var sum = @vpiGetIntNull(vpi, 6)
        var label = @vpiGetBoolNull(vpi, 7)
        var rec = @vpiGetBool(vpi, 8)

        //var sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = @intToSql(12345679)
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = res
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = elem
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = i
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = j
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = n
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = row
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = sum
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = @vpiGetIntNull(vpi, 7)
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = @vpiGetIntNull(vpi, 8)
//
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = @intToSql(12345)

        if(@sqlToBool(rec)){

        var out_res : Integer
        var out_elem : Integer
        var out_i : Integer
        var out_j : Integer
        var out_n : Integer
        var out_row : Integer
        var out_sum : Integer
        var out_label : Boolean
        var out_rec : Boolean

        insert_pr = @indCteScanGetInsertTempTablePR(&cte_scan)

        if(!@sqlToBool(label)){
            var if_result11_res : Integer
            var if_result11_elem : Integer
            var if_result11_i : Integer
            var if_result11_j : Integer
            var if_result11_n : Integer
            var if_result11_row : Integer
            var if_result11_sum : Integer
            var if_result11_label : Boolean
            var if_result11_rec : Boolean

            var pred_2 = i <= n

            if(!@sqlToBool(pred_2)){
            //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
            //        sample_out.col1 = @intToSql(6578)

               var bool_98 = @boolToSql(false)
               if_result11_rec = bool_98
               bool_98 = @initSqlNull(&bool_98)
               if_result11_label = bool_98
               if_result11_res = sum
               if_result11_elem = elem
               if_result11_i = i
               if_result11_j = j
               if_result11_n = n
               if_result11_row = row
               if_result11_sum = sum
            } else {
            //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
            //                    sample_out.col1 = @intToSql(212119)

               var bool_198 = @boolToSql(true)
               if_result11_rec = bool_198
               //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
               //if(@sqlToBool(bool_198)){
               //                    sample_out.col1 = @intToSql(11)
               //                    }else{
               //                        sample_out.col1 = @intToSql(22)
               //                    }
               if_result11_label = bool_198
               var int_198 : Integer
               int_198 = @initSqlNull(&int_198)
               if_result11_res = int_198
               if_result11_elem = elem
               if_result11_i = i
               int_198 = @intToSql(1)
               if_result11_j = int_198
               if_result11_n = n
               int_198 = @intToSql(0)
               if_result11_row = int_198
               if_result11_sum = sum
            }
            out_res = if_result11_res
            out_elem = if_result11_elem
            out_i = if_result11_i
            out_j = if_result11_j
            out_n = if_result11_n
            out_row = if_result11_row
            out_sum = if_result11_sum
            out_label = if_result11_label
            out_rec = if_result11_rec
            //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
            //if(@sqlToBool(if_result11_rec)){
            //        sample_out.col1 = @intToSql(11)
            //        }else{
            //            sample_out.col1 = @intToSql(22)
            //        }
        } else {
            var if_result17_res : Integer
            var if_result17_elem : Integer
            var if_result17_i : Integer
            var if_result17_j : Integer
            var if_result17_n : Integer
            var if_result17_row : Integer
            var if_result17_sum : Integer
            var if_result17_label : Boolean
            var if_result17_rec : Boolean

            var pred_5 = j <= n
            //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
            //                sample_out.col1 = @intToSql(572932348)
            if(!@sqlToBool(pred_5)){
                var bool_538 = @boolToSql(true)
                if_result17_rec = bool_538
                bool_538 = @boolToSql(false)
                if_result17_label = bool_538
                var int_538 : Integer
                int_538 = @initSqlNull(&int_538)
                if_result17_res = int_538
                if_result17_elem = elem
                if_result17_i = i + @intToSql(1)
                if_result17_j = j
                if_result17_n = n
                if_result17_row = row
                if_result17_sum = sum
            } else {
                //var sample_out2 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                //sample_out2.col1 = @intToSql(572938)
                //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                //sample_out.col1 = @intT


                var if_result20_res : Integer
                var if_result20_elem : Integer
                var if_result20_i : Integer
                var if_result20_j : Integer
                var if_result20_n : Integer
                var if_result20_row : Integer
                var if_result20_sum : Integer
                var if_result20_label : Boolean
                var if_result20_rec : Boolean

                var q : QueryState
                var queryState = &q
                queryState.execCtx = exec_ctx

                var pstate : PQuery22_State
                var pipelineState = &pstate
                Query2_Pipeline2_InitPipelineState(queryState, pipelineState)
                @aggInit(&queryState.aggs.agg_term_attr0)

                var tviBase36: TableVectorIterator
                    var tvi155 = &tviBase36
                    var col_oids155: [3]uint32
                    col_oids155[0] = 1
                    col_oids155[1] = 3
                    col_oids155[2] = 2
                    var index_iter1: IndexIterator
                    @indexIteratorInit(&index_iter1, exec_ctx, 1, 1002, 1004, col_oids155)
                    var lo_index_pr1 = @indexIteratorGetLoPR(&index_iter1)
                    var hi_index_pr1 = @indexIteratorGetHiPR(&index_iter1)
                    @prSetIntNull(lo_index_pr1, 0, i)
                    @prSetIntNull(hi_index_pr1, 0, j)
                    for (@indexIteratorScanAscending(&index_iter1, 0, 0); @indexIteratorAdvance(&index_iter1); ) {
                        var table_pr1 = @indexIteratorGetTablePR(&index_iter1)
                        var slot1 = @indexIteratorGetSlot(&index_iter1)
                        if (@sqlToBool(@prGetIntNull(table_pr1, 0) == i)) {


                        var col_oids306: [3]uint32
                        col_oids306[0] = 3
                        col_oids306[1] = 1
                        col_oids306[2] = 2

                        var index_iter: IndexIterator
                        @indexIteratorInit(&index_iter, queryState.execCtx, 2, 1003, 1005, col_oids306)
                        var lo_index_pr = @indexIteratorGetLoPR(&index_iter)
                        var hi_index_pr = @indexIteratorGetHiPR(&index_iter)
                        @prSetIntNull(lo_index_pr, 1, j)
                        @prSetIntNull(lo_index_pr, 0, @prGetIntNull(table_pr1, 1))
                        @prSetIntNull(hi_index_pr, 1, j)
                        @prSetIntNull(hi_index_pr, 0, @prGetIntNull(table_pr1, 1))
                        for (@indexIteratorScanKey(&index_iter); @indexIteratorAdvance(&index_iter); ) {
                            var table_pr = @indexIteratorGetTablePR(&index_iter)
                            if (@sqlToBool(@prGetIntNull(table_pr, 1) == j)
                                    and @sqlToBool(@prGetIntNull(table_pr1, 1) == @prGetIntNull(table_pr, 0))) {
                                    var aggValues: AggValues
                                    //var sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                                    //sample_out.col1 = @prGetIntNull(table_pr1, 2)
                                    //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                                    //sample_out.col1 = @prGetIntNull(table_pr, 2)
                                    aggValues.agg_term_attr0 = @prGetIntNull(table_pr1, 2) * @prGetIntNull(table_pr, 2)
                                    @aggAdvance(&queryState.aggs.agg_term_attr0, &aggValues.agg_term_attr0)
                                    //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                                    //sample_out.col1 = @aggResult(&queryState.aggs.agg_term_attr0)
                            }
                                }
                                @indexIteratorFree(&index_iter)
                            }
                    }
                    @indexIteratorFree(&index_iter1)

                   Query2_Pipeline2_TearDownPipelineState(queryState, pipelineState)

                   var aggRow = &queryState.aggs
                   var output = @aggResult(&aggRow.agg_term_attr0)
                   //var sample_out1 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                   //sample_out1.col1 = @intToSql(9870)
                   //sample_out1 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                   //sample_out1.col1 = output
                   var q6_5 = @isValNull(output)
                   @aggReset(&queryState.aggs.agg_term_attr0)
                   if (q6_5) {
                    var bool_138 = @boolToSql(true)
                    if_result20_rec = bool_138
                    if_result20_label = bool_138
                    var int_138 : Integer
                    int_138 = @initSqlNull(&int_138)
                    if_result20_res = int_138
                    if_result20_elem = @intToSql(0)
                    if_result20_i = i
                    if_result20_j = j + @intToSql(1)
                    if_result20_n = n
                    if_result20_row = row
                    if_result20_sum = sum
                   } else {
                    var bool_238 = @boolToSql(true)
                    if_result20_rec = bool_238
                    if_result20_label = bool_238
                    var int_238 : Integer
                    int_238 = @initSqlNull(&int_238)
                    if_result20_res = int_238
                    if_result20_elem = output
                    if_result20_i = i
                    if_result20_j = j + @intToSql(1)
                    if_result20_n = n
                    if_result20_row = output
                    if_result20_sum = sum + output * output
                   }
                   if_result17_rec = if_result20_rec
                   if_result17_label = if_result20_label
                   if_result17_res = if_result20_res
                   if_result17_elem = if_result20_elem
                   if_result17_i = if_result20_i
                   if_result17_j = if_result20_j
                   if_result17_n = if_result20_n
                   if_result17_row = if_result20_row
                   if_result17_sum = if_result20_sum
            }
            
            out_res = if_result17_res
            out_elem = if_result17_elem
            out_i = if_result17_i
            out_j = if_result17_j
            out_n = if_result17_n
            out_row = if_result17_row
            out_sum = if_result17_sum
            out_label = if_result17_label
            out_rec = if_result17_rec
        }


        @prSetIntNull(insert_pr, 0, out_res)
        @prSetIntNull(insert_pr, 1, out_elem)
        @prSetIntNull(insert_pr, 2, out_i)
        @prSetIntNull(insert_pr, 3, out_j)
        @prSetIntNull(insert_pr, 4, out_n)
        @prSetIntNull(insert_pr, 5, out_row)
        @prSetIntNull(insert_pr, 6, out_sum)
        @prSetBoolNull(insert_pr, 7, out_label)
        @prSetBoolNull(insert_pr, 8, out_rec)

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
      var resres = @vpiGetIntNull(vpi, 0)
      //var recrec = @vpiGetBool(vpi, 4)
      //if(@sqlToBool(recrec)){
        out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        out.col1 = resres
      //}
    }
    @vpiReset(vpi)
  }
  @tableIterClose(&tvi)
  @indCteScanFree(&cte_scan)

  @resultBufferFinalize(output_buffer)
  @resultBufferFree(output_buffer)
  return ret
}
