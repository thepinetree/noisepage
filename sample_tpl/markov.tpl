struct output_struct {
  col1: Integer
  col2: Real
}

struct SortRow {
    attr0: Integer
    attr1: Date
}

struct AggPayload {
    agg_term_attr0: RealMinAggregate
}

struct AggValues {
    agg_term_attr0: Real
}

fun Query0_Pipeline2_FilterClause(execCtx: *ExecutionContext, vp: *VectorProjection, tids: *TupleIdList, context: *uint8) -> nil {
// todo change
    var filter_val: Integer = @intToSql(2)
    @filterEq(execCtx, vp, 0, filter_val, tids)
    return
}

fun Compare(lhs: *SortRow, rhs: *SortRow) -> int32 {
    if (@sqlToBool(lhs.attr1 < rhs.attr1)) {
        return -1
    }
    if (@sqlToBool(lhs.attr1 > rhs.attr1)) {
        return 1
    }
    return 0
}

struct QueryState {
    execCtx: *ExecutionContext
    sorter : Sorter
}

fun main(exec_ctx: *ExecutionContext) -> int32 {
  // Initialize CTE Scan Iterator

  var output_buffer = @resultBufferNew(exec_ctx)

  var query_state : QueryState
  var ret : int32

  var col_types: [11]uint32
  col_types[0] = 4  // res
  col_types[1] = 4  // buy
  col_types[2] = 8  // cheapest
  col_types[3] = 4  // cheapest_order
  col_types[4] = 8  // margin
  col_types[5] = 4  // partkey
  col_types[6] = 8  // profit
  col_types[7] = 4  // sell
  col_types[8] = 4  // this_order_k
  col_types[9] = 9  // this_order_d
  col_types[10] = 1  // rec?

  var out : *output_struct

  var m : Integer
  var TEMP_OID_MASK: uint32 = 2147483647 + 1                       // 2^31
  var temp_col_oids: [11]uint32
  temp_col_oids[0] = TEMP_OID_MASK | 1 // res
  temp_col_oids[1] = TEMP_OID_MASK | 2 // result
  temp_col_oids[2] = TEMP_OID_MASK | 3 // s
  temp_col_oids[3] = TEMP_OID_MASK | 4 // x
  temp_col_oids[4] = TEMP_OID_MASK | 5 // rec?
  temp_col_oids[5] = TEMP_OID_MASK | 6 // rec?
  temp_col_oids[6] = TEMP_OID_MASK | 7 // rec?
  temp_col_oids[7] = TEMP_OID_MASK | 8 // rec?
  temp_col_oids[8] = TEMP_OID_MASK | 9 // rec?
  temp_col_oids[9] = TEMP_OID_MASK | 10 // rec?
  temp_col_oids[10] = TEMP_OID_MASK | 11 // rec?

  var cte_scan: IndCteScanIterator

  var partkeys_tviBase_2: TableVectorIterator
  var partkeys_tvi_2 = &partkeys_tviBase_2
  var partkeys_col_oids_2: [1]uint32
  var part_col_oid = @testCatalogLookup(exec_ctx, "part", "p_partkey")
  var partkey_table_oid = @testCatalogLookup(exec_ctx, "part", "")
  partkeys_col_oids_2[0] = part_col_oid

  var lineitem_oid = @testCatalogLookup(exec_ctx, "lineitem", "")
  var l_orderkey_oid = @testCatalogLookup(exec_ctx, "lineitem", "l_orderkey")
  var l_extendedprice_oid = @testCatalogLookup(exec_ctx, "lineitem", "l_extendedprice")
  var l_discount_oid = @testCatalogLookup(exec_ctx, "lineitem", "l_discount")
  var l_tax_oid = @testCatalogLookup(exec_ctx, "lineitem", "l_tax")
  var l_partkey_oid = @testCatalogLookup(exec_ctx, "lineitem", "l_partkey")

  var orders_oid = @testCatalogLookup(exec_ctx, "orders", "")
  var o_orderkey_oid = @testCatalogLookup(exec_ctx, "orders", "o_orderkey")
  var o_orderdate_oid = @testCatalogLookup(exec_ctx, "orders", "o_orderdate")

  var lineitem_l_partkey_l_orderkey_oid = @testCatalogIndexLookup(exec_ctx, "lineitem_l_partkey_l_orderkey")
  var orders_o_orderdate_o_orderkey_oid = @testCatalogIndexLookup(exec_ctx, "orders_o_orderdate_o_orderkey")
  var l_pk_oid = 1020

  @tableIterInit(partkeys_tvi_2, exec_ctx, partkey_table_oid, partkeys_col_oids_2)
  var count = 0
  for(@tableIterAdvance(partkeys_tvi_2) and (count < 1)){
      var partkey_vpi_2 = @tableIterGetVPI(partkeys_tvi_2)
      for (; @vpiHasNext(partkey_vpi_2); @vpiAdvance(partkey_vpi_2)) {
      var input_partkey = @vpiGetInt(partkey_vpi_2, 0)
      //var sample_out111 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
      //sample_out111.col1 = @intToSql(123456)
      //sample_out111 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
      //sample_out111.col1 = input_partkey


  @indCteScanInit(&cte_scan, exec_ctx, TEMP_OID_MASK, temp_col_oids, col_types, false)


  var out_tvi: TableVectorIterator

  var insert_pr = @indCteScanGetInsertTempTablePR(&cte_scan)
  var integer_ins : Integer
  var real_ins : Real
  var date_ins : Date
  var init_partkey = input_partkey

  var null_val = @initSqlNull(&integer_ins)
  @prSetIntNull(insert_pr, 0, null_val)
  integer_ins = @initSqlNull(&integer_ins)
  @prSetIntNull(insert_pr, 1, null_val)
  var null_real = @initSqlNull(&real_ins)
  @prSetRealNull(insert_pr, 2, null_real)
  integer_ins = @initSqlNull(&integer_ins)
  @prSetIntNull(insert_pr, 3, null_val)
  real_ins = @initSqlNull(&real_ins)
  @prSetRealNull(insert_pr, 4, null_real)

  real_ins = @initSqlNull(&real_ins)
  @prSetIntNull(insert_pr, 5, init_partkey)

  real_ins = @initSqlNull(&real_ins)
  @prSetRealNull(insert_pr, 6, null_real)
  integer_ins = @initSqlNull(&integer_ins)
  @prSetIntNull(insert_pr, 7, null_val)
  integer_ins = @initSqlNull(&integer_ins)
  @prSetIntNull(insert_pr, 8, null_val)


    //SELECT * FROM (SELECT "RTE1"."o_orderkey" AS "rowk",
    //                                    "RTE1"."o_orderdate" AS "rowb"
    //                            FROM lineitem AS "RTE0",
    //                                 orders AS "RTE1"
    //                            WHERE ("RTE0"."l_orderkey" = "RTE1"."o_orderkey"
    //                                   AND
    //                                   "RTE0"."l_partkey" = "partkey")
    //                            ORDER BY ("RTE1"."o_orderdate") ASC
    //                            LIMIT 1) "this_order_1"
    //                   ) AS "let4"("this_order_k", "this_order_d")
  var sorter : Sorter

  @sorterInit(&sorter, exec_ctx, Compare, @sizeOf(SortRow))
  var col_oids1: [2]uint32
      col_oids1[0] = 2
      col_oids1[1] = 1
      var index_iter165: IndexIterator
      @indexIteratorInit(&index_iter165, exec_ctx, 1, lineitem_oid, l_pk_oid, col_oids1)
      var lo_index_pr1165 = @indexIteratorGetLoPR(&index_iter165)
      var hi_index_pr1165 = @indexIteratorGetHiPR(&index_iter165)
      @prSetInt(lo_index_pr1165, 0, input_partkey)
      @prSetInt(lo_index_pr1165, 0, input_partkey)
      @prSetInt(hi_index_pr1165, 0, input_partkey)
      for (@indexIteratorScanAscending(&index_iter165, 0, 0); @indexIteratorAdvance(&index_iter165); ) {
          var table_pr172 = @indexIteratorGetTablePR(&index_iter165)
          var slot1 = @indexIteratorGetSlot(&index_iter165)
          if (@sqlToBool(@prGetInt(table_pr172, 1) == input_partkey)) {
              var col_oids: [2]uint32
              col_oids[0] = 5
              col_oids[1] = 1
              var index_iter178: IndexIterator
              // 1015 is primary key oid?
              @indexIteratorInit(&index_iter178, exec_ctx, 1, orders_oid, 1015, col_oids)
              var lo_index_pr178 = @indexIteratorGetLoPR(&index_iter178)
              var hi_index_pr178 = @indexIteratorGetHiPR(&index_iter178)
              @prSetInt(lo_index_pr178, 0, @prGetInt(table_pr172, 0))
              @prSetInt(hi_index_pr178, 0, @prGetInt(table_pr172, 0))
              //var sample_out3 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
              //        sample_out3.col1 = @intToSql(3201234)
              //        sample_out3 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
              //                sample_out3.col1 = lo_index_pr178
              for (@indexIteratorScanKey(&index_iter178); @indexIteratorAdvance(&index_iter178); ) {
                  var table_pr190 = @indexIteratorGetTablePR(&index_iter178)
                  var slot = @indexIteratorGetSlot(&index_iter178)
                  if (@sqlToBool(@prGetInt(table_pr172, 0) == @prGetInt(table_pr190, 0))) {
                      var sortRow190 = @ptrCast(*SortRow, @sorterInsert(&sorter))
                      sortRow190.attr0 = @prGetInt(table_pr190, 0)
                      sortRow190.attr1 = @prGetDate(table_pr190, 1)
                  }
              }
              @indexIteratorFree(&index_iter178)
          }
      }
      @indexIteratorFree(&index_iter165)

  var base_iterBase: SorterIterator
  var base_iter = &base_iterBase
  @sorterIterInit(base_iter, &sorter)
  var key_val179 : Integer
  key_val179 = @initSqlNull(&key_val179)
  var date_val179 : Date
  date_val179 = @initSqlNull(&date_val179)
  for (; @sorterIterHasNext(base_iter); @sorterIterNext(base_iter)) {
      var sortRow211 = @ptrCast(*SortRow, @sorterIterGetRow(base_iter))
      var sample_out2 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out2.col1 = @intToSql(3201234)
        sample_out2 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                sample_out2.col1 = sortRow211.attr0
      key_val179 = sortRow211.attr0
      date_val179 = sortRow211.attr1
  }
  @sorterIterClose(base_iter)
  @sorterFree(&sorter)

  @prSetIntNull(insert_pr, 8, key_val179)
  @prSetDateNull(insert_pr, 9, date_val179)

  var bool_ins = @boolToSql(true)
  @prSetBool(insert_pr, 10, bool_ins)
  var base_insert_temp_table_slot = @indCteScanTableInsert(&cte_scan)

  //var sample_out220 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
  //sample_out220.col1 = key_val179

  for(@indCteScanAccumulate(&cte_scan)){
    var cte = @indCteScanGetReadCte(&cte_scan)
    var ind_tvi : TableVectorIterator
    @tableIterInit(&ind_tvi, exec_ctx, TEMP_OID_MASK, temp_col_oids)
    //var sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
    //                 sample_out.col1 = @intToSql(204)
    for(@tableIterAdvance(&ind_tvi)){
    //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
    //sample_out.col1 = @intToSql(207)
      var ind_vpi = @tableIterGetVPI(&ind_tvi)
      for(; @vpiHasNext(ind_vpi); @vpiAdvance(ind_vpi)){
      // FROM run AS "run"("rec?", "res", "result", "s", "x"),
        var res = @vpiGetIntNull(ind_vpi, 9)
        var buy = @vpiGetIntNull(ind_vpi, 0)
        var cheapest = @vpiGetRealNull(ind_vpi, 1)
        var cheapest_order = @vpiGetIntNull(ind_vpi, 2)
        var margin = @vpiGetRealNull(ind_vpi, 3)
        var partkey = @vpiGetIntNull(ind_vpi, 4)
        var profit = @vpiGetRealNull(ind_vpi, 5)
        var sell = @vpiGetIntNull(ind_vpi, 6)
        var this_order_k = @vpiGetIntNull(ind_vpi, 7)
        var this_order_d = @vpiGetDateNull(ind_vpi, 8)
        var rec = @vpiGetBool(ind_vpi, 10)

        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = res
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = buy
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col2 = cheapest
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = cheapest_order
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col2 = margin
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = partkey
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col2 = profit
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = sell
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = this_order_k
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = @vpiGetIntNull(ind_vpi, 8)
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = @vpiGetIntNull(ind_vpi, 10)
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //sample_out.col1 = @intToSql(7896)

        if(@sqlToBool(rec)){
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //                     sample_out.col1 = @intToSql(254)
        insert_pr = @indCteScanGetInsertTempTablePR(&cte_scan)
        var pred_2 = !@isValNull(this_order_k)
        var if_result7_res : Integer
        var if_result7_buy : Integer
        var if_result7_cheapest : Real
        var if_result7_cheapest_order : Integer
        var if_result7_margin : Real
        var if_result7_partkey : Integer
        var if_result7_profit : Real
        var if_result7_sell : Integer
        var if_result7_this_order_k : Integer
        var if_result7_this_order_d : Date
        var if_result7_rec : Boolean
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //                                             sample_out.col1 = @intToSql(999999)
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //                                     sample_out.col1 = @vpiGetIntNull(ind_vpi, 7)
        if(pred_2){
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //                             sample_out.col1 = @intToSql(276)
            var if_result11_res : Integer
            var if_result11_buy : Integer
            var if_result11_cheapest : Real
            var if_result11_cheapest_order : Integer
            var if_result11_margin : Real
            var if_result11_partkey : Integer
            var if_result11_profit : Real
            var if_result11_sell : Integer
            var if_result11_this_order_k : Integer
            var if_result11_this_order_d : Date
            var if_result11_rec : Boolean

            var price_3 : Real
            var ir11_tviBase_2: TableVectorIterator
            var ir11_tvi_2 = &ir11_tviBase_2
            var if_result_11_col_oids_2: [5]uint32
            if_result_11_col_oids_2[0] = 1
            if_result_11_col_oids_2[1] = 6
            if_result_11_col_oids_2[2] = 2
            if_result_11_col_oids_2[3] = 7
            if_result_11_col_oids_2[4] = 8
            var agg_index_iter: IndexIterator
            @indexIteratorInit(&agg_index_iter, exec_ctx, 2, lineitem_oid, lineitem_l_partkey_l_orderkey_oid,
            if_result_11_col_oids_2)
            var index_pr = @indexIteratorGetPR(&agg_index_iter)
            var setval: Integer = partkey
            @prSetInt(index_pr, 0, setval)
            var setval1: Integer = this_order_k
            @prSetInt(index_pr, 1, setval1)
            var slot_2 : TupleSlot
            var aggs : AggPayload
            //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
            //sample_out.col1 = @intToSql(268)
            for (@indexIteratorScanKey(&agg_index_iter); @indexIteratorAdvance(&agg_index_iter); ) {
                var table_pr = @indexIteratorGetTablePR(&agg_index_iter)
                //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                //            sample_out.col1 = @intToSql(271)
                 //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 //sample_out.col1 = @vpiGetInt(vpi_2, 0)
                 //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 //sample_out.col2 = @vpiGetReal(vpi_2, 1)
                 //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 //sample_out.col1 = @vpiGetInt(vpi_2, 2)
                 //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 //sample_out.col2 = @vpiGetReal(vpi_2, 3)
                 //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 //sample_out.col2 = @vpiGetReal(vpi_2, 4)
                 //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 //sample_out.col1 = @intToSql(5555)
                 //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 //sample_out.col1 = this_order_k
                 //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 //sample_out.col1 = @vpiGetInt(vpi_2, 0)
                 //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 //sample_out.col1 = @vpiGetInt(vpi_2, 2)
                 //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 //sample_out.col1 = partkey

                 if(@sqlToBool(@prGetInt(table_pr, 4) == this_order_k)
                      and (@sqlToBool(@prGetInt(table_pr, 3) == partkey))){
                      var aggValues: AggValues
                      aggValues.agg_term_attr0 = @prGetDouble(table_pr, 0) * @intToSql(1)
                        - @prGetDouble(table_pr, 1) * @intToSql(1) + @prGetDouble(table_pr, 2)
                      @aggAdvance(&aggs.agg_term_attr0, &aggValues.agg_term_attr0)
                  }
            }
            @indexIteratorFree(&agg_index_iter)
            price_3 = @aggResult(&aggs.agg_term_attr0)
            //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
            //sample_out.col1 = @intToSql(325)
            //sample_out.col2 = price_3

            //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
            //            sample_out.col1 = @intToSql(5000)

            var cheapest_4 = cheapest
            if(@isValNull(cheapest_4)){
                cheapest_4 = price_3
            }

            var q5_3 = price_3 <= cheapest_4
            if(@sqlToBool(q5_3)){
                var if_result17_res : Integer
                var if_result17_buy : Integer
                var if_result17_cheapest : Real
                var if_result17_cheapest_order : Integer
                var if_result17_margin : Real
                var if_result17_partkey : Integer
                var if_result17_profit : Real
                var if_result17_sell : Integer
                var if_result17_this_order_k : Integer
                var if_result17_this_order_d : Date
                var if_result17_rec : Boolean

                var cheapest_9 = price_3
                var cheapest_order_7 = this_order_k
                var profit_4 = price_3 - price_3

                var margin_5 = margin
                if(@isValNull(margin_5)){
                    margin_5 = profit_4
                 }
                 var q9_4 = profit_4 >= margin_5
                 if(@sqlToBool(q9_4)){
                   if_result17_res = @initSqlNull(&if_result17_res)
                   //var sample_out397 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                   //      sample_out397.col1 = @intToSql(397)
                   //      sample_out397 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                   //      sample_out397.col1 = cheapest_order_7

                   if_result17_buy = cheapest_order_7
                   var buy_6 = cheapest_order_7
                   if_result17_cheapest = cheapest_9
                   if_result17_cheapest_order = cheapest_order_7
                   if_result17_margin = profit_4
                   var margin_7 = profit_4
                   if_result17_partkey = partkey
                   if_result17_profit = profit_4
                   if_result17_sell = this_order_k
                   var sell_6 = this_order_k
                   if_result17_rec = @boolToSql(true)


                     // line 99


                   @sorterInit(&sorter, exec_ctx, Compare, @sizeOf(SortRow))

                  var col_oids1: [2]uint32
                        col_oids1[0] = 2
                        col_oids1[1] = 1
                        var index_iter435: IndexIterator
                        @indexIteratorInit(&index_iter435, exec_ctx, 1, lineitem_oid, l_pk_oid, col_oids1)
                        var lo_index_pr1435 = @indexIteratorGetLoPR(&index_iter435)
                        var hi_index_pr1435 = @indexIteratorGetHiPR(&index_iter435)
                        @prSetInt(lo_index_pr1435, 0, partkey)
                        @prSetInt(hi_index_pr1435, 0, partkey)
                        for (@indexIteratorScanAscending(&index_iter435, 0, 0); @indexIteratorAdvance(&index_iter435); ) {
                            var table_pr443 = @indexIteratorGetTablePR(&index_iter435)
                            var slot1 = @indexIteratorGetSlot(&index_iter435)
                            if (@sqlToBool(@prGetInt(table_pr443, 1) == partkey)) {
                                var col_oids: [2]uint32
                                col_oids[0] = 5
                                col_oids[1] = 1
                                var index_iter448: IndexIterator
                                // 1015 is primary key oid?
                                @indexIteratorInit(&index_iter448, exec_ctx, 1, orders_oid, 1015, col_oids)
                                var lo_index_pr448 = @indexIteratorGetLoPR(&index_iter448)
                                var hi_index_pr448 = @indexIteratorGetHiPR(&index_iter448)
                                @prSetInt(lo_index_pr448, 0, @prGetInt(table_pr443, 0))
                                @prSetInt(hi_index_pr448, 0, @prGetInt(table_pr443, 0))
                                for (@indexIteratorScanKey(&index_iter448); @indexIteratorAdvance(&index_iter448); ) {
                                    var table_pr456 = @indexIteratorGetTablePR(&index_iter448)
                                    var slot = @indexIteratorGetSlot(&index_iter448)
                                    if (@sqlToBool(@prGetInt(table_pr443, 0) == @prGetInt(table_pr456, 0))
                                    and @sqlToBool(@prGetDate(table_pr456, 1) > this_order_d)) {
                                        var sortRow456 = @ptrCast(*SortRow, @sorterInsert(&sorter))
                                        sortRow456.attr0 = @prGetInt(table_pr456, 0)
                                        sortRow456.attr1 = @prGetDate(table_pr456, 1)
                                    }
                                }
                                @indexIteratorFree(&index_iter448)
                            }
                        }
                        @indexIteratorFree(&index_iter435)

                   var iterBase17: SorterIterator
                   var iter17 = &iterBase17
                   @sorterIterInit(iter17, &sorter)
                   if_result17_this_order_k = @initSqlNull(&if_result17_this_order_k)
                   if_result17_this_order_d = @initSqlNull(&if_result17_this_order_d)
                   for (; @sorterIterHasNext(iter17); @sorterIterNext(iter17)) {
                       var sortRow475 = @ptrCast(*SortRow, @sorterIterGetRow(iter17))
                       if_result17_this_order_k = sortRow475.attr0
                       if_result17_this_order_d = sortRow475.attr1
                   }
                   @sorterIterClose(iter17)
                   @sorterFree(&sorter)
                 }else{
                     if_result17_res = @initSqlNull(&if_result17_res)
                     if_result17_buy = buy
                     if_result17_cheapest = cheapest_9
                     if_result17_cheapest_order = cheapest_order_7
                     if_result17_margin = margin_5
                     if_result17_partkey = partkey
                     if_result17_profit = profit_4
                     if_result17_sell = sell
                     if_result17_rec = @boolToSql(true)
                     @sorterInit(&sorter, exec_ctx, Compare, @sizeOf(SortRow))
                     var col_oids1: [2]uint32
                           col_oids1[0] = 2
                           col_oids1[1] = 1
                           var index_iter495: IndexIterator
                           @indexIteratorInit(&index_iter495, exec_ctx, 1, lineitem_oid, l_pk_oid, col_oids1)
                           var lo_index_pr1495 = @indexIteratorGetLoPR(&index_iter495)
                           var hi_index_pr1495 = @indexIteratorGetHiPR(&index_iter495)
                           @prSetInt(lo_index_pr1495, 0, partkey)
                           @prSetInt(hi_index_pr1495, 0, partkey)
                           for (@indexIteratorScanAscending(&index_iter495, 0, 0); @indexIteratorAdvance(&index_iter495); ) {
                               var table_pr5021 = @indexIteratorGetTablePR(&index_iter495)
                               var slot1 = @indexIteratorGetSlot(&index_iter495)
                               if (@sqlToBool(@prGetInt(table_pr5021, 1) == partkey)) {
                                   var col_oids: [2]uint32
                                   col_oids[0] = 5
                                   col_oids[1] = 1
                                   var index_iter508: IndexIterator
                                   // 1015 is primary key oid?
                                   @indexIteratorInit(&index_iter508, exec_ctx, 1, orders_oid, 1015, col_oids)
                                   var lo_index_pr508 = @indexIteratorGetLoPR(&index_iter508)
                                   var hi_index_pr508 = @indexIteratorGetHiPR(&index_iter508)
                                   @prSetInt(lo_index_pr508, 0, @prGetInt(table_pr5021, 0))
                                   @prSetInt(hi_index_pr508, 0, @prGetInt(table_pr5021, 0))
                                   for (@indexIteratorScanKey(&index_iter508); @indexIteratorAdvance(&index_iter508); ) {
                                       var table_pr502 = @indexIteratorGetTablePR(&index_iter508)
                                       var slot = @indexIteratorGetSlot(&index_iter508)
                                       if (@sqlToBool(@prGetInt(table_pr5021, 0) == @prGetInt(table_pr502, 0))
                                           and @sqlToBool(@prGetDate(table_pr502, 1) > this_order_d)) {
                                           var sortRow502 = @ptrCast(*SortRow, @sorterInsert(&sorter))
                                           sortRow502.attr0 = @prGetInt(table_pr502, 0)
                                           sortRow502.attr1 = @prGetDate(table_pr502, 1)
                                       }
                                   }
                                   @indexIteratorFree(&index_iter508)
                               }
                           }
                           @indexIteratorFree(&index_iter495)

                     var iterBase2: SorterIterator
                     var iter2 = &iterBase2
                     if_result17_this_order_k = @initSqlNull(&if_result17_this_order_k)
                     if_result17_this_order_d = @initSqlNull(&if_result17_this_order_d)
                     @sorterIterInit(iter2, &sorter)
                     for (; @sorterIterHasNext(iter2); @sorterIterNext(iter2)) {
                         var sortRow535 = @ptrCast(*SortRow, @sorterIterGetRow(iter2))
                         if_result17_this_order_k = sortRow535.attr0
                         if_result17_this_order_d = sortRow535.attr1
                     }
                     @sorterIterClose(iter2)
                     @sorterFree(&sorter)
                 }
                 if_result11_res = if_result17_res
                 if_result11_buy = if_result17_buy
                 if_result11_cheapest = if_result17_cheapest
                 if_result11_cheapest_order = if_result17_cheapest_order
                 if_result11_margin = if_result17_margin
                 if_result11_partkey = if_result17_partkey
                 if_result11_profit = if_result17_profit
                 if_result11_sell = if_result17_sell
                 if_result11_this_order_k = if_result17_this_order_k
                 if_result11_this_order_d = if_result17_this_order_d
                 if_result11_rec = if_result17_rec
                }else{
                //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                //                             sample_out.col1 = @intToSql(509)
                    var if_result17_res : Integer
                    var if_result17_buy : Integer
                    var if_result17_cheapest : Real
                    var if_result17_cheapest_order : Integer
                    var if_result17_margin : Real
                    var if_result17_partkey : Integer
                    var if_result17_profit : Real
                    var if_result17_sell : Integer
                    var if_result17_this_order_k : Integer
                    var if_result17_this_order_d : Date
                    var if_result17_rec : Boolean

                    var cheapest_9 = price_3
                    var cheapest_order_7 = this_order_k
                    var profit_4 = price_3 - price_3

                    var margin_5 = margin
                    if(@isValNull(margin_5)){
                       margin_5 = profit_4
                    }
                    var q9_4 = profit_4 >= margin_5
                    //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                    //                        sample_out.col2 = profit_4

                    if(@sqlToBool(q9_4)){
                        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                        //sample_out.col1 = @intToSql(528)
                        if_result17_res = @initSqlNull(&if_result17_res)
                        //var sample_out397 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                        //                         sample_out397.col1 = @intToSql(397)
                        //                         sample_out397 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                        //                         sample_out397.col1 = cheapest_order_7
                        if_result17_buy = cheapest_order_7
                        if_result17_cheapest = cheapest_9
                        if_result17_cheapest_order = cheapest_order_7
                        if_result17_margin = profit_4
                        if_result17_partkey = partkey
                        if_result17_profit = profit_4
                        if_result17_sell = this_order_k
                        if_result17_rec = @boolToSql(true)

                        // line 99

                        var buy_6 = cheapest_order_7
                        var sell_6 = this_order_k

                        @sorterInit(&sorter, exec_ctx, Compare, @sizeOf(SortRow))
                        var col_oids1: [2]uint32
                          col_oids1[0] = 2
                          col_oids1[1] = 1
                          var index_iter606: IndexIterator
                          @indexIteratorInit(&index_iter606, exec_ctx, 1, lineitem_oid, l_pk_oid, col_oids1)
                          var lo_index_pr1606 = @indexIteratorGetLoPR(&index_iter606)
                          var hi_index_pr1606 = @indexIteratorGetHiPR(&index_iter606)
                          @prSetInt(lo_index_pr1606, 0, partkey)
                          @prSetInt(hi_index_pr1606, 0, partkey)
                          for (@indexIteratorScanAscending(&index_iter606, 0, 0); @indexIteratorAdvance(&index_iter606); ) {
                              var table_pr6131 = @indexIteratorGetTablePR(&index_iter606)
                              var slot1 = @indexIteratorGetSlot(&index_iter606)
                              if (@sqlToBool(@prGetInt(table_pr6131, 1) == partkey)) {
                                  var col_oids: [2]uint32
                                  col_oids[0] = 5
                                  col_oids[1] = 1
                                  var index_iter619: IndexIterator
                                  // 1015 is primary key oid?
                                  @indexIteratorInit(&index_iter619, exec_ctx, 1, orders_oid, 1015, col_oids)
                                  var lo_index_pr619 = @indexIteratorGetLoPR(&index_iter619)
                                  var hi_index_pr619 = @indexIteratorGetHiPR(&index_iter619)
                                  @prSetInt(lo_index_pr619, 0, @prGetInt(table_pr6131, 0))
                                  @prSetInt(hi_index_pr619, 0, @prGetInt(table_pr6131, 0))
                                  for (@indexIteratorScanKey(&index_iter619); @indexIteratorAdvance(&index_iter619); ) {
                                      var table_pr613 = @indexIteratorGetTablePR(&index_iter619)
                                      var slot = @indexIteratorGetSlot(&index_iter619)
                                      if (@sqlToBool(@prGetInt(table_pr6131, 0) == @prGetInt(table_pr613, 0))
                                          and @sqlToBool(@prGetDate(table_pr613, 1) > this_order_d)) {
                                          var sortRow613 = @ptrCast(*SortRow, @sorterInsert(&sorter))
                                          sortRow613.attr0 = @prGetInt(table_pr613, 0)
                                          sortRow613.attr1 = @prGetDate(table_pr613, 1)
                                      }
                                  }
                                  @indexIteratorFree(&index_iter619)
                              }
                          }
                          @indexIteratorFree(&index_iter606)

                          var iterBase: SorterIterator
                          var iter646 = &iterBase
                          @sorterIterInit(iter646, &sorter)
                          if_result17_this_order_k = @initSqlNull(&if_result17_this_order_k)
                          if_result17_this_order_d = @initSqlNull(&if_result17_this_order_d)
                          for (; @sorterIterHasNext(iter646); @sorterIterNext(iter646)) {
                              var sortRow646 = @ptrCast(*SortRow, @sorterIterGetRow(iter646))
                              if_result17_this_order_k = sortRow646.attr0
                              if_result17_this_order_d = sortRow646.attr1
                          }
                          @sorterIterClose(iter646)
                          @sorterFree(&sorter)
                    }else{
                    if_result17_res = @initSqlNull(&if_result17_res)
                    if_result17_buy = buy
                    if_result17_cheapest = cheapest_4
                    if_result17_cheapest_order = cheapest_order
                    if_result17_margin = margin_5
                    if_result17_partkey = partkey
                    if_result17_profit = profit_4
                    if_result17_sell = sell
                    if_result17_rec = @boolToSql(true)
                    @sorterInit(&sorter, exec_ctx, Compare, @sizeOf(SortRow))
                    var col_oids1: [2]uint32
                      col_oids1[0] = 2
                      col_oids1[1] = 1
                      var index_iter666: IndexIterator
                      @indexIteratorInit(&index_iter666, exec_ctx, 1, lineitem_oid, l_pk_oid, col_oids1)
                      var lo_index_pr1666 = @indexIteratorGetLoPR(&index_iter666)
                      var hi_index_pr1666 = @indexIteratorGetHiPR(&index_iter666)
                      @prSetInt(lo_index_pr1666, 0, partkey)
                      @prSetInt(hi_index_pr1666, 0, partkey)
                      for (@indexIteratorScanAscending(&index_iter666, 0, 0); @indexIteratorAdvance(&index_iter666); ) {
                          var table_pr6731 = @indexIteratorGetTablePR(&index_iter666)
                          var slot1 = @indexIteratorGetSlot(&index_iter666)
                          if (@sqlToBool(@prGetInt(table_pr6731, 1) == partkey)) {
                              var col_oids: [2]uint32
                              col_oids[0] = 5
                              col_oids[1] = 1
                              var index_iter679: IndexIterator
                              // 1015 is primary key oid?
                              @indexIteratorInit(&index_iter679, exec_ctx, 1, orders_oid, 1015, col_oids)
                              var lo_index_pr679 = @indexIteratorGetLoPR(&index_iter679)
                              var hi_index_pr679 = @indexIteratorGetHiPR(&index_iter679)
                              @prSetInt(lo_index_pr679, 0, @prGetInt(table_pr6731, 0))
                              @prSetInt(hi_index_pr679, 0, @prGetInt(table_pr6731, 0))
                              for (@indexIteratorScanKey(&index_iter679); @indexIteratorAdvance(&index_iter679); ) {
                                  var table_pr673 = @indexIteratorGetTablePR(&index_iter679)
                                  var slot = @indexIteratorGetSlot(&index_iter679)
                                  if (@sqlToBool(@prGetInt(table_pr6731, 0) == @prGetInt(table_pr673, 0))
                                        and @sqlToBool(@prGetDate(table_pr673, 1) > this_order_d)) {
                                      var sortRow673 = @ptrCast(*SortRow, @sorterInsert(&sorter))
                                      sortRow673.attr0 = @prGetInt(table_pr673, 0)
                                      sortRow673.attr1 = @prGetDate(table_pr673, 1)
                                  }
                              }
                              @indexIteratorFree(&index_iter679)
                          }
                      }
                      @indexIteratorFree(&index_iter666)

                        var iterBase: SorterIterator
                        var iter706 = &iterBase
                        @sorterIterInit(iter706, &sorter)
                        if_result17_this_order_k = @initSqlNull(&if_result17_this_order_k)
                        if_result17_this_order_d = @initSqlNull(&if_result17_this_order_d)
                        for (; @sorterIterHasNext(iter706); @sorterIterNext(iter706)) {
                            var sortRow706 = @ptrCast(*SortRow, @sorterIterGetRow(iter706))
                            if_result17_this_order_k = sortRow706.attr0
                            if_result17_this_order_d = sortRow706.attr1
                        }
                        @sorterIterClose(iter706)
                        @sorterFree(&sorter)
                    }
                    if_result11_res = if_result17_res
                    if_result11_buy = if_result17_buy
                    if_result11_cheapest = if_result17_cheapest
                    if_result11_cheapest_order = if_result17_cheapest_order
                    if_result11_margin = if_result17_margin
                    if_result11_partkey = if_result17_partkey
                    if_result11_profit = if_result17_profit
                    if_result11_sell = if_result17_sell
                    if_result11_this_order_k = if_result17_this_order_k
                    if_result11_this_order_d = if_result17_this_order_d
                    if_result11_rec = if_result17_rec
                }

                if_result7_res = if_result11_res
                //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                //                                sample_out.col1 = @intToSql(6969)
                //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                //                sample_out.col1 = if_result11_buy
                if_result7_buy = if_result11_buy
                if_result7_cheapest = if_result11_cheapest
                if_result7_cheapest_order = if_result11_cheapest_order
                if_result7_margin = if_result11_margin
                if_result7_partkey = if_result11_partkey
                if_result7_profit = if_result11_profit
                if_result7_sell = if_result11_sell
                if_result7_this_order_k = if_result11_this_order_k
                if_result7_this_order_d = if_result11_this_order_d
                if_result7_rec = if_result11_rec

            }else{
                if_result7_res =  buy
                if_result7_buy = buy
                if_result7_cheapest = cheapest
                if_result7_cheapest_order = cheapest_order
                if_result7_margin = margin
                if_result7_partkey = partkey
                if_result7_profit = profit
                if_result7_sell = sell
                if_result7_this_order_k = this_order_k
                if_result7_this_order_d = this_order_d
                if_result7_rec = @boolToSql(false)
            }
            //if_result7_res = @intToSql(67)
            @prSetIntNull(insert_pr, 0, if_result7_res)
           //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
           //                                                sample_out.col1 = @intToSql(126969)
                                                            //if_result7_buy = @intToSql(68)
                                                            //if_result7_cheapest = @floatToSql(70.00)
                                                            //if_result7_cheapest_order = @intToSql(71)
                                                            //if_result7_margin = @floatToSql(72.00)
                                                            //if_result7_profit = @floatToSql(73.00)
                                                            //if_result7_sell = @intToSql(74)
           //                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
           //                                 sample_out.col1 = if_result7_buy
            @prSetIntNull(insert_pr, 1, if_result7_buy)
            @prSetRealNull(insert_pr, 2, if_result7_cheapest)
            @prSetIntNull(insert_pr, 3, if_result7_cheapest_order)
            @prSetRealNull(insert_pr, 4, if_result7_margin)
            @prSetIntNull(insert_pr, 5, if_result7_partkey)
            @prSetRealNull(insert_pr, 6, if_result7_profit)
            @prSetIntNull(insert_pr, 7, if_result7_sell)
            @prSetIntNull(insert_pr, 8, if_result7_this_order_k)
            @prSetDateNull(insert_pr, 9, if_result7_this_order_d)
            @prSetBool(insert_pr, 10, if_result7_rec)
            var ind_insert_temp_table_slot = @indCteScanTableInsert(&cte_scan)
        }
        }
        @vpiReset(ind_vpi)
      }
      @tableIterClose(&ind_tvi)
    }

  ret = 0
  var endtviBase: TableVectorIterator
    var endtvi1 = &endtviBase
  @tableIterInit(endtvi1, exec_ctx, TEMP_OID_MASK, temp_col_oids)
  for (@tableIterAdvance(endtvi1)) {
    var endvpi = @tableIterGetVPI(endtvi1)
    for (; @vpiHasNext(endvpi); @vpiAdvance(endvpi)) {
      var resres = @vpiGetIntNull(endvpi, 0)
      var recrec = @vpiGetBool(endvpi, 4)
      out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
              out.col1 = @intToSql(783)
        out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        out.col1 = resres
    }
    @vpiReset(endvpi)
  }
  @tableIterClose(endtvi1)
  @indCteScanFree(&cte_scan)

    }
    @vpiReset(partkey_vpi_2)
    }
    @tableIterClose(partkeys_tvi_2)

  @resultBufferFinalize(output_buffer)
  @resultBufferFree(output_buffer)
  return ret
}
