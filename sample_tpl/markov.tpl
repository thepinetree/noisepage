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
    @filterEq(execCtx, vp, 0, &filter_val, tids)
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

  var TEMP_OID_MASK: uint32 = 2147483648                       // 2^31
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
  @indCteScanInit(&cte_scan, exec_ctx, TEMP_OID_MASK, temp_col_oids, col_types, false)

  var lineitem_oid = @testCatalogLookup(exec_ctx, "lineitem", "")
  var l_orderkey_oid = @testCatalogLookup(exec_ctx, "lineitem", "l_orderkey")
  var l_extendedprice_oid = @testCatalogLookup(exec_ctx, "lineitem", "l_extendedprice")
  var l_discount_oid = @testCatalogLookup(exec_ctx, "lineitem", "l_discount")
  var l_tax_oid = @testCatalogLookup(exec_ctx, "lineitem", "l_tax")
  var l_partkey_oid = @testCatalogLookup(exec_ctx, "lineitem", "l_partkey")

  var orders_oid = @testCatalogLookup(exec_ctx, "orders", "")
  var o_orderkey_oid = @testCatalogLookup(exec_ctx, "orders", "o_orderkey")
  var o_orderdate_oid = @testCatalogLookup(exec_ctx, "orders", "o_orderdate")


  var out_tvi: TableVectorIterator

  var insert_pr = @indCteScanGetInsertTempTablePR(&cte_scan)
  var integer_ins : Integer
  var real_ins : Real
  var date_ins : Date
  var init_partkey = @intToSql(2)

  var null_val = @initSqlNull(&integer_ins)
  @prSetIntNull(insert_pr, 0, &null_val)
  integer_ins = @initSqlNull(&integer_ins)
  @prSetIntNull(insert_pr, 1, &null_val)
  var null_real = @initSqlNull(&real_ins)
  @prSetRealNull(insert_pr, 2, &null_real)
  integer_ins = @initSqlNull(&integer_ins)
  @prSetIntNull(insert_pr, 3, &null_val)
  real_ins = @initSqlNull(&real_ins)
  @prSetRealNull(insert_pr, 4, &null_real)

  real_ins = @initSqlNull(&real_ins)
  @prSetIntNull(insert_pr, 5, &init_partkey)

  real_ins = @initSqlNull(&real_ins)
  @prSetRealNull(insert_pr, 6, &null_real)
  integer_ins = @initSqlNull(&integer_ins)
  @prSetIntNull(insert_pr, 7, &null_val)
  integer_ins = @initSqlNull(&integer_ins)
  @prSetIntNull(insert_pr, 8, &null_val)


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
  var tviBase: TableVectorIterator
      var tvi1 = &tviBase
      var col_oids1: [2]uint32
      col_oids1[0] = o_orderkey_oid
      col_oids1[1] = o_orderdate_oid
      @tableIterInit(tvi1, exec_ctx, orders_oid, col_oids1)
      var slot1: TupleSlot
      for (@tableIterAdvance(tvi1)) {
          var vpi1 = @tableIterGetVPI(tvi1)
          for (; @vpiHasNext(vpi1); @vpiAdvance(vpi1)) {
              var tviBase1: TableVectorIterator
              var tvi = &tviBase1
              var col_oids: [2]uint32
              col_oids[0] = l_orderkey_oid
              col_oids[1] = l_partkey_oid
              @tableIterInit(tvi, exec_ctx, lineitem_oid, col_oids)
              var slot: TupleSlot
              for (@tableIterAdvance(tvi)) {
                  var vpi = @tableIterGetVPI(tvi)
                  for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
                      //partkey PARAMETER
                      //vpi1,0 is orderkey from orders
                      //vpi,0 is orderkey from lineitems
                      //vpi,1 is partkey from lineitems???
                      if (@sqlToBool(@vpiGetInt(vpi, 0) == @vpiGetInt(vpi1, 0))){
                        if(@sqlToBool(@vpiGetInt(vpi, 1) == init_partkey)) {
                              var sortRow = @ptrCast(*SortRow, @sorterInsertTopK(&sorter, 1))
                              sortRow.attr0 = @vpiGetIntNull(vpi1, 0)
                              sortRow.attr1 = @vpiGetDate(vpi1, 1)
                              @sorterInsertTopKFinish(&sorter, 1)
                          }
                      }
                  }
                  @vpiReset(vpi)
              }
              @tableIterClose(tvi)
          }
          @vpiReset(vpi1)
      }
  @tableIterClose(tvi1)

  var base_iterBase: SorterIterator
  var base_iter = &base_iterBase
  @sorterIterInit(base_iter, &sorter)
  var key_val179 : Integer
  key_val179 = @initSqlNull(&key_val179)
  var date_val179 : Date
  date_val179 = @initSqlNull(&date_val179)
  for (; @sorterIterHasNext(base_iter); @sorterIterNext(base_iter)) {
      var sortRow = @ptrCast(*SortRow, @sorterIterGetRow(base_iter))
      var sample_out2 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out2.col1 = @intToSql(3201234)
        sample_out2 = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                sample_out2.col1 = sortRow.attr0
      key_val179 = sortRow.attr0
      date_val179 = sortRow.attr1
  }
  @sorterIterClose(base_iter)
  @sorterFree(&sorter)

  @prSetIntNull(insert_pr, 8, &key_val179)
  @prSetDateNull(insert_pr, 9, &date_val179)

  var bool_ins = @boolToSql(true)
  @prSetBool(insert_pr, 10, &bool_ins)
  var base_insert_temp_table_slot = @indCteScanTableInsert(&cte_scan)


  for(@indCteScanAccumulate(&cte_scan)){
    var cte = @indCteScanGetReadCte(&cte_scan)
    var ind_tvi : TableVectorIterator
    @tableIterInit(&ind_tvi, exec_ctx, TEMP_OID_MASK, temp_col_oids)
    var sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                     sample_out.col1 = @intToSql(204)
    for(@tableIterAdvance(&ind_tvi)){
    sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
    sample_out.col1 = @intToSql(207)
      var ind_vpi = @tableIterGetVPI(&ind_tvi)
      for(; @vpiHasNext(ind_vpi); @vpiAdvance(ind_vpi)){
      // FROM run AS "run"("rec?", "res", "result", "s", "x"),
        var res = @vpiGetIntNull(ind_vpi, 0)
        var buy = @vpiGetIntNull(ind_vpi, 1)
        var cheapest = @vpiGetDoubleNull(ind_vpi, 2)
        var cheapest_order = @vpiGetIntNull(ind_vpi, 3)
        var margin = @vpiGetDoubleNull(ind_vpi, 5)
        var partkey = @vpiGetIntNull(ind_vpi, 4)
        var profit = @vpiGetDoubleNull(ind_vpi, 6)
        var sell = @vpiGetIntNull(ind_vpi, 9)
        var this_order_k = @vpiGetIntNull(ind_vpi, 7)
        var this_order_d = @vpiGetDateNull(ind_vpi, 8)
        var rec = @vpiGetBool(ind_vpi, 10)

        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 0)
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 1)
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 2)
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 3)
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 4)
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 5)
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 6)
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 7)
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 8)
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 9)
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        sample_out.col1 = @vpiGetIntNull(ind_vpi, 10)

        if(@sqlToBool(rec)){
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                             sample_out.col1 = @intToSql(254)
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
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                                                     sample_out.col1 = @intToSql(999999)
        //sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        //                                     sample_out.col1 = @vpiGetIntNull(ind_vpi, 7)

        if(pred_2){
        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                                     sample_out.col1 = @intToSql(276)
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
            if_result_11_col_oids_2[0] = l_orderkey_oid
            if_result_11_col_oids_2[1] = l_extendedprice_oid
            if_result_11_col_oids_2[2] = l_partkey_oid
            if_result_11_col_oids_2[3] = l_discount_oid
            if_result_11_col_oids_2[4] = l_tax_oid
            @tableIterInit(ir11_tvi_2, exec_ctx, lineitem_oid, if_result_11_col_oids_2)
            var slot_2 : TupleSlot
            var aggs : AggPayload
            sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
            sample_out.col1 = @intToSql(268)
            for(@tableIterAdvance(ir11_tvi_2)){
                sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                            sample_out.col1 = @intToSql(271)
                var vpi_2 = @tableIterGetVPI(ir11_tvi_2)
                for (; @vpiHasNext(vpi_2); @vpiAdvance(vpi_2)) {
                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 sample_out.col1 = @vpiGetInt(vpi_2, 0)
                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 sample_out.col2 = @vpiGetDouble(vpi_2, 1)
                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 sample_out.col1 = @vpiGetInt(vpi_2, 2)
                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 sample_out.col2 = @vpiGetDouble(vpi_2, 3)
                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 sample_out.col2 = @vpiGetDouble(vpi_2, 4)
                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 sample_out.col1 = @intToSql(5555)
                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 sample_out.col1 = this_order_k
                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 sample_out.col1 = @vpiGetInt(vpi_2, 0)
                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 sample_out.col1 = @vpiGetInt(vpi_2, 2)
                 sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                 sample_out.col1 = partkey

                 if(@sqlToBool(@vpiGetInt(vpi_2, 0) == this_order_k)){
                      if (@sqlToBool(@vpiGetInt(vpi_2, 2) == partkey)){
                      var aggValues: AggValues
                      aggValues.agg_term_attr0 = @vpiGetDouble(vpi_2, 1) * (@floatToSql(1.0) - @vpiGetDouble(vpi_2, 3)) * (@floatToSql(1.0) + @vpiGetDouble(vpi_2, 4))
                      @aggAdvance(&aggs.agg_term_attr0, &aggValues.agg_term_attr0)
                  }
                }
                }
            }
            @tableIterClose(ir11_tvi_2)
            price_3 = @aggResult(&aggs.agg_term_attr0)
            sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
            sample_out.col1 = @intToSql(325)
            sample_out.col2 = price_3

            sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                        sample_out.col1 = @intToSql(5000)

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

                   var ir17_tviBase1: TableVectorIterator
                   var ir17_tvi1 = &ir17_tviBase1
                   var ir17_col_oids1: [2]uint32
                   ir17_col_oids1[0] = o_orderkey_oid
                   ir17_col_oids1[1] = o_orderdate_oid
                   @tableIterInit(ir17_tvi1, exec_ctx, orders_oid, col_oids1)
                   var slot1: TupleSlot
                   for (@tableIterAdvance(ir17_tvi1)) {
                       var vpi15 = @tableIterGetVPI(ir17_tvi1)
                       for (; @vpiHasNext(vpi15); @vpiAdvance(vpi15)) {
                           var tviBase2: TableVectorIterator
                           var tvi2 = &tviBase2
                           var col_oids: [2]uint32
                           col_oids[0] = l_partkey_oid
                           col_oids[1] = l_orderkey_oid
                           @tableIterInit(tvi2, exec_ctx, lineitem_oid, col_oids)
                           var slot2: TupleSlot
                           for (@tableIterAdvance(tvi2)) {
                               var vpi2 = @tableIterGetVPI(tvi2)
                               for (; @vpiHasNext(vpi2); @vpiAdvance(vpi2)) {
                                   //vpi1,0 is orderkey from orders
                                   //vpi,0 is orderkey from lineitems
                                   //vpi,1 is partkey from lineitems???
                                   if (@sqlToBool(@vpiGetInt(vpi2, 0) == @vpiGetInt(vpi15, 0))){
                                       if(@sqlToBool(@vpiGetInt(vpi2, 1) == partkey)) {
                                       if(@sqlToBool(@vpiGetDate(vpi15, 1) > this_order_d)){
                                           var sortRow = @ptrCast(*SortRow, @sorterInsertTopK(&sorter, 1))
                                           sortRow.attr0 = @vpiGetInt(vpi15, 0)
                                           sortRow.attr1 = @vpiGetDate(vpi15, 1)
                                           @sorterInsertTopKFinish(&sorter, 1)
                                       }
                                       }
                                   }
                               }
                               @vpiReset(vpi2)
                           }
                           @tableIterClose(tvi2)
                       }
                        @vpiReset(vpi15)
                       }
                   @tableIterClose(ir17_tvi1)

                   var iterBase17: SorterIterator
                   var iter17 = &iterBase17
                   @sorterIterInit(iter17, &sorter)
                   if_result17_this_order_k = @initSqlNull(&if_result17_this_order_k)
                   if_result17_this_order_d = @initSqlNull(&if_result17_this_order_d)
                   for (; @sorterIterHasNext(iter17); @sorterIterNext(iter17)) {
                       var sortRow = @ptrCast(*SortRow, @sorterIterGetRow(iter17))
                       if_result17_this_order_k = sortRow.attr0
                       if_result17_this_order_d = sortRow.attr1
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
                     var tviBase: TableVectorIterator
                     var tvi1 = &tviBase
                     var col_oids1: [2]uint32
                     col_oids1[0] = o_orderkey_oid
                     col_oids1[1] = o_orderdate_oid
                     @tableIterInit(tvi1, exec_ctx, orders_oid, col_oids1)
                     var slot1: TupleSlot
                     for (@tableIterAdvance(tvi1)) {
                         var vpi1 = @tableIterGetVPI(tvi1)
                         for (; @vpiHasNext(vpi1); @vpiAdvance(vpi1)) {
                             var tviBase1: TableVectorIterator
                             var tvi = &tviBase1
                             var col_oids: [2]uint32
                             col_oids[0] = l_partkey_oid
                             col_oids[1] = l_orderkey_oid
                             @tableIterInit(tvi, exec_ctx, lineitem_oid, col_oids)
                             var slot: TupleSlot
                             for (@tableIterAdvance(tvi)) {
                                 var vpi = @tableIterGetVPI(tvi)
                                 for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
                                     //vpi1,0 is orderkey from orders
                                                           //vpi,0 is orderkey from lineitems
                                                           //vpi,1 is partkey from lineitems???
                                     if (@sqlToBool(@vpiGetInt(vpi, 0) == @vpiGetInt(vpi1, 0))){
                                          if(@sqlToBool(@vpiGetInt(vpi, 1) == partkey)) {
                                         if(@sqlToBool(@vpiGetDate(vpi1, 1) > this_order_d)){
                                             var sortRow = @ptrCast(*SortRow, @sorterInsertTopK(&sorter, 1))
                                             sortRow.attr0 = @vpiGetInt(vpi1, 0)
                                             sortRow.attr1 = @vpiGetDate(vpi1, 1)
                                             @sorterInsertTopKFinish(&sorter, 1)
                                         }
                                     }
                                     }
                                 }
                             }
                             @tableIterClose(tvi)
                         }
                     }

                     var iterBase2: SorterIterator
                     var iter2 = &iterBase2
                     if_result17_this_order_k = @initSqlNull(&if_result17_this_order_k)
                     if_result17_this_order_d = @initSqlNull(&if_result17_this_order_d)
                     @sorterIterInit(iter2, &sorter)
                     for (; @sorterIterHasNext(iter2); @sorterIterNext(iter2)) {
                         var sortRow = @ptrCast(*SortRow, @sorterIterGetRow(iter2))
                         if_result17_this_order_k = sortRow.attr0
                         if_result17_this_order_d = sortRow.attr1
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
                sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                                             sample_out.col1 = @intToSql(509)
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
                    sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                                            sample_out.col2 = profit_4

                    if(@sqlToBool(q9_4)){
                        sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                        sample_out.col1 = @intToSql(528)
                        if_result17_res = @initSqlNull(&if_result17_res)
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
                          var ir17_tviBase: TableVectorIterator
                          var ir17_tvi = &tviBase
                          var ir17_col_oids: [2]uint32
                          ir17_col_oids[0] = o_orderkey_oid
                          ir17_col_oids[1] = o_orderdate_oid
                          @tableIterInit(ir17_tvi, exec_ctx, orders_oid, ir17_col_oids)
                          var slot: TupleSlot

                          for (@tableIterAdvance(ir17_tvi)) {
                              var vpi1 = @tableIterGetVPI(ir17_tvi)
                              for (; @vpiHasNext(vpi1); @vpiAdvance(vpi1)) {
                                  var tviBase1: TableVectorIterator
                                  var tvi = &tviBase1
                                  var col_oids: [2]uint32
                                  col_oids[0] = l_partkey_oid
                                  col_oids[1] = o_orderdate_oid
                                  @tableIterInit(tvi1, exec_ctx, lineitem_oid, col_oids)
                                  var slot: TupleSlot
                                  for (@tableIterAdvance(tvi)) {
                                      var vpi = @tableIterGetVPI(tvi)
                                      for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
                                          //vpi1,0 is orderkey from orders
                                                                //vpi,0 is orderkey from lineitems
                                                                //vpi,1 is partkey from lineitems???
                                          sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                                          sample_out.col1 = @vpiGetInt(vpi, 0)
                                          sample_out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
                                          sample_out.col1 = @vpiGetInt(vpi1, 1)
                                          if (@sqlToBool(@vpiGetInt(vpi, 0) == @vpiGetInt(vpi1, 0))){
                                              if(@sqlToBool(@vpiGetInt(vpi, 1) == partkey)) {
                                              if(@sqlToBool(@vpiGetDate(vpi1, 1) > this_order_d)){
                                                  var sortRow = @ptrCast(*SortRow, @sorterInsertTopK(&sorter, 1))
                                                  sortRow.attr0 = @vpiGetInt(vpi1, 0)
                                                  sortRow.attr1 = @vpiGetDate(vpi1, 1)
                                                  @sorterInsertTopKFinish(&sorter, 1)
                                              }
                                              }
                                          }
                                      }
                                  }
                                  @tableIterClose(tvi1)
                              }
                          }
                          @tableIterClose(ir17_tvi)

                          var iterBase: SorterIterator
                          var iter = &iterBase
                          @sorterIterInit(iter, &sorter)
                          if_result17_this_order_k = @initSqlNull(&if_result17_this_order_k)
                          if_result17_this_order_d = @initSqlNull(&if_result17_this_order_d)
                          for (; @sorterIterHasNext(iter); @sorterIterNext(iter)) {
                              var sortRow = @ptrCast(*SortRow, @sorterIterGetRow(iter))
                              if_result17_this_order_k = sortRow.attr0
                              if_result17_this_order_d = sortRow.attr1
                          }
                          @sorterIterClose(iter)
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
                        var  if_result17_tviBase: TableVectorIterator
                        var  if_result17_tvi1 = &tviBase
                        var  if_result17_col_oids1: [2]uint32
                        if_result17_col_oids1[0] = o_orderkey_oid
                        if_result17_col_oids1[1] = o_orderdate_oid
                        @tableIterInit(if_result17_tvi1, exec_ctx, orders_oid, if_result17_col_oids1)
                        var ir17_slot1: TupleSlot
                        for (@tableIterAdvance(if_result17_tvi1)) {
                            var vpi1 = @tableIterGetVPI(if_result17_tvi1)
                            for (; @vpiHasNext(vpi1); @vpiAdvance(vpi1)) {
                                var tviBase1: TableVectorIterator
                                var tvi = &tviBase1
                                var col_oids: [2]uint32
                                col_oids[0] = l_orderkey_oid
                                col_oids[1] = l_partkey_oid
                                @tableIterInit(tvi, exec_ctx, lineitem_oid, col_oids)
                                var slot: TupleSlot
                                for (@tableIterAdvance(tvi)) {
                                    var vpi = @tableIterGetVPI(tvi)
                                    for (; @vpiHasNext(vpi); @vpiAdvance(vpi)) {
                                        if (@sqlToBool(@vpiGetInt(vpi, 0) == @vpiGetInt(vpi1, 0))){
                                            if(@sqlToBool(@vpiGetInt(vpi, 1) == partkey)) {
                                            if(@sqlToBool(@vpiGetDate(vpi1, 1) > this_order_d)){
                                                var sortRow = @ptrCast(*SortRow, @sorterInsertTopK(&sorter, 1))
                                                sortRow.attr0 = @vpiGetInt(vpi1, 0)
                                                sortRow.attr1 = @vpiGetDate(vpi1, 1)
                                                @sorterInsertTopKFinish(&sorter, 1)
                                            }
                                        }
                                        }
                                    }
                                }
                                @tableIterClose(tvi)
                            }
                        }
                        @tableIterClose(if_result17_tvi1)

                        var iterBase: SorterIterator
                        var iter = &iterBase
                        @sorterIterInit(iter, &sorter)
                        if_result17_this_order_k = @initSqlNull(&if_result17_this_order_k)
                        if_result17_this_order_d = @initSqlNull(&if_result17_this_order_d)
                        for (; @sorterIterHasNext(iter); @sorterIterNext(iter)) {
                            var sortRow = @ptrCast(*SortRow, @sorterIterGetRow(iter))
                            if_result17_this_order_k = sortRow.attr0
                            if_result17_this_order_d = sortRow.attr1
                        }
                        @sorterIterClose(iter)
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
            @prSetIntNull(insert_pr, 0, &if_result7_res)
            @prSetIntNull(insert_pr, 1, &if_result7_buy)
            @prSetDouble(insert_pr, 2, &if_result7_cheapest)
            @prSetIntNull(insert_pr, 3, &if_result7_cheapest_order)
            @prSetDouble(insert_pr, 4, &if_result7_margin)
            @prSetIntNull(insert_pr, 5, &if_result7_partkey)
            @prSetDoubleNull(insert_pr, 6, &if_result7_profit)
            @prSetIntNull(insert_pr, 7, &if_result7_sell)
            @prSetIntNull(insert_pr, 8, &if_result7_this_order_k)
            @prSetDateNull(insert_pr, 9, &if_result7_this_order_d)
            @prSetBool(insert_pr, 10, &if_result7_rec)
            var ind_insert_temp_table_slot = @indCteScanTableInsert(&cte_scan)
        }
        }
        @vpiReset(ind_vpi)
      }
      @tableIterClose(&ind_tvi)
    }

  var ret = 0
  var endtviBase: TableVectorIterator
    var endtvi1 = &endtviBase
  @tableIterInit(endtvi1, exec_ctx, TEMP_OID_MASK, temp_col_oids)
  for (@tableIterAdvance(endtvi1)) {
    var endvpi = @tableIterGetVPI(endtvi1)
    for (; @vpiHasNext(endvpi); @vpiAdvance(endvpi)) {
      var resres = @vpiGetInt(endvpi, 1)
      var recrec = @vpiGetBool(endvpi, 4)
        out = @ptrCast(*output_struct, @resultBufferAllocRow(output_buffer))
        out.col1 = resres
    }
    @vpiReset(endvpi)
  }
  @tableIterClose(endtvi1)
  @indCteScanFree(&cte_scan)


  @resultBufferFinalize(output_buffer)
  @resultBufferFree(output_buffer)
  return ret
}
