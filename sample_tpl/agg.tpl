// Expected output: 10
// SQL: SELECT col_b, count(col_a) FROM test_1 GROUP BY col_b

struct State {
    table: AggregationHashTable
    count: int32
}

struct Agg {
    key: Integer
    count: CountStarAggregate
}

fun setUpState(execCtx: *ExecutionContext, state: *State) -> nil {
    state.count = 0
    @aggHTInit(&state.table, execCtx, @execCtxGetMem(execCtx), @sizeOf(Agg))
}

fun tearDownState(state: *State) -> nil {
    @aggHTFree(&state.table)
}

fun keyCheck(agg: *Agg, vpi: *VectorProjectionIterator) -> bool {
    var key = @vpiGetInt(vpi, 1)
    return @sqlToBool(key == agg.key)
}

fun constructAgg(agg: *Agg, vpi: *VectorProjectionIterator) -> nil {
    agg.key = @vpiGetInt(vpi, 1)
    @aggInit(&agg.count)
}

fun updateAgg(agg: *Agg, vpi: *VectorProjectionIterator) -> nil {
    var input = @vpiGetInt(vpi, 0)
    @aggAdvance(&agg.count, &input)
}

fun pipeline_1(execCtx: *ExecutionContext, state: *State, lam : lambda [(int32)->nil] ) -> nil {
    var ht = &state.table
    var tvi: TableVectorIterator
    var table_oid = @testCatalogLookup(execCtx, "test_1", "")
    var col_oids: [2]uint32
    col_oids[0] = @testCatalogLookup(execCtx, "test_1", "colA")
    col_oids[1] = @testCatalogLookup(execCtx, "test_1", "colB")
    for (@tableIterInit(&tvi, execCtx, table_oid, col_oids); @tableIterAdvance(&tvi); ) {
        var vec = @tableIterGetVPI(&tvi)
        for (; @vpiHasNext(vec); @vpiAdvance(vec)) {
           lam(@vpiGetInt(vec, 0))
        }
    }
    @tableIterClose(&tvi)
}

fun execQuery(execCtx: *ExecutionContext, qs: *State, lam : lambda [(Integer)->nil] ) -> nil {
    pipeline_1(execCtx, qs, lam)
}

fun main(execCtx: *ExecutionContext) -> int32 {
    var count : Integer
    count = 0
    var lam = lambda (x : Integer) -> nil {
                        count = count + x
                    }
    var state: State

    setUpState(execCtx, &state)
    execQuery(execCtx, &state, lam)
    tearDownState(&state)

    var ret = state.count
    return count
}
