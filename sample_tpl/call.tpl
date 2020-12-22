// Expected output: 70

fun f(z : Date ) -> Date { return z  }

fun main(exec : *ExecutionContext) -> int32 {
    var y = @dateToSql(1998, 2, 11)
    //var f = 25
    var lam = lambda [y] (z : Date ) -> nil {
                    y = z
                }
    var d = @dateToSql(1999, 2, 11)
    //f(lam, d)
    var k : Date
    //var h = &k
    //*h = d
    //k = f(d)
    lam(d)
    if(@datePart(y, @intToSql(21)) == @intToSql(1999)){
        // good
        return 1
    }
    return 0
}
