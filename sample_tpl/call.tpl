// Expected output: 70

// fun f(x: int) -> int32 { return x * 7 }

fun main() -> int32 {
    var x = 10
    var f = 25
    for(var i = 0;i < 1;i = i + 1){
    var lam = lambda (z : int) -> nil {
                    x = z
                }
    lam(i)
    }
    return x
}
