// Expected output: 70

fun f(lam : *(int32)->nil) -> nil { (*lam)(24)  }

fun main() -> int32 {
    var x = 10
    //var f = 25
    var lam = lambda (z : int ) -> nil {
                    x = z
                }
    f(lam)
    return x
}
