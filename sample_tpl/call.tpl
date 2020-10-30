// Expected output: 70

fun f(lam : lambda[(int32)->nil], z : int ) -> nil { (lam)(z)  }

fun main() -> int32 {
    var y = 20
    var x = 10
    //var f = 25
    var lam = lambda (z : int ) -> nil {
                    y = z + x
                }
    f(lam, 24)
    return y
}
