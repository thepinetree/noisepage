// Expected output: 70

// fun f(x: int) -> int32 { return x * 7 }

fun main() -> int32 {
    var x = 10
    var f = 25
    var lam = lambda (z : int) -> nil {
            x = f + z
        }
    var k = lam
    k(2)
    return x
}
