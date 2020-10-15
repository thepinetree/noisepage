// Expected output: 70

// fun f(x: int) -> int32 { return x * 7 }

fun main() -> int32 {
    var x = 10
    var lam = lambda (z : int) -> nil { x = x + z }
    lam(2)
    return x
}
