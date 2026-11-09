package health

import "math"

// z95 is the normal quantile of a 95 percent bound
const z95 = 1.96

// LowerBound is the Wilson lower bound at 95 percent of ok successes in n, 1 with no samples
func LowerBound(ok, n int) float64 {
	if n <= 0 {
		return 1
	}
	nf := float64(n)
	p := float64(ok) / nf
	z2 := z95 * z95
	spread := z95 * math.Sqrt(p*(1-p)/nf+z2/(4*nf*nf))
	return (p + z2/(2*nf) - spread) / (1 + z2/nf)
}
