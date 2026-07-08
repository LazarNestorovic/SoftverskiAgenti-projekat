package evaluation

import (
	"math"
)

type RoundMetric struct {
	RoundID int
	RMSE    float64
	MAE     float64
	R2      float64
	Loss    float64
}

func MSE(y, target []float64) float64 {
	n := len(y)
	squaredDiff := float64(0)
	for i := range n {
		squaredDiff += (y[i] - target[i]) * (y[i] - target[i])
	}
	return squaredDiff / float64(n)
}

func RMSE(y, target []float64) float64 {
	return math.Sqrt(MSE(y, target))
}

func MAE(y, target []float64) float64 {
	n := len(y)
	absDiff := float64(0)
	for i := range n {
		absDiff += math.Abs((y[i] - target[i]))
	}
	return absDiff / float64(n)
}

func R2(pred, target []float64) float64 {
	var mean float64
	for _, t := range target {
		mean += t
	}
	mean /= float64(len(target))

	var ssTot, ssRes float64
	for i, t := range target {
		ssTot += (t - mean) * (t - mean)
		d := pred[i] - t
		ssRes += d * d
	}
	if ssTot == 0 {
		return 1
	}
	return 1 - ssRes/ssTot
}
