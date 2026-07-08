package model

type LinearModel struct {
	Weights     []float64
	Bias        float64
	NumFeatures int
}

func New(numFeatures int) *LinearModel {
	return &LinearModel{
		Weights:     make([]float64, numFeatures),
		Bias:        0.0,
		NumFeatures: numFeatures,
	}
}

func (l *LinearModel) Predict(x []float64) float64 {
	var y float64
	for i, w := range l.Weights {
		y += w * x[i]
	}
	return y + l.Bias
}

func (l *LinearModel) MSE(X [][]float64, y []float64) float64 {
	var sum float64
	for i, x := range X {
		d := l.Predict(x) - y[i]
		sum += d * d
	}
	return sum / float64(len(X))
}

func (l *LinearModel) Train(X [][]float64, y []float64, epochs int, lr float64) float64 {
	n := float64(len(X))
	for range epochs {
		gradW := make([]float64, l.NumFeatures)
		var gradB float64

		for i, x := range X {
			err := l.Predict(x) - y[i]
			for j, f := range x {
				gradW[j] += (2 / n) * err * f
			}
			gradB += (2 / n) * err
		}
		for j := range l.Weights {
			l.Weights[j] -= lr * gradW[j]
		}
		l.Bias -= lr * gradB
	}
	return l.MSE(X, y)
}

func (l *LinearModel) Clone() *LinearModel {
	copyWeights := make([]float64, len(l.Weights))
	copy(copyWeights, l.Weights)
	return &LinearModel{
		Weights:     copyWeights,
		Bias:        l.Bias,
		NumFeatures: l.NumFeatures,
	}
}
