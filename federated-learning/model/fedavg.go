package model

type ClientUpdate struct {
	ClientID   string
	Weights    []float64
	Bias       float64
	NumSamples int
	Loss       float64
}

func FedAvg(updates []ClientUpdate) *LinearModel {
	if len(updates) == 0 {
		return nil
	}

	N := 0
	for _, c := range updates {
		N += c.NumSamples
	}
	globalW := make([]float64, len(updates[0].Weights))
	globalBias := float64(0)
	for _, c := range updates {
		for i, w := range c.Weights {
			globalW[i] += (float64(c.NumSamples) / float64(N)) * w
		}
		globalBias += (float64(c.NumSamples) / float64(N)) * c.Bias
	}

	return &LinearModel{
		Weights:     globalW,
		Bias:        globalBias,
		NumFeatures: len(globalW),
	}
}
