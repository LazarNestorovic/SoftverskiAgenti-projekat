package data

import (
	"encoding/csv"
	"math"
	"os"
	"strconv"
)

type Sample struct {
	Features []float64
	Target   float64
}

const NumFeatures = 8

func Load(path string) ([]Sample, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	samples := []Sample{}
	for i, row := range records {
		if i == 0 {
			continue
		}
		features := make([]float64, NumFeatures)
		valid := true
		for i := range NumFeatures {
			v, err := strconv.ParseFloat(row[i], 64)
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
				valid = false
				break
			}
			features[i] = v
		}
		if !valid {
			continue
		}
		target, err := strconv.ParseFloat(row[NumFeatures], 64)
		if err != nil {
			continue
		}
		samples = append(samples, Sample{
			Features: features,
			Target:   target,
		})
	}
	return samples, nil
}
