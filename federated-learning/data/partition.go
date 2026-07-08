package data

import (
	"math/rand/v2"
	"sort"
)

type PartitionMode int

const (
	IID = iota
	NonIID
)

func Partition(samples []Sample, n int, mode PartitionMode) [][]Sample {
	temp := make([]Sample, len(samples))
	copy(temp, samples)
	if mode == IID {
		rand.Shuffle(len(temp), func(i, j int) { temp[i], temp[j] = temp[j], temp[i] })
	} else {
		sort.Slice(temp, func(i, j int) bool {
			return temp[i].Features[1] < temp[j].Features[1]
		})
	}

	parts := make([][]Sample, n)
	size := len(temp) / n
	for i := range n {
		start := i * size
		end := start + size
		if i == n-1 {
			end = len(temp)
		}
		parts[i] = temp[start:end]
	}
	return parts
}

func Normalize(samples []Sample) (mins, maxs []float64, minTarget, maxTarget float64) {
	if len(samples) == 0 {
		return
	}
	numFeatures := len(samples[0].Features)
	mins = make([]float64, numFeatures)
	maxs = make([]float64, numFeatures)

	for j := range numFeatures {
		mins[j] = samples[0].Features[j]
		maxs[j] = samples[0].Features[j]
	}

	for _, s := range samples[1:] {
		for j, f := range s.Features {
			if f < mins[j] {
				mins[j] = f
			}
			if f > maxs[j] {
				maxs[j] = f
			}
		}
	}

	for i := range samples {
		for j := range samples[i].Features {
			r := maxs[j] - mins[j]
			if r > 0 {
				samples[i].Features[j] = (samples[i].Features[j] - mins[j]) / r
			}
		}
	}

	minTarget = samples[0].Target
	maxTarget = samples[0].Target
	for _, s := range samples[1:] {
		if s.Target < minTarget {
			minTarget = s.Target
		}
		if s.Target > maxTarget {
			maxTarget = s.Target
		}
	}

	// Normalizuj target in-place
	r := maxTarget - minTarget
	for i := range samples {
		if r > 0 {
			samples[i].Target = (samples[i].Target - minTarget) / r
		}
	}
	return
}

func ToMatrices(samples []Sample) (X [][]float64, y []float64) {
	if len(samples) == 0 {
		return
	}
	n := len(samples)
	X = make([][]float64, n)
	y = make([]float64, n)
	for i := range n {
		X[i] = samples[i].Features
		y[i] = samples[i].Target
	}
	return
}
