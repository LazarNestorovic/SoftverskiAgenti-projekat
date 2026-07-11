package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	FL      FLConfig      `yaml:"federated_learning"`
	Dataset DatasetConfig `yaml:"dataset"`
}

type FLConfig struct {
	NumClients           int     `yaml:"num_clients"`
	NumRounds            int     `yaml:"num_rounds"`
	LearningRate         float64 `yaml:"learning_rate"`
	Epochs               int     `yaml:"epochs"`
	ConvergenceThreshold float64 `yaml:"convergence_threshold"`
	PartitionMode        string  `yaml:"partition_mode"`
	NumClusters          int     `yaml:"num_clusters"`
}

type DatasetConfig struct {
	Path            string  `yaml:"path"`
	ValidationSplit float64 `yaml:"validation_split"`
}

func loadConfig(path string) (*Config, error) {
	f, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	return &cfg, yaml.Unmarshal(f, &cfg)
}
