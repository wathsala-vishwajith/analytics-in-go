package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Column struct {
	Name      string `yaml:"name"`
	Type      string `yaml:"type"`
	Aggregate string `yaml:"aggregate,omitempty"`
}

type SortBy struct {
	Column string `yaml:"column"`
	Order  string `yaml:"order"`
}

type Config struct {
	TableName     string   `yaml:"table_name"`
	InputCSV      string   `yaml:"input_csv"`
	OutputParquet string   `yaml:"output_parquet"`
	OutputArrow   string   `yaml:"output_arrow"`
	Columns       []Column `yaml:"columns"`
	SortBy        SortBy   `yaml:"sort_by"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
