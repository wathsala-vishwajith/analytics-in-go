package config

import (
    "os"
    "gopkg.in/yaml.v3"
)

type Aggregation struct {
    Name      string `yaml:"name"`      // e.g. total_revenue
    Column    string `yaml:"column"`    // e.g. total_price
    Operation string `yaml:"operation"` // e.g. sum, count
}

type Config struct {
    Insight     string        `yaml:"insight"`
    InputFile   string        `yaml:"input_file"`
    OutputFile  string        `yaml:"output_file"`
    GroupBy     []string      `yaml:"group_by"`
    Aggregations []Aggregation `yaml:"aggregations"`
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
