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

type ViewConfig struct {
	Type        string `yaml:"type"`        // "table", "bar_chart", "pie_chart", "line_chart"
	Title       string `yaml:"title"`       // Chart title
	XAxis       string `yaml:"x_axis"`      // Column for X-axis (labels)
	YAxis       string `yaml:"y_axis"`      // Column for Y-axis (values)
	Description string `yaml:"description"` // Description for the view
}

type Config struct {
	TableName     string     `yaml:"table_name"`
	Name          string     `yaml:"name"`
	InputCSV      string     `yaml:"input_csv"`
	OutputParquet string     `yaml:"output_parquet"`
	OutputArrow   string     `yaml:"output_arrow"`
	Columns       []Column   `yaml:"columns"`
	SortBy        SortBy     `yaml:"sort_by"`
	Limit         int        `yaml:"limit,omitempty"`
	URLEndpoint   string     `yaml:"url_endpoint"`
	APIEndpoint   string     `yaml:"api_endpoint"`
	View          ViewConfig `yaml:"view"`
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
