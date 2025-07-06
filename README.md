# Analytics Dashboard in Go

A high-performance analytics dashboard built with Go that processes CSV data and provides both REST API endpoints and server-side rendered (SSR) web interfaces with dynamic visualizations.

## Features

- **Configuration-driven data processing** - Define analytics queries via YAML config files
- **Dual server architecture** - REST API (port 8080) + SSR web interface (port 8081)
- **Real-time data visualization** - Charts, tables, and graphs with Chart.js
- **Server-side pagination** - Efficient data handling with pagination and sorting
- **Parquet file optimization** - Fast data storage and retrieval with Apache Arrow
- **Hot config reloading** - Dynamic configuration updates without server restart
- **Comprehensive test suite** - Full test coverage with `--test` flag

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Config YAML   │    │   CSV Data      │    │  Parquet Files  │
│   Files         │───▶│   Processing    │───▶│   (Arrow)       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Dashboard │◀───│   SSR Server    │◀───│   REST API      │
│   (Port 8081)   │    │   (Templates)   │    │   (Port 8080)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Performance Features

- **In-memory caching** with TTL for Arrow tables
- **Lazy loading** of Parquet files
- **Server-side pagination** for large datasets
- **Efficient data structures** with Apache Arrow
- **Hot configuration reloading** without downtime

## Dependencies

- **Apache Arrow** - High-performance columnar data
- **Echo Framework** - Web framework for SSR
- **Chart.js** - Client-side data visualization
- **Tailwind CSS** - Utility-first CSS framework
- **fsnotify** - File system event monitoring

## Quick Start

### 1. Install Dependencies
```bash
go mod download
```

### 2. Move GO_test_5m.csv to src/data/raw/
```bash
mv GO_test_5m.csv ${project_folder}/src/data/raw
```

### 2. Run Tests
```bash
# Run comprehensive test suite
./test.sh

# Or run tests directly
cd src && go run main.go ssr_server.go --test
```

### 3. Start the Servers
```bash
# Start both API and SSR servers
cd src && go run main.go ssr_server.go --serve
```

### 4. Access the Dashboard
- **Web Interface**: http://localhost:8081
- **REST API**: http://localhost:8080
- **Dynamic Views**: http://localhost:8081/dashboard/{config-name}

## Configuration

Create YAML files in `src/config/` directory:

```yaml
# Example: country_level_revenue.yaml
table_name: "country_level_revenue"
input_csv: "data/raw/sales_data.csv"
output_parquet: "data/processed/country_level_revenue.parquet"
url_endpoint: "/api/country-level-revenue"
api_endpoint: "/api/country-level-revenue"
view:
  type: "table"  # Options: table, bar_chart, pie_chart, line_chart
  title: "Country Level Revenue Analysis"
  x_axis: "Country"
  y_axis: "Revenue"
  description: "Revenue breakdown by country"
columns:
  - name: "Country"
    type: "string"
  - name: "Revenue"
    type: "float64"
transformations:
  - type: "group_by"
    column: "Country"
    aggregation: "sum"
    target_column: "Revenue"
```

## Testing

The application includes a comprehensive test suite covering:

### Test Categories

1. **Config Loading & Validation**
   - YAML file parsing
   - Configuration validation
   - Error handling

2. **Parquet File Generation**
   - CSV to Parquet conversion
   - Data integrity checks
   - File system operations

3. **API Endpoints**
   - REST API functionality
   - JSON response validation
   - Error handling

4. **SSR Server**
   - Template rendering
   - Dynamic view generation
   - HTTP response validation

5. **Pagination Logic**
   - Server-side pagination
   - Data slicing and limits
   - Metadata calculation

6. **Data Processing**
   - Arrow table operations
   - Schema validation
   - Data transformation

### Running Tests

```bash
# Run all tests
./test.sh

# Run with verbose logging
cd src && go run main.go ssr_server.go --test --verbose

# Run specific test by modifying the test runner
cd src && go run main.go ssr_server.go --test
```

### Test Output
```
🧪 Analytics Dashboard Test Suite
=================================

Starting comprehensive test suite...

✅ PASSED: Config Loading
✅ PASSED: Parquet File Generation  
✅ PASSED: API Endpoints
✅ PASSED: SSR Server
✅ PASSED: Pagination
✅ PASSED: Data Processing

=== TEST SUMMARY ===
Total: 6, Passed: 6, Failed: 0
🎉 All tests passed!
```

## API Endpoints

### Pagination Support
All endpoints support pagination parameters:

```bash
# Basic pagination
curl "http://localhost:8080/api/country-level-revenue?page=1&limit=10"

# With sorting
curl "http://localhost:8080/api/country-level-revenue?page=1&limit=10&sort=Revenue&order=desc"
```

### Response Format
```json
{
  "data": [...],
  "total": 150,
  "page": 1,
  "limit": 10,
  "total_pages": 15,
  "has_next": true,
  "has_prev": false
}
```

## Development

### Project Structure
```
analytics-in-go/
├── src/
│   ├── config/           # YAML configuration files
│   ├── data/
│   │   ├── raw/          # Input CSV files
│   │   └── processed/    # Generated Parquet files
│   ├── templates/        # HTML templates for SSR
│   ├── static/           # CSS, JS, and Chart.js files
│   ├── main.go           # Main application with API server
│   ├── ssr_server.go     # SSR server implementation
│   └── ...
├── test.sh              # Test runner script
└── README.md
```

### Adding New Analytics

1. Create a new YAML config file in `src/config/`
2. Define your data transformation and view type
3. Restart the server or wait for hot reload
4. Access via `/dashboard/{config-name}`

### Command Line Options

```bash
# Run in server mode
go run main.go ssr_server.go --serve

# Run tests
go run main.go ssr_server.go --test

# Enable verbose logging
go run main.go ssr_server.go --serve --verbose
```

