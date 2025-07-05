package main

import (
	"html/template"
	"io"
	"net/http"

	"analytics-in-go/src/config"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// Template renderer for Echo
type TemplateRenderer struct {
	templates *template.Template
}

func (t *TemplateRenderer) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}

func convertTableToRecords(tbl arrow.Table) []map[string]interface{} {
	records := make([]map[string]interface{}, 0)
	for i := int64(0); i < tbl.NumRows(); i++ {
		row := make(map[string]interface{})
		for j, col := range tbl.Schema().Fields() {
			colData := tbl.Column(j)
			chunked := colData.Data()
			if chunked.Len() > 0 {
				chunk := chunked.Chunk(0)
				if int(i) < chunk.Len() {
					switch arr := chunk.(type) {
					case *array.String:
						row[col.Name] = arr.Value(int(i))
					case *array.Int64:
						row[col.Name] = arr.Value(int(i))
					case *array.Float64:
						row[col.Name] = arr.Value(int(i))
					default:
						row[col.Name] = chunk.ValueStr(int(i))
					}
				}
			}
		}
		records = append(records, row)
	}
	return records
}

func startSSRServer(configs []*config.Config) {
	// Initialize Echo
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Initialize templates
	renderer := &TemplateRenderer{
		templates: template.Must(template.ParseGlob("templates/*.html")),
	}
	e.Renderer = renderer

	// Serve static files
	e.Static("/static", "static")

	// Home page
	e.GET("/", func(c echo.Context) error {
		return c.Render(http.StatusOK, "dashboard.html", map[string]interface{}{
			"Configs": configs,
		})
	})

	// Dashboard page (same as home for now)
	e.GET("/dashboard", func(c echo.Context) error {
		return c.Render(http.StatusOK, "dashboard.html", map[string]interface{}{
			"Configs": configs,
		})
	})

	// Dashboard endpoints for each config
	for _, cfg := range configs {
		currentCfg := cfg

		// Dashboard endpoint - using the URL endpoint path
		e.GET("/dashboard"+currentCfg.URLEndpoint, func(c echo.Context) error {
			// Extract the endpoint from the URL to match the existing API
			endpoint := currentCfg.URLEndpoint

			tbl, err := getTable(endpoint, currentCfg)
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, "Data not loaded: "+err.Error())
			}

			records := convertTableToRecords(tbl)
			return c.Render(http.StatusOK, "data-table.html", map[string]interface{}{
				"Name":   currentCfg.Name,
				"Config": currentCfg,
				"Data":   records,
			})
		})
	}

	// Start SSR server
	verboseLog("Starting SSR server on port 8081")
	e.Logger.Fatal(e.Start(":8081"))
}
