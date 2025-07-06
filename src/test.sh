#!/bin/bash

# Test script for analytics-in-go
echo "🧪 Analytics Dashboard Test Suite"
echo "================================="
echo ""

# Change to src directory
cd src

# Run tests
echo "Starting comprehensive test suite..."
echo ""
go run main.go ssr_server.go --test

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 All tests passed successfully!"
    echo ""
    echo "Test Coverage:"
    echo "- ✅ Config Loading & Validation"
    echo "- ✅ Parquet File Generation & Processing"
    echo "- ✅ API Endpoints & Data Serving"
    echo "- ✅ SSR Server & Template Rendering"
    echo "- ✅ Pagination Logic & Data Slicing"
    echo "- ✅ Data Processing & Schema Validation"
    echo ""
else
    echo ""
    echo "❌ Some tests failed. Check the output above for details."
    exit 1
fi 