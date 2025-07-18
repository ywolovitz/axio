# Complete cURL Commands Guide

## 🔧 Setup & Health Checks

### Check Server Health
```bash
curl -X GET http://localhost:3000/health
```

### List All Available Endpoints
```bash
curl -X GET http://localhost:3000/
```

### Test API Connectivity (Before Importing)
```bash
curl -X GET http://localhost:3000/test-api
```

## 📊 Table Management

### View All Table Information
```bash
curl -X GET http://localhost:3000/table-info
```

### Reset Individual Tables (Optional - if you need fresh tables)

#### Reset Buildings Table
```bash
curl -X GET http://localhost:3000/reset-buildings-table
```

#### Reset Cases Table
```bash
curl -X GET http://localhost:3000/reset-cases-table
```

#### Reset Conversations Table
```bash
curl -X GET http://localhost:3000/reset-conversations-table
```

#### Reset Interactions Table
```bash
curl -X GET http://localhost:3000/reset-interactions-table
```

#### Reset User State Interactions Table
```bash
curl -X GET http://localhost:3000/reset-user-state-interactions-table
```

#### Reset Users Table
```bash
curl -X GET http://localhost:3000/reset-users-table
```

#### Reset User Session History Table
```bash
curl -X GET http://localhost:3000/reset-user-session-history-table
```

#### Reset Schedule Table
```bash
curl -X GET http://localhost:3000/reset-schedule-table
```

#### Reset SLA Policy Table
```bash
curl -X GET http://localhost:3000/reset-sla-policy-table
```

#### Reset NOC Interactions Table
```bash
curl -X GET http://localhost:3000/reset-noc-interactions-table
```

## 📥 Individual Data Import Commands

### Import Buildings Data
```bash
curl -X GET http://localhost:3000/import-buildings
```

### Import Cases Data
```bash
curl -X GET http://localhost:3000/import-cases
```

### Import Conversations Data
```bash
curl -X GET http://localhost:3000/import-conversations
```

### Import Interactions Data
```bash
curl -X GET http://localhost:3000/import-interactions
```

### Import User State Interactions Data
```bash
curl -X GET http://localhost:3000/import-user-state-interactions
```

### Import Users Data
```bash
curl -X GET http://localhost:3000/import-users
```

### Import User Session History Data
```bash
curl -X GET http://localhost:3000/import-user-session-history
```

### Import Schedule Data
```bash
curl -X GET http://localhost:3000/import-schedule
```

### Import SLA Policy Data
```bash
curl -X GET http://localhost:3000/import-sla-policy
```

### Import NOC Interactions Data ✨
```bash
curl -X GET http://localhost:3000/import-noc-interactions
```

## 🚀 Bulk Import Command

### Import All Data Types at Once
```bash
curl -X GET http://localhost:3000/import-all
```

## 📋 Complete Import Workflow

### Option 1: Quick Start (Import Everything)
```bash
# 1. Check server health
curl -X GET http://localhost:3000/health

# 2. Test API connectivity
curl -X GET http://localhost:3000/test-api

# 3. Import all data at once
curl -X GET http://localhost:3000/import-all

# 4. Check results
curl -X GET http://localhost:3000/table-info
```

### Option 2: Step-by-Step Import
```bash
# 1. Health check
curl -X GET http://localhost:3000/health

# 2. Test APIs
curl -X GET http://localhost:3000/test-api

# 3. Import each data type individually
curl -X GET http://localhost:3000/import-buildings
curl -X GET http://localhost:3000/import-cases
curl -X GET http://localhost:3000/import-conversations
curl -X GET http://localhost:3000/import-interactions
curl -X GET http://localhost:3000/import-user-state-interactions
curl -X GET http://localhost:3000/import-users
curl -X GET http://localhost:3000/import-user-session-history
curl -X GET http://localhost:3000/import-schedule
curl -X GET http://localhost:3000/import-sla-policy
curl -X GET http://localhost:3000/import-noc-interactions

# 4. Verify results
curl -X GET http://localhost:3000/table-info
```

### Option 3: Import Only NOC Interactions
```bash
# 1. Health check
curl -X GET http://localhost:3000/health

# 2. Test NOC Interactions API
curl -X GET http://localhost:3000/test-api

# 3. Reset NOC Interactions table (optional)
curl -X GET http://localhost:3000/reset-noc-interactions-table

# 4. Import NOC Interactions data
curl -X GET http://localhost:3000/import-noc-interactions

# 5. Check results
curl -X GET http://localhost:3000/table-info
```

## 🔍 Advanced cURL Options

### With Verbose Output
```bash
curl -v -X GET http://localhost:3000/import-noc-interactions
```

### With Timeout (30 seconds)
```bash
curl --max-time 30 -X GET http://localhost:3000/import-noc-interactions
```

### Save Response to File
```bash
curl -X GET http://localhost:3000/import-noc-interactions -o import_response.json
```

### Pretty Print JSON Response
```bash
curl -X GET http://localhost:3000/import-noc-interactions | jq '.'
```

### Follow Redirects
```bash
curl -L -X GET http://localhost:3000/import-noc-interactions
```

## 📝 Bash Script for Complete Import

### Create import_all.sh
```bash
#!/bin/bash

BASE_URL="http://localhost:3000"

echo "🚀 Starting data import process..."

# Health check
echo "📊 Checking server health..."
curl -s "$BASE_URL/health" | jq '.'

# Test APIs
echo "🔍 Testing API connectivity..."
curl -s "$BASE_URL/test-api" | jq '.'

# Import all data
echo "📥 Starting bulk import..."
curl -s "$BASE_URL/import-all" | jq '.'

# Check final results
echo "📋 Final table information..."
curl -s "$BASE_URL/table-info" | jq '.'

echo "✅ Import process completed!"
```

### Make it executable and run
```bash
chmod +x import_all.sh
./import_all.sh
```

## 🔧 Troubleshooting Commands

### If Import Fails - Check Logs
```bash
# Check if server is running
curl -X GET http://localhost:3000/health

# Test specific API endpoint
curl -v -X GET http://localhost:3000/test-api

# Reset problematic table and retry
curl -X GET http://localhost:3000/reset-noc-interactions-table
curl -X GET http://localhost:3000/import-noc-interactions
```

### Check Response Codes
```bash
# Get HTTP status code only
curl -o /dev/null -s -w "%{http_code}\n" http://localhost:3000/import-noc-interactions
```

## 📱 Alternative Servers/Ports

### If using different port
```bash
curl -X GET http://localhost:3001/import-noc-interactions
```

### If using different host
```bash
curl -X GET http://your-server.com:3000/import-noc-interactions
```

### If using HTTPS
```bash
curl -X GET https://your-server.com/import-noc-interactions
```

## ⏱️ Expected Response Times

- **Health check**: < 1 second
- **API test**: 10-30 seconds (tests all endpoints)
- **Individual imports**: 30 seconds - 5 minutes (depending on data size)
- **Full import**: 5-15 minutes (all data types)
- **Table info**: < 5 seconds

## 📊 Response Examples

### Successful Import Response
```json
{
  "success": true,
  "message": "NOC interactions data processed successfully",
  "recordsProcessed": 166
}
```

### Error Response
```json
{
  "success": false,
  "message": "NOC interactions import failed",
  "error": "Connection timeout",
  "suggestion": "Try again - the API might be temporarily slow"
}
```