# CSV Import Server

A robust Node.js server for automated CSV data imports with scheduled hourly processing, comprehensive API management, and filtered date-range imports.

## Features

- **10 Data Types**: Buildings, Cases, Conversations, Interactions, User State Interactions, Users, User Session History, Schedule, SLA Policy, NOC Interactions
- **Automated Scheduling**: Hourly imports with cron jobs and enhanced memory management
- **Filtered Imports**: Import data by specific date ranges while preserving existing records
- **API Management**: Full CRUD operations via REST endpoints
- **Error Handling**: Comprehensive error tracking and recovery with special Cases optimization
- **Progress Monitoring**: Real-time import progress and statistics with memory tracking
- **Table Management**: Dynamic table reset and structure management
- **Data Preservation**: Filtered imports append data without clearing existing records

## Installation

```bash
# Clone or create project directory
mkdir csv-import-server && cd csv-import-server

# Install dependencies
npm install express axios csv-parser mysql2 dotenv node-cron

# Create environment file
cp .env.example .env
# Edit .env with your database credentials
```

## Configuration

### Environment Variables (`.env`)
```env
# Server Configuration
EXPRESS_PORT=3000
PORT=3000

# Database Configuration
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=root
DB_NAME=ysw_data
DB_PORT=3306

# API Configuration
API_TOKEN=your_api_token
API_UID=your_api_uid
```

## Quick Start

```bash
# Start the server
node server.js

# Server will start on http://localhost:3000
```

## API Endpoints

### Health Check
```bash
# Check server status and scheduler info
curl http://localhost:3000/health
```

### Test API Connectivity
```bash
# Test all data source APIs before importing
curl http://localhost:3000/test-api
```

### View Database Info
```bash
# Get table structures and row counts
curl http://localhost:3000/table-info
```

### View Server Logs
```bash
# Get recent server logs
curl http://localhost:3000/logs

# Get specific number of log lines
curl "http://localhost:3000/logs?lines=100"
```

## Data Import Endpoints

### Individual Data Imports
```bash
# Import buildings data
curl http://localhost:3000/import-buildings

# Import cases data (optimized with extended timeouts)
curl http://localhost:3000/import-cases

# Import conversations data
curl http://localhost:3000/import-conversations

# Import interactions data
curl http://localhost:3000/import-interactions

# Import user state interactions
curl http://localhost:3000/import-user-state-interactions

# Import users data
curl http://localhost:3000/import-users

# Import user session history
curl http://localhost:3000/import-user-session-history

# Import schedule data
curl http://localhost:3000/import-schedule

# Import SLA policy data
curl http://localhost:3000/import-sla-policy

# Import NOC interactions data
curl http://localhost:3000/import-noc-interactions
```

### Import All Data
```bash
# Import all data types in optimized sequence (Cases processed last)
curl http://localhost:3000/import-all
```

### ⭐ NEW: Filtered Date Range Imports

Import data for specific date ranges while preserving existing database records.

```bash
# Import cases for April 2025 (preserves existing data)
curl -X POST http://localhost:3000/import-filtered-data \
  -H "Content-Type: application/json" \
  -d '{
    "id": "5002645397",
    "startDate": "2025-04-01",
    "endDate": "2025-04-30"
  }'

# Import conversations for a specific week
curl -X POST http://localhost:3000/import-filtered-data \
  -H "Content-Type: application/json" \
  -d '{
    "id": "5002207692",
    "startDate": "2025-07-01",
    "endDate": "2025-07-07"
  }'

# Import buildings data for specific month
curl -X POST http://localhost:3000/import-filtered-data \
  -H "Content-Type: application/json" \
  -d '{
    "id": "5077534948",
    "startDate": "2025-05-01",
    "endDate": "2025-05-31"
  }'
```

#### Supported Export IDs for Filtered Imports

| Data Type | Export ID | Description |
|-----------|-----------|-------------|
| Buildings | 5077534948 | Building information |
| Cases | 5002645397 | Support cases and tickets |
| Conversations | 5002207692 | Conversation history |
| Interactions | 5053863837 | User interactions |
| NOC Interactions | 5157703494 | Network Operations Center interactions |
| User State Interactions | 4693855982 | User state changes |
| Users | 5157670999 | User accounts |
| User Session History | 5219392695 | Session history |
| Schedule | 20348692306 | Schedule data |
| SLA Policy | 20357111093 | Service Level Agreement policies |

#### Filtered Import Response Example

```json
{
  "success": true,
  "message": "Successfully processed 45 filtered records",
  "exportId": "5002645397",
  "dataType": "cases",
  "dateRange": {
    "startDate": "2025-04-01",
    "endDate": "2025-04-30"
  },
  "results": {
    "totalRecordsRetrieved": 1250,
    "filteredRecordsFound": 45,
    "methodUsed": 1,
    "jsonFile": {
      "filename": "cases_5002645397_20250401_to_20250430_2025-07-23T10-30-45.json",
      "filepath": "./exports/cases_5002645397_20250401_to_20250430_2025-07-23T10-30-45.json",
      "recordCount": 45
    },
    "database": {
      "recordsProcessed": 45,
      "recordsInserted": 42,
      "duplicatesSkipped": 3,
      "tableName": "cases",
      "preservedExisting": true,
      "note": "Data appended without clearing existing records"
    }
  },
  "duration": "8.45s"
}
```

## Scheduler Management

### Check Scheduler Status
```bash
curl http://localhost:3000/scheduler/status

# Enhanced response with memory monitoring:
{
  "success": true,
  "scheduler": {
    "isScheduled": true,
    "nextRun": "Every hour at minute 0 (00:00, 01:00, 02:00, etc.) (Africa/Johannesburg)",
    "timezone": "Africa/Johannesburg",
    "schedule": "0 * * * *",
    "isRunning": true,
    "lastCheck": "2025-01-18T10:30:00.000Z",
    "memoryUsage": {
      "heapUsed": "125.4MB",
      "heapTotal": "200.1MB",
      "rss": "180.2MB"
    },
    "casesOptimizations": {
      "extendedTimeout": "5 minutes",
      "retryAttempts": "3",
      "memoryManagement": "enabled",
      "importOrder": "cases processed last"
    }
  }
}
```

### Start Automated Hourly Imports
```bash
curl -X POST http://localhost:3000/scheduler/start

# Response:
{
  "success": true,
  "message": "Scheduled hourly imports started successfully",
  "schedule": "Every hour at minute 0 (Africa/Johannesburg)"
}
```

### Stop Scheduled Imports
```bash
curl -X POST http://localhost:3000/scheduler/stop

# Response:
{
  "success": true,
  "message": "Scheduled imports stopped successfully"
}
```

### Trigger Manual Import
```bash
# Manually start import process with enhanced Cases handling
curl -X POST http://localhost:3000/scheduler/trigger

# Response:
{
  "success": true,
  "message": "Manual import completed successfully",
  "report": {
    "totalRecords": 25420,
    "successRate": "98.5%",
    "overallSuccess": true
  },
  "duration": "8.7 minutes"
}
```

## Database Table Management

### Reset Individual Tables
```bash
# Reset buildings table structure
curl http://localhost:3000/reset-buildings-table

# Reset cases table structure (clears all data)
curl http://localhost:3000/reset-cases-table

# Reset conversations table structure
curl http://localhost:3000/reset-conversations-table

# Reset interactions table structure
curl http://localhost:3000/reset-interactions-table

# Reset user state interactions table
curl http://localhost:3000/reset-user-state-interactions-table

# Reset users table structure
curl http://localhost:3000/reset-users-table

# Reset user session history table
curl http://localhost:3000/reset-user-session-history-table

# Reset schedule table structure
curl http://localhost:3000/reset-schedule-table

# Reset SLA policy table structure
curl http://localhost:3000/reset-sla-policy-table

# Reset NOC interactions table structure
curl http://localhost:3000/reset-noc-interactions-table
```

## Schedule Configuration

### Default Schedule
- **Every Hour** at minute 0 (00:00, 01:00, 02:00, etc.)
- **Timezone**: Africa/Johannesburg
- **Cron Expression**: `0 * * * *`
- **Special Cases Handling**: Extended timeouts, retry logic, memory management

### Custom Schedule Options
Edit `scheduler.js` to modify the schedule:

```javascript
const CRON_CONFIG = {
  // Current default
  everyHour: '0 * * * *',          // Every hour at minute 0
  
  // Alternative schedules:
  dailyImport: '0 2 * * *',        // Daily at 2:00 AM
  every3Hours: '0 */3 * * *',      // Every 3 hours
  every6Hours: '0 */6 * * *',      // Every 6 hours
  every12Hours: '0 */12 * * *',    // Every 12 hours
  weekdays9AM: '0 9 * * 1-5',      // Weekdays at 9 AM
  sunday3AM: '0 3 * * 0',          // Sundays at 3 AM
  
  timezone: 'Africa/Johannesburg'
};
```

### Cron Expression Format
```
┌─────────────────────  minute (0 - 59)
│ ┌───────────────────  hour (0 - 23)
│ │ ┌─────────────────  day of month (1 - 31)
│ │ │ ┌───────────────  month (1 - 12)
│ │ │ │ ┌─────────────  day of week (0 - 6) (Sunday to Saturday)
│ │ │ │ │
* * * * *
```

## Typical Workflows

### 1. Setup and Test
```bash
# 1. Start server
node server.js

# 2. Check health
curl http://localhost:3000/health

# 3. Test API connectivity
curl http://localhost:3000/test-api

# 4. Test manual import
curl -X POST http://localhost:3000/scheduler/trigger
```

### 2. Enable Automation
```bash
# Start hourly scheduled imports
curl -X POST http://localhost:3000/scheduler/start

# Verify scheduler is running
curl http://localhost:3000/scheduler/status
```

### 3. Monitor Operations
```bash
# Check database status
curl http://localhost:3000/table-info

# View recent logs
curl http://localhost:3000/logs

# Check scheduler status with memory info
curl http://localhost:3000/scheduler/status
```

### 4. Use Filtered Imports for Specific Data
```bash
# Import only April 2025 cases (preserves existing data)
curl -X POST http://localhost:3000/import-filtered-data \
  -H "Content-Type: application/json" \
  -d '{
    "id": "5002645397",
    "startDate": "2025-04-01",
    "endDate": "2025-04-30"
  }'

# Check the ./exports directory for JSON files
ls -la ./exports/

# Verify data was appended to database
curl http://localhost:3000/table-info
```

## Key Features

### 🔄 Enhanced Scheduler
- **Hourly imports** instead of daily for more frequent data updates
- **Memory management** with automatic garbage collection
- **Cases optimization** with extended timeouts and retry logic
- **Import order optimization** (Cases processed last to prevent memory issues)

### 📊 Filtered Imports
- **Date range filtering** for importing specific time periods
- **Data preservation** - existing records are never deleted
- **Duplicate handling** - automatically skips existing records
- **JSON exports** - creates timestamped files in `./exports/` directory
- **Multiple filtering methods** - API-level and pattern matching fallbacks

### 💾 Memory Management
- **Real-time monitoring** of heap usage and RSS memory
- **Garbage collection triggers** at configurable thresholds
- **Memory logging** during large dataset processing
- **Optimized import order** to minimize memory pressure

### 🎯 Cases Dataset Optimization
- **Extended timeouts** (5 minutes vs standard 2 minutes)
- **Retry attempts** (3 attempts with progressive delays)
- **Special error handling** and logging
- **Processed last** in import sequence to prevent memory issues

## Troubleshooting

### Common Issues

#### API Connection Problems
```bash
# Test individual API endpoints first
curl http://localhost:3000/test-api
```

#### Database Connection Issues
```bash
# Check health endpoint for database status
curl http://localhost:3000/health
```

#### Scheduler Not Working
```bash
# Check if scheduler is running
curl http://localhost:3000/scheduler/status

# Stop and restart scheduler
curl -X POST http://localhost:3000/scheduler/stop
curl -X POST http://localhost:3000/scheduler/start
```

#### Import Failures (Especially Cases)
```bash
# Try manual import to see detailed logs
curl -X POST http://localhost:3000/scheduler/trigger

# Check memory usage in scheduler status
curl http://localhost:3000/scheduler/status

# Reset problematic tables if needed
curl http://localhost:3000/reset-cases-table
```

#### Memory Issues
```bash
# Monitor memory usage during imports
curl http://localhost:3000/scheduler/status

# Use filtered imports for smaller datasets
curl -X POST http://localhost:3000/import-filtered-data \
  -H "Content-Type: application/json" \
  -d '{"id": "5002645397", "startDate": "2025-01-01", "endDate": "2025-01-07"}'
```

#### Filtered Import Issues
```bash
# Check supported export IDs
curl http://localhost:3000/

# Verify date format (YYYY-MM-DD)
curl -X POST http://localhost:3000/import-filtered-data \
  -H "Content-Type: application/json" \
  -d '{"id": "5002645397", "startDate": "2025-04-01", "endDate": "2025-04-30"}'

# Check exports directory for JSON files
ls -la ./exports/
```

### Log Monitoring
Enhanced server logs provide detailed information:
- Import progress (every 1000 records)
- Memory usage at key points
- Success/failure counts with duplicate tracking
- Processing duration
- Error details with suggestions
- Cases-specific optimization info

## Project Structure

```
csv-import-server/
├── config.js              # Configuration and URLs
├── database.js             # Database connection and table schemas
├── datahandlers.js         # Data processing and insertion logic
├── csvUtils.js             # CSV download and parsing utilities
├── scheduler.js            # Enhanced cron job and automation logic
├── filteredImport.js       # NEW: Filtered date-range import endpoint
├── server.js              # Express server and API routes
├── logs/                  # Log files directory
├── exports/               # NEW: JSON export files from filtered imports
├── .env                   # Environment variables
├── .gitignore             # Git ignore rules
└── README.md              # This file
```

## Security Notes

- Never commit `.env` file to version control
- API credentials are stored in environment variables
- Database connections use connection pooling
- Error messages don't expose sensitive information
- Filtered import JSON files contain metadata but no sensitive API tokens

## Performance

- **Enhanced Batch Processing**: Data processed in optimized chunks for memory efficiency
- **Real-time Progress Tracking**: Progress indicators with memory monitoring
- **Advanced Error Recovery**: Continues processing if individual records fail
- **Memory Management**: Automatic garbage collection and heap monitoring
- **Connection Management**: Proper database connection handling with pooling
- **Import Order Optimization**: Large datasets processed last to prevent memory issues

## Production Deployment

### Using PM2
```bash
# Install PM2
npm install -g pm2

# Start with PM2 and enable memory monitoring
pm2 start server.js --name "csv-import-server" --max-memory-restart 1G

# Enable auto-restart on system reboot
pm2 startup
pm2 save

# Monitor logs
pm2 logs csv-import-server

# Monitor memory usage
pm2 monit
```

### Auto-start Scheduler
Uncomment these lines in `server.js` for automatic scheduler startup:
```javascript
// logger('\n📅 Starting automatic hourly import scheduler...');
// scheduledImportJob = startScheduledImports();
```

### Enable Node.js Garbage Collection
For production, enable garbage collection monitoring:
```bash
# Start with garbage collection exposed
node --expose-gc server.js

# Or with PM2
pm2 start server.js --name "csv-import-server" --node-args="--expose-gc"
```

---

## Support

For issues or questions:
1. Check server logs at `http://localhost:3000/logs` for detailed error information
2. Use `/health` endpoint to verify system status and memory usage
3. Test individual components using the provided curl commands
4. For filtered imports, check the `./exports/` directory for JSON output files
5. Verify environment configuration and database connectivity
6. Monitor memory usage via `/scheduler/status` for performance issues

## Changelog

### Version 2.1.0 (Latest)
-  **NEW**: Filtered date-range imports with data preservation
-  **Enhanced**: Hourly scheduler instead of daily
-  **Improved**: Cases dataset optimization with extended timeouts
-  **Added**: Real-time memory monitoring and management
-  **Added**: JSON export functionality for filtered data
-  **Enhanced**: Comprehensive error handling and logging
-  **Optimized**: Import order for better memory efficiency