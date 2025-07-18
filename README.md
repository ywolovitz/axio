# 📊 CSV Import Server

A robust Node.js server for automated CSV data imports with scheduled daily processing and comprehensive API management.

## 🚀 Features

- **10 Data Types**: Buildings, Cases, Conversations, Interactions, User State Interactions, Users, User Session History, Schedule, SLA Policy, NOC Interactions
- **Automated Scheduling**: Daily imports with cron jobs
- **API Management**: Full CRUD operations via REST endpoints
- **Error Handling**: Comprehensive error tracking and recovery
- **Progress Monitoring**: Real-time import progress and statistics
- **Table Management**: Dynamic table reset and structure management

## 📦 Installation

```bash
# Clone or create project directory
mkdir csv-import-server && cd csv-import-server

# Install dependencies
npm install express axios csv-parser mysql2 dotenv node-cron

# Create environment file
cp .env.example .env
# Edit .env with your database credentials
```

## ⚙️ Configuration

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
```

## 🎯 Quick Start

```bash
# Start the server
node server.js

# Server will start on http://localhost:3000
```

## 📡 API Endpoints

### 🏥 Health Check
```bash
# Check server status and scheduler info
curl http://localhost:3000/health
```

### 🔍 Test API Connectivity
```bash
# Test all data source APIs before importing
curl http://localhost:3000/test-api
```

### 📊 View Database Info
```bash
# Get table structures and row counts
curl http://localhost:3000/table-info
```

## 📥 Data Import Endpoints

### Individual Data Imports
```bash
# Import buildings data
curl http://localhost:3000/import-buildings

# Import cases data
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
# Import all data types in sequence
curl http://localhost:3000/import-all
```

## 📅 Scheduler Management

### Check Scheduler Status
```bash
curl http://localhost:3000/scheduler/status

# Response example:
{
  "success": true,
  "scheduler": {
    "isScheduled": true,
    "nextRun": "Daily at 2:00 AM (Africa/Johannesburg)",
    "timezone": "Africa/Johannesburg",
    "schedule": "0 2 * * *",
    "isRunning": true,
    "lastCheck": "2025-01-18T10:30:00.000Z"
  }
}
```

### Start Automated Daily Imports
```bash
curl -X POST http://localhost:3000/scheduler/start

# Response:
{
  "success": true,
  "message": "Scheduled daily imports started successfully",
  "schedule": "Daily at 2:00 AM (Africa/Johannesburg)"
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
# Manually start import process (useful for testing)
curl -X POST http://localhost:3000/scheduler/trigger

# Response:
{
  "success": true,
  "message": "Manual import completed successfully",
  "report": {
    "totalRecords": 15420,
    "successRate": "98.5%",
    "overallSuccess": true
  },
  "duration": "4.2 minutes"
}
```

## 🔧 Database Table Management

### Reset Individual Tables
```bash
# Reset buildings table structure
curl http://localhost:3000/reset-buildings-table

# Reset cases table structure
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

## ⏰ Schedule Configuration

### Default Schedule
- **Daily at 2:00 AM** (Africa/Johannesburg timezone)
- **Cron Expression**: `0 2 * * *`

### Custom Schedule Options
Edit `scheduler.js` to modify the schedule:

```javascript
const CRON_CONFIG = {
  dailyImport: '0 2 * * *',        // Daily at 2:00 AM
  // everyHour: '0 * * * *',       // Every hour
  // every6Hours: '0 */6 * * *',   // Every 6 hours
  // weekdays9AM: '0 9 * * 1-5',   // Weekdays at 9 AM
  // sunday3AM: '0 3 * * 0',       // Sundays at 3 AM
  
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

## 📊 Typical Workflow

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
# Start daily scheduled imports
curl -X POST http://localhost:3000/scheduler/start

# Verify scheduler is running
curl http://localhost:3000/scheduler/status
```

### 3. Monitor Operations
```bash
# Check database status
curl http://localhost:3000/table-info

# Check import logs (server console)
# View scheduler status
curl http://localhost:3000/scheduler/status
```

## 🐛 Troubleshooting

### Common Issues

#### API Connection Problems
```bash
# Test individual API endpoints
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

#### Import Failures
```bash
# Try manual import to see detailed logs
curl -X POST http://localhost:3000/scheduler/trigger

# Reset problematic tables
curl http://localhost:3000/reset-cases-table
```

### Log Monitoring
Server logs provide detailed information:
- Import progress (every 1000 records)
- Success/failure counts
- Processing duration
- Error details

## 📁 Project Structure

```
csv-import-server/
├── config.js           # Configuration and URLs
├── database.js         # Database connection and table schemas
├── datahandlers.js     # Data processing and insertion logic
├── csvUtils.js         # CSV download and parsing utilities
├── scheduler.js        # Cron job and automation logic
├── server.js          # Express server and API routes
├── .env              # Environment variables
├── .gitignore        # Git ignore rules
└── README.md         # This file
```

## 🔒 Security Notes

- Never commit `.env` file to version control
- API credentials are stored in environment variables
- Database connections use connection pooling
- Error messages don't expose sensitive information

## 📈 Performance

- **Batch Processing**: Data processed in chunks for memory efficiency
- **Progress Tracking**: Real-time progress indicators
- **Error Recovery**: Continues processing if individual records fail
- **Connection Management**: Proper database connection handling

## 🎯 Production Deployment

### Using PM2
```bash
# Install PM2
npm install -g pm2

# Start with PM2
pm2 start server.js --name "csv-import-server"

# Enable auto-restart on system reboot
pm2 startup
pm2 save
```

### Auto-start Scheduler
Uncomment these lines in `server.js` for automatic scheduler startup:
```javascript
// console.log('\n📅 Starting automatic daily import scheduler...');
// scheduledImportJob = startScheduledImports();
```

---

## 📞 Support

For issues or questions:
1. Check server logs for detailed error information
2. Use `/health` endpoint to verify system status
3. Test individual components using the provided curl commands
4. Verify environment configuration and database connectivity