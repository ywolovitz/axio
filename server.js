const express = require('express');
const fs = require('fs');
const path = require('path');
const { SERVER_CONFIG, EXPORT_URLS } = require('./config');
const { initDatabase, getConnection, closeDatabase } = require('./database');
const {
  startScheduledImports,
  stopScheduledImports,
  triggerManualImport,
  getSchedulerStatus
} = require('./scheduler');
const {
  saveBuildingsData,
  handleCasesData,
  handleConversationsData,
  handleInteractionsData,
  handleUserStateInteractionsData,
  handleUsersData,
  handleUserSessionHistoryData,
  handleScheduleData,
  handleSLAPolicyData,
  handleNOCInteractionsData
} = require('./datahandlers');
const {
  downloadAndParseCSV,
  testAllAPIEndpoints,
  handleCSVError,
  logProcessingSummary,
  createProcessingReport
} = require('./csvUtils');

const app = express();
const PORT = SERVER_CONFIG.port;

// Global variable to store the cron job reference
let scheduledImportJob = null;

// Logging configuration
const LOG_CONFIG = {
  logDirectory: './logs',
  logFileName: 'express-server.log',
  maxLogSizeBytes: 10 * 1024 * 1024, // 10MB
  keepLogDays: 30
};

// Ensure log directory exists
function ensureLogDirectory() {
  if (!fs.existsSync(LOG_CONFIG.logDirectory)) {
    fs.mkdirSync(LOG_CONFIG.logDirectory, { recursive: true });
  }
}

// Get current log file path with date
function getCurrentLogFilePath() {
  const today = new Date().toISOString().split('T')[0]; // YYYY-MM-DD
  const fileName = `${today}-${LOG_CONFIG.logFileName}`;
  return path.join(LOG_CONFIG.logDirectory, fileName);
}

// Custom logger that writes to both console and file
function logger(message, level = 'INFO') {
  const timestamp = new Date().toISOString();
  const logEntry = `[${timestamp}] [${level}] ${message}`;
  
  // Write to console
  console.log(message);
  
  // Write to file
  try {
    ensureLogDirectory();
    const logFilePath = getCurrentLogFilePath();
    fs.appendFileSync(logFilePath, logEntry + '\n', 'utf8');
  } catch (error) {
    console.error('Failed to write to log file:', error.message);
  }
}

// Custom error logger
function logError(message, error = null) {
  const errorMessage = error ? `${message}: ${error.message}` : message;
  logger(errorMessage, 'ERROR');
}

// Clean up old log files
function cleanupOldLogs() {
  try {
    const files = fs.readdirSync(LOG_CONFIG.logDirectory);
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - LOG_CONFIG.keepLogDays);
    
    files.forEach(file => {
      const filePath = path.join(LOG_CONFIG.logDirectory, file);
      const stats = fs.statSync(filePath);
      
      if (stats.mtime < cutoffDate && file.endsWith('.log')) {
        fs.unlinkSync(filePath);
        logger(`Deleted old log file: ${file}`);
      }
    });
  } catch (error) {
    logError('Failed to cleanup old logs', error);
  }
}

// Function to read recent logs (utility function)
function getRecentLogs(lines = 50) {
  try {
    const logFilePath = getCurrentLogFilePath();
    if (!fs.existsSync(logFilePath)) {
      return 'No log file found for today.';
    }
    
    const logContent = fs.readFileSync(logFilePath, 'utf8');
    const logLines = logContent.split('\n').filter(line => line.trim());
    const recentLines = logLines.slice(-lines);
    
    return recentLines.join('\n');
  } catch (error) {
    logError('Failed to read recent logs', error);
    return 'Error reading log file.';
  }
}

// Middleware
app.use(express.json());

// Middleware to log all requests
app.use((req, res, next) => {
  logger(`${req.method} ${req.path} - ${req.ip}`, 'REQUEST');
  next();
});

// Root route with API documentation
app.get('/', (req, res) => {
  logger('API documentation requested');
  res.json({ 
    message: 'CSV Import Server Running',
    version: '2.0.0',
    endpoints: [
      'GET /test-api - Test API connectivity before importing',
      'GET /import-buildings - Import buildings data',
      'GET /import-cases - Import cases data', 
      'GET /import-conversations - Import conversations data',
      'GET /import-interactions - Import interactions data',
      'GET /import-user-state-interactions - Import user state interactions data',
      'GET /import-users - Import users data',
      'GET /import-user-session-history - Import user session history data',
      'GET /import-schedule - Import schedule data',
      'GET /import-sla-policy - Import SLA Policy data',
      'GET /import-all - Import all data types',
      'GET /table-info - View table structures and row counts',
      'GET /health - Health check',
      'GET /logs - View recent server logs'
    ],
    resetEndpoints: [
      'GET /reset-cases-table - Reset Cases table structure',
      'GET /reset-conversations-table - Reset Conversations table structure',
      'GET /reset-interactions-table - Reset Interactions table structure',
      'GET /reset-user-state-interactions-table - Reset UserStateInteractions table structure',
      'GET /reset-users-table - Reset Users table structure',
      'GET /reset-user-session-history-table - Reset UserSessionHistory table structure',
      'GET /reset-schedule-table - Reset Schedule table structure',
      'GET /reset-sla-policy-table - Reset SLAPolicy table structure'
    ],
    schedulerEndpoints: [
      'GET /scheduler/status - Get scheduler status',
      'POST /scheduler/start - Start scheduled imports',
      'POST /scheduler/stop - Stop scheduled imports',
      'POST /scheduler/trigger - Trigger manual import'
    ]
  });
});

// New route to view recent logs
app.get('/logs', (req, res) => {
  try {
    const lines = req.query.lines ? parseInt(req.query.lines) : 50;
    const logs = getRecentLogs(lines);
    
    res.json({
      success: true,
      logFile: getCurrentLogFilePath(),
      recentLogs: logs,
      message: `Last ${lines} log entries`
    });
  } catch (error) {
    logError('Failed to retrieve logs', error);
    res.status(500).json({
      success: false,
      message: 'Failed to retrieve logs',
      error: error.message
    });
  }
});

// Individual import routes
app.get('/import-buildings', async (req, res) => {
  const startTime = Date.now();
  
  try {
    logger('🏢 Starting buildings import...');
    const buildingsData = await downloadAndParseCSV(EXPORT_URLS.buildings, 'buildings');
    await saveBuildingsData(buildingsData);
    
    logProcessingSummary('buildings', startTime, buildingsData.length);
    
    const response = {
      success: true,
      message: 'Buildings data imported successfully',
      recordsProcessed: buildingsData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    };
    
    logger(`🏢 Buildings import completed - ${buildingsData.length} records processed`);
    res.json(response);
  } catch (error) {
    logError('🏢 Buildings import failed', error);
    const errorResponse = handleCSVError(error, 'buildings');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-cases', async (req, res) => {
  const startTime = Date.now();
  
  try {
    logger('📋 Starting cases import...');
    const casesData = await downloadAndParseCSV(EXPORT_URLS.cases, 'cases');
    await handleCasesData(casesData);
    
    logProcessingSummary('cases', startTime, casesData.length);
    
    const response = {
      success: true,
      message: 'Cases data processed successfully',
      recordsProcessed: casesData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    };
    
    logger(`📋 Cases import completed - ${casesData.length} records processed`);
    res.json(response);
  } catch (error) {
    logError('📋 Cases import failed', error);
    const errorResponse = handleCSVError(error, 'cases');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-conversations', async (req, res) => {
  const startTime = Date.now();
  
  try {
    logger('💬 Starting conversations import...');
    const conversationsData = await downloadAndParseCSV(EXPORT_URLS.conversations, 'conversations');
    await handleConversationsData(conversationsData);
    
    logProcessingSummary('conversations', startTime, conversationsData.length);
    
    const response = {
      success: true,
      message: 'Conversations data processed successfully',
      recordsProcessed: conversationsData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    };
    
    logger(`💬 Conversations import completed - ${conversationsData.length} records processed`);
    res.json(response);
  } catch (error) {
    logError('💬 Conversations import failed', error);
    const errorResponse = handleCSVError(error, 'conversations');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-interactions', async (req, res) => {
  const startTime = Date.now();
  
  try {
    logger('🔄 Starting interactions import...');
    const interactionsData = await downloadAndParseCSV(EXPORT_URLS.interactions, 'interactions');
    await handleInteractionsData(interactionsData);
    
    logProcessingSummary('interactions', startTime, interactionsData.length);
    
    const response = {
      success: true,
      message: 'Interactions data processed successfully',
      recordsProcessed: interactionsData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    };
    
    logger(`🔄 Interactions import completed - ${interactionsData.length} records processed`);
    res.json(response);
  } catch (error) {
    logError('🔄 Interactions import failed', error);
    const errorResponse = handleCSVError(error, 'interactions');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-user-state-interactions', async (req, res) => {
  const startTime = Date.now();
  
  try {
    logger('👤 Starting user state interactions import...');
    const userStateInteractionsData = await downloadAndParseCSV(EXPORT_URLS.userStateInteractions, 'user state interactions');
    await handleUserStateInteractionsData(userStateInteractionsData);
    
    logProcessingSummary('user state interactions', startTime, userStateInteractionsData.length);
    
    const response = {
      success: true,
      message: 'User state interactions data processed successfully',
      recordsProcessed: userStateInteractionsData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    };
    
    logger(`👤 User state interactions import completed - ${userStateInteractionsData.length} records processed`);
    res.json(response);
  } catch (error) {
    logError('👤 User state interactions import failed', error);
    const errorResponse = handleCSVError(error, 'user state interactions');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-users', async (req, res) => {
  const startTime = Date.now();
  
  try {
    logger('👥 Starting users import...');
    const usersData = await downloadAndParseCSV(EXPORT_URLS.users, 'users');
    await handleUsersData(usersData);
    
    logProcessingSummary('users', startTime, usersData.length);
    
    const response = {
      success: true,
      message: 'Users data processed successfully',
      recordsProcessed: usersData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    };
    
    logger(`👥 Users import completed - ${usersData.length} records processed`);
    res.json(response);
  } catch (error) {
    logError('👥 Users import failed', error);
    const errorResponse = handleCSVError(error, 'users');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-user-session-history', async (req, res) => {
  const startTime = Date.now();
  
  try {
    logger('📅 Starting user session history import...');
    const userSessionHistoryData = await downloadAndParseCSV(EXPORT_URLS.userSessionHistory, 'user session history');
    await handleUserSessionHistoryData(userSessionHistoryData);
    
    logProcessingSummary('user session history', startTime, userSessionHistoryData.length);
    
    const response = {
      success: true,
      message: 'User session history data processed successfully',
      recordsProcessed: userSessionHistoryData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    };
    
    logger(`📅 User session history import completed - ${userSessionHistoryData.length} records processed`);
    res.json(response);
  } catch (error) {
    logError('📅 User session history import failed', error);
    const errorResponse = handleCSVError(error, 'user session history');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-schedule', async (req, res) => {
  const startTime = Date.now();
  
  try {
    logger('📋 Starting schedule import...');
    const scheduleData = await downloadAndParseCSV(EXPORT_URLS.schedule, 'schedule');
    await handleScheduleData(scheduleData);
    
    logProcessingSummary('schedule', startTime, scheduleData.length);
    
    const response = {
      success: true,
      message: 'Schedule data processed successfully',
      recordsProcessed: scheduleData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    };
    
    logger(`📋 Schedule import completed - ${scheduleData.length} records processed`);
    res.json(response);
  } catch (error) {
    logError('📋 Schedule import failed', error);
    const errorResponse = handleCSVError(error, 'schedule');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-sla-policy', async (req, res) => {
  const startTime = Date.now();
  
  try {
    logger('📊 Starting SLA Policy import...');
    const slaPolicyData = await downloadAndParseCSV(EXPORT_URLS.slaPolicy, 'SLA Policy');
    await handleSLAPolicyData(slaPolicyData);
    
    logProcessingSummary('SLA Policy', startTime, slaPolicyData.length);
    
    const response = {
      success: true,
      message: 'SLA Policy data processed successfully',
      recordsProcessed: slaPolicyData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    };
    
    logger(`📊 SLA Policy import completed - ${slaPolicyData.length} records processed`);
    res.json(response);
  } catch (error) {
    logError('📊 SLA Policy import failed', error);
    const errorResponse = handleCSVError(error, 'SLA Policy');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-noc-interactions', async (req, res) => {
  const startTime = Date.now();
  
  try {
    logger('🔄 Starting NOC interactions import...');
    const nocInteractionsData = await downloadAndParseCSV(EXPORT_URLS.nocInteractions, 'NOC interactions');
    await handleNOCInteractionsData(nocInteractionsData);
    
    logProcessingSummary('NOC interactions', startTime, nocInteractionsData.length);
    
    const response = {
      success: true,
      message: 'NOC interactions data processed successfully',
      recordsProcessed: nocInteractionsData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    };
    
    logger(`🔄 NOC interactions import completed - ${nocInteractionsData.length} records processed`);
    res.json(response);
  } catch (error) {
    logError('🔄 NOC interactions import failed', error);
    const errorResponse = handleCSVError(error, 'NOC interactions');
    res.status(500).json(errorResponse);
  }
});

// Import all data types
app.get('/import-all', async (req, res) => {
  const overallStartTime = Date.now();
  const results = {};
  
  try {
    logger('🚀 Starting full import...');
    
    const importTasks = [
      { name: 'buildings', url: EXPORT_URLS.buildings, handler: saveBuildingsData, emoji: '🏢' },
      { name: 'cases', url: EXPORT_URLS.cases, handler: handleCasesData, emoji: '📋' },
      { name: 'conversations', url: EXPORT_URLS.conversations, handler: handleConversationsData, emoji: '💬' },
      { name: 'interactions', url: EXPORT_URLS.interactions, handler: handleInteractionsData, emoji: '🔄' },
      { name: 'userStateInteractions', url: EXPORT_URLS.userStateInteractions, handler: handleUserStateInteractionsData, emoji: '👤' },
      { name: 'users', url: EXPORT_URLS.users, handler: handleUsersData, emoji: '👥' },
      { name: 'userSessionHistory', url: EXPORT_URLS.userSessionHistory, handler: handleUserSessionHistoryData, emoji: '📅' },
      { name: 'schedule', url: EXPORT_URLS.schedule, handler: handleScheduleData, emoji: '📋' },
      { name: 'slaPolicy', url: EXPORT_URLS.slaPolicy, handler: handleSLAPolicyData, emoji: '📊' },
      { name: 'nocInteractions', url: EXPORT_URLS.nocInteractions, handler: handleNOCInteractionsData, emoji: '🔄' }
    ];
    
    for (let i = 0; i < importTasks.length; i++) {
      const task = importTasks[i];
      const taskStartTime = Date.now();
      
      try {
        logger(`${task.emoji} Phase ${i + 1}: Importing ${task.name}...`);
        const data = await downloadAndParseCSV(task.url, task.name);
        await task.handler(data);
        
        const taskDuration = Date.now() - taskStartTime;
        results[task.name] = {
          success: true,
          recordsProcessed: data.length,
          duration: taskDuration
        };
        
        logProcessingSummary(task.name, taskStartTime, data.length);
        
      } catch (error) {
        logError(`${task.emoji} Failed to import ${task.name}`, error);
        results[task.name] = {
          success: false,
          error: error.message,
          recordsProcessed: 0,
          duration: Date.now() - taskStartTime
        };
      }
    }
    
    const totalDuration = Date.now() - overallStartTime;
    const report = createProcessingReport(results);
    
    logger('\n🎉 FULL IMPORT COMPLETED 🎉');
    logger(`Total duration: ${(totalDuration / 1000).toFixed(2)} seconds`);
    logger(`Overall success rate: ${report.successRate}`);
    logger(`Total records processed: ${report.totalRecords}`);
    
    res.json({
      success: report.overallSuccess,
      message: report.overallSuccess ? 'All data imported successfully' : 'Import completed with some errors',
      totalDuration: `${(totalDuration / 1000).toFixed(2)}s`,
      report,
      results
    });
    
  } catch (error) {
    logError('🚀 Full import failed', error);
    res.status(500).json({
      success: false,
      message: 'Full import failed',
      error: error.message,
      suggestion: 'Try importing data types separately'
    });
  }
});

// API testing route
app.get('/test-api', async (req, res) => {
  try {
    logger('🔍 Starting API connectivity test...');
    const testResults = await testAllAPIEndpoints(EXPORT_URLS);
    logger('🔍 API connectivity test completed');
    res.json(testResults);
  } catch (error) {
    logError('🔍 API test failed', error);
    res.status(500).json({
      success: false,
      message: 'API test failed',
      error: error.message
    });
  }
});

// Table information route
app.get('/table-info', async (req, res) => {
  try {
    logger('📊 Retrieving table information...');
    const connection = getConnection();
    
    // Get table structures and row counts
    const tables = ['Buildings', 'Cases', 'Conversations', 'Interactions', 'UserStateInteractions', 'Users', 'UserSessionHistory', 'Schedule', 'SLAPolicy', 'NOCInteractions'];
    const tableInfo = {};
    
    for (const table of tables) {
      try {
        const [columns] = await connection.execute(`DESCRIBE ${table}`);
        const [count] = await connection.execute(`SELECT COUNT(*) as count FROM ${table}`);
        
        tableInfo[table] = {
          columns: columns,
          rowCount: count[0].count
        };
      } catch (error) {
        tableInfo[table] = {
          error: `Table not found or error: ${error.message}`,
          rowCount: 0
        };
      }
    }
    
    logger('📊 Table information retrieved successfully');
    res.json({
      success: true,
      tables: tableInfo
    });
  } catch (error) {
    logError('Error getting table info', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get table information',
      error: error.message
    });
  }
});

// Health check route
app.get('/health', (req, res) => {
  const healthData = { 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    version: '2.0.0',
    scheduler: getSchedulerStatus(),
    logging: {
      logDirectory: LOG_CONFIG.logDirectory,
      currentLogFile: getCurrentLogFilePath()
    }
  };
  
  logger('Health check requested');
  res.json(healthData);
});

// Scheduler management routes
app.get('/scheduler/status', (req, res) => {
  try {
    const status = getSchedulerStatus();
    const response = {
      success: true,
      scheduler: {
        ...status,
        isRunning: scheduledImportJob !== null,
        lastCheck: new Date().toISOString()
      }
    };
    
    logger('Scheduler status requested');
    res.json(response);
  } catch (error) {
    logError('Failed to get scheduler status', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get scheduler status',
      error: error.message
    });
  }
});

app.post('/scheduler/start', (req, res) => {
  try {
    if (scheduledImportJob) {
      logger('Attempt to start scheduler that is already running');
      return res.json({
        success: false,
        message: 'Scheduler is already running'
      });
    }
    
    scheduledImportJob = startScheduledImports();
    logger('Scheduled daily imports started successfully');
    
    res.json({
      success: true,
      message: 'Scheduled daily imports started successfully',
      schedule: 'Daily at 2:00 AM (Africa/Johannesburg)'
    });
  } catch (error) {
    logError('Failed to start scheduler', error);
    res.status(500).json({
      success: false,
      message: 'Failed to start scheduler',
      error: error.message
    });
  }
});

app.post('/scheduler/stop', (req, res) => {
  try {
    if (!scheduledImportJob) {
      logger('Attempt to stop scheduler when none is running');
      return res.json({
        success: false,
        message: 'No scheduled imports are currently running'
      });
    }
    
    stopScheduledImports(scheduledImportJob);
    scheduledImportJob = null;
    logger('Scheduled imports stopped successfully');
    
    res.json({
      success: true,
      message: 'Scheduled imports stopped successfully'
    });
  } catch (error) {
    logError('Failed to stop scheduler', error);
    res.status(500).json({
      success: false,
      message: 'Failed to stop scheduler',
      error: error.message
    });
  }
});

app.post('/scheduler/trigger', async (req, res) => {
  try {
    logger('🔧 Manual import triggered via API...');
    const result = await triggerManualImport();
    
    const response = {
      success: result.success,
      message: result.success ? 'Manual import completed successfully' : 'Manual import completed with errors',
      report: result.report,
      duration: `${(result.duration / 1000 / 60).toFixed(2)} minutes`
    };
    
    logger(`Manual import completed via API - Success: ${result.success}`);
    res.json(response);
  } catch (error) {
    logError('💥 Manual import failed via API', error);
    res.status(500).json({
      success: false,
      message: 'Manual import failed',
      error: error.message
    });
  }
});

// Table reset routes (keeping the original functionality)
const tableResetRoutes = [
  { path: '/reset-buildings-table', table: 'Buildings', createFunction: 'createBuildingsTable' },
  { path: '/reset-cases-table', table: 'Cases', createFunction: 'createCasesTable' },
  { path: '/reset-conversations-table', table: 'Conversations', createFunction: 'createConversationsTable' },
  { path: '/reset-interactions-table', table: 'Interactions', createFunction: 'createInteractionsTable' },
  { path: '/reset-user-state-interactions-table', table: 'UserStateInteractions', createFunction: 'createUserStateInteractionsTable' },
  { path: '/reset-users-table', table: 'Users', createFunction: 'createUsersTable' },
  { path: '/reset-user-session-history-table', table: 'UserSessionHistory', createFunction: 'createUserSessionHistoryTable' },
  { path: '/reset-schedule-table', table: 'Schedule', createFunction: 'createScheduleTable' },
  { path: '/reset-sla-policy-table', table: 'SLAPolicy', createFunction: 'createSLAPolicyTable' },
  { path: '/reset-noc-interactions-table', table: 'NOCInteractions', createFunction: 'createNOCInteractionsTable' }
];

// Dynamically create reset routes
tableResetRoutes.forEach(route => {
  app.get(route.path, async (req, res) => {
    try {
      logger(`🔄 Resetting ${route.table} table structure...`);
      
      const connection = getConnection();
      
      // Drop existing table
      await connection.execute(`DROP TABLE IF EXISTS ${route.table}`);
      logger(`✓ Dropped old ${route.table} table`);
      
      // Recreate table - we need to import the specific function
      const { [route.createFunction]: createFunction } = require('./database');
      await createFunction();
      logger(`✓ Created new ${route.table} table`);
      
      const response = {
        success: true,
        message: `${route.table} table reset successfully with updated structure`,
        note: 'Table recreated with proper schema'
      };
      
      logger(`${route.table} table reset completed successfully`);
      res.json(response);
    } catch (error) {
      logError(`🔄 Failed to reset ${route.table} table`, error);
      res.status(500).json({
        success: false,
        message: `Failed to reset ${route.table} table`,
        error: error.message
      });
    }
  });
});

// Error handling middleware
app.use((error, req, res, next) => {
  logError('Unhandled error', error);
  res.status(500).json({
    success: false,
    message: 'Internal server error',
    error: error.message
  });
});

// 404 handler
app.use((req, res) => {
  logger(`404 - Endpoint not found: ${req.method} ${req.path}`, 'WARN');
  res.status(404).json({
    success: false,
    message: 'Endpoint not found',
    availableEndpoints: '/ for full API documentation'
  });
});

// Graceful shutdown
process.on('SIGINT', async () => {
  logger('Shutting down gracefully...');
  if (scheduledImportJob) {
    stopScheduledImports(scheduledImportJob);
  }
  await closeDatabase();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  logger('Received SIGTERM, shutting down gracefully...');
  if (scheduledImportJob) {
    stopScheduledImports(scheduledImportJob);
  }
  await closeDatabase();
  process.exit(0);
});

// Start server
async function startServer() {
  try {
    await initDatabase();
    
    // Clean up old logs on startup
    cleanupOldLogs();
    
    const server = app.listen(PORT, () => {
      logger(`🚀 CSV Import Server v2.0.0 running on port ${PORT}`);
      logger(`📖 Visit http://localhost:${PORT} for API documentation`);
      logger(`🏥 Health check: http://localhost:${PORT}/health`);
      logger(`🔍 Test APIs: http://localhost:${PORT}/test-api`);
      logger(`📅 Scheduler status: http://localhost:${PORT}/scheduler/status`);
      logger(`📋 View logs: http://localhost:${PORT}/logs`);
      logger(`📁 Log files are saved to: ${LOG_CONFIG.logDirectory}`);
      
      // Optionally start scheduler automatically (uncomment if desired)
      // logger('\n📅 Starting automatic daily import scheduler...');
      // scheduledImportJob = startScheduledImports();
    });
    
    server.on('error', (error) => {
      if (error.code === 'EADDRINUSE') {
        logError(`❌ Port ${PORT} is already in use. Please:`);
        logError('1. Check if another application is using this port');
        logError('2. Kill any process using this port, or');
        logError('3. Set EXPRESS_PORT=3001 in your .env file to use a different port');
        process.exit(1);
      } else {
        logError('❌ Server error', error);
        process.exit(1);
      }
    });
    
    return server;
  } catch (error) {
    logError('❌ Failed to start server', error);
    process.exit(1);
  }
}

// Only start server if this file is run directly
if (require.main === module) {
  startServer();
}

module.exports = { 
  app, 
  startServer, 
  logger, 
  logError, 
  getRecentLogs,
  LOG_CONFIG 
};