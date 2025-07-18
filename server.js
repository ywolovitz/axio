const express = require('express');
const { SERVER_CONFIG, EXPORT_URLS } = require('./config');
const { initDatabase, getConnection, closeDatabase } = require('./database');
const {
  saveBuildingsData,
  handleCasesData,
  handleConversationsData,
  handleInteractionsData,
  handleUserStateInteractionsData,
  handleUsersData,
  handleUserSessionHistoryData,
  handleScheduleData,
  handleSLAPolicyData
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

// Middleware
app.use(express.json());

// Root route with API documentation
app.get('/', (req, res) => {
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
      'GET /health - Health check'
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
    ]
  });
});

// Individual import routes
app.get('/import-buildings', async (req, res) => {
  const startTime = Date.now();
  
  try {
    console.log('🏢 Starting buildings import...');
    const buildingsData = await downloadAndParseCSV(EXPORT_URLS.buildings, 'buildings');
    await saveBuildingsData(buildingsData);
    
    logProcessingSummary('buildings', startTime, buildingsData.length);
    
    res.json({
      success: true,
      message: 'Buildings data imported successfully',
      recordsProcessed: buildingsData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    });
  } catch (error) {
    console.error('🏢 Buildings import failed:', error);
    const errorResponse = handleCSVError(error, 'buildings');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-cases', async (req, res) => {
  const startTime = Date.now();
  
  try {
    console.log('📋 Starting cases import...');
    const casesData = await downloadAndParseCSV(EXPORT_URLS.cases, 'cases');
    await handleCasesData(casesData);
    
    logProcessingSummary('cases', startTime, casesData.length);
    
    res.json({
      success: true,
      message: 'Cases data processed successfully',
      recordsProcessed: casesData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    });
  } catch (error) {
    console.error('📋 Cases import failed:', error);
    const errorResponse = handleCSVError(error, 'cases');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-conversations', async (req, res) => {
  const startTime = Date.now();
  
  try {
    console.log('💬 Starting conversations import...');
    const conversationsData = await downloadAndParseCSV(EXPORT_URLS.conversations, 'conversations');
    await handleConversationsData(conversationsData);
    
    logProcessingSummary('conversations', startTime, conversationsData.length);
    
    res.json({
      success: true,
      message: 'Conversations data processed successfully',
      recordsProcessed: conversationsData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    });
  } catch (error) {
    console.error('💬 Conversations import failed:', error);
    const errorResponse = handleCSVError(error, 'conversations');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-interactions', async (req, res) => {
  const startTime = Date.now();
  
  try {
    console.log('🔄 Starting interactions import...');
    const interactionsData = await downloadAndParseCSV(EXPORT_URLS.interactions, 'interactions');
    await handleInteractionsData(interactionsData);
    
    logProcessingSummary('interactions', startTime, interactionsData.length);
    
    res.json({
      success: true,
      message: 'Interactions data processed successfully',
      recordsProcessed: interactionsData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    });
  } catch (error) {
    console.error('🔄 Interactions import failed:', error);
    const errorResponse = handleCSVError(error, 'interactions');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-user-state-interactions', async (req, res) => {
  const startTime = Date.now();
  
  try {
    console.log('👤 Starting user state interactions import...');
    const userStateInteractionsData = await downloadAndParseCSV(EXPORT_URLS.userStateInteractions, 'user state interactions');
    await handleUserStateInteractionsData(userStateInteractionsData);
    
    logProcessingSummary('user state interactions', startTime, userStateInteractionsData.length);
    
    res.json({
      success: true,
      message: 'User state interactions data processed successfully',
      recordsProcessed: userStateInteractionsData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    });
  } catch (error) {
    console.error('👤 User state interactions import failed:', error);
    const errorResponse = handleCSVError(error, 'user state interactions');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-users', async (req, res) => {
  const startTime = Date.now();
  
  try {
    console.log('👥 Starting users import...');
    const usersData = await downloadAndParseCSV(EXPORT_URLS.users, 'users');
    await handleUsersData(usersData);
    
    logProcessingSummary('users', startTime, usersData.length);
    
    res.json({
      success: true,
      message: 'Users data processed successfully',
      recordsProcessed: usersData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    });
  } catch (error) {
    console.error('👥 Users import failed:', error);
    const errorResponse = handleCSVError(error, 'users');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-user-session-history', async (req, res) => {
  const startTime = Date.now();
  
  try {
    console.log('📅 Starting user session history import...');
    const userSessionHistoryData = await downloadAndParseCSV(EXPORT_URLS.userSessionHistory, 'user session history');
    await handleUserSessionHistoryData(userSessionHistoryData);
    
    logProcessingSummary('user session history', startTime, userSessionHistoryData.length);
    
    res.json({
      success: true,
      message: 'User session history data processed successfully',
      recordsProcessed: userSessionHistoryData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    });
  } catch (error) {
    console.error('📅 User session history import failed:', error);
    const errorResponse = handleCSVError(error, 'user session history');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-schedule', async (req, res) => {
  const startTime = Date.now();
  
  try {
    console.log('📋 Starting schedule import...');
    const scheduleData = await downloadAndParseCSV(EXPORT_URLS.schedule, 'schedule');
    await handleScheduleData(scheduleData);
    
    logProcessingSummary('schedule', startTime, scheduleData.length);
    
    res.json({
      success: true,
      message: 'Schedule data processed successfully',
      recordsProcessed: scheduleData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    });
  } catch (error) {
    console.error('📋 Schedule import failed:', error);
    const errorResponse = handleCSVError(error, 'schedule');
    res.status(500).json(errorResponse);
  }
});

app.get('/import-sla-policy', async (req, res) => {
  const startTime = Date.now();
  
  try {
    console.log('📊 Starting SLA Policy import...');
    const slaPolicyData = await downloadAndParseCSV(EXPORT_URLS.slaPolicy, 'SLA Policy');
    await handleSLAPolicyData(slaPolicyData);
    
    logProcessingSummary('SLA Policy', startTime, slaPolicyData.length);
    
    res.json({
      success: true,
      message: 'SLA Policy data processed successfully',
      recordsProcessed: slaPolicyData.length,
      duration: `${((Date.now() - startTime) / 1000).toFixed(2)}s`
    });
  } catch (error) {
    console.error('📊 SLA Policy import failed:', error);
    const errorResponse = handleCSVError(error, 'SLA Policy');
    res.status(500).json(errorResponse);
  }
});

// Import all data types
app.get('/import-all', async (req, res) => {
  const overallStartTime = Date.now();
  const results = {};
  
  try {
    console.log('🚀 Starting full import...');
    
    const importTasks = [
      { name: 'buildings', url: EXPORT_URLS.buildings, handler: saveBuildingsData, emoji: '🏢' },
      { name: 'cases', url: EXPORT_URLS.cases, handler: handleCasesData, emoji: '📋' },
      { name: 'conversations', url: EXPORT_URLS.conversations, handler: handleConversationsData, emoji: '💬' },
      { name: 'interactions', url: EXPORT_URLS.interactions, handler: handleInteractionsData, emoji: '🔄' },
      { name: 'userStateInteractions', url: EXPORT_URLS.userStateInteractions, handler: handleUserStateInteractionsData, emoji: '👤' },
      { name: 'users', url: EXPORT_URLS.users, handler: handleUsersData, emoji: '👥' },
      { name: 'userSessionHistory', url: EXPORT_URLS.userSessionHistory, handler: handleUserSessionHistoryData, emoji: '📅' },
      { name: 'schedule', url: EXPORT_URLS.schedule, handler: handleScheduleData, emoji: '📋' },
      { name: 'slaPolicy', url: EXPORT_URLS.slaPolicy, handler: handleSLAPolicyData, emoji: '📊' }
    ];
    
    for (let i = 0; i < importTasks.length; i++) {
      const task = importTasks[i];
      const taskStartTime = Date.now();
      
      try {
        console.log(`${task.emoji} Phase ${i + 1}: Importing ${task.name}...`);
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
        console.error(`${task.emoji} Failed to import ${task.name}:`, error.message);
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
    
    console.log('\n🎉 FULL IMPORT COMPLETED 🎉');
    console.log(`Total duration: ${(totalDuration / 1000).toFixed(2)} seconds`);
    console.log(`Overall success rate: ${report.successRate}`);
    console.log(`Total records processed: ${report.totalRecords}`);
    
    res.json({
      success: report.overallSuccess,
      message: report.overallSuccess ? 'All data imported successfully' : 'Import completed with some errors',
      totalDuration: `${(totalDuration / 1000).toFixed(2)}s`,
      report,
      results
    });
    
  } catch (error) {
    console.error('🚀 Full import failed:', error);
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
    const testResults = await testAllAPIEndpoints(EXPORT_URLS);
    res.json(testResults);
  } catch (error) {
    console.error('🔍 API test failed:', error);
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
    const connection = getConnection();
    
    // Get table structures and row counts
    const tables = ['Buildings', 'Cases', 'Conversations', 'Interactions', 'UserStateInteractions', 'Users', 'UserSessionHistory', 'Schedule', 'SLAPolicy'];
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
    
    res.json({
      success: true,
      tables: tableInfo
    });
  } catch (error) {
    console.error('Error getting table info:', error);
    res.status(500).json({
      success: false,
      message: 'Failed to get table information',
      error: error.message
    });
  }
});

// Health check route
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    version: '2.0.0'
  });
});

// Table reset routes (keeping the original functionality)
const tableResetRoutes = [
  { path: '/reset-cases-table', table: 'Cases', createFunction: 'createCasesTable' },
  { path: '/reset-conversations-table', table: 'Conversations', createFunction: 'createConversationsTable' },
  { path: '/reset-interactions-table', table: 'Interactions', createFunction: 'createInteractionsTable' },
  { path: '/reset-user-state-interactions-table', table: 'UserStateInteractions', createFunction: 'createUserStateInteractionsTable' },
  { path: '/reset-users-table', table: 'Users', createFunction: 'createUsersTable' },
  { path: '/reset-user-session-history-table', table: 'UserSessionHistory', createFunction: 'createUserSessionHistoryTable' },
  { path: '/reset-schedule-table', table: 'Schedule', createFunction: 'createScheduleTable' },
  { path: '/reset-sla-policy-table', table: 'SLAPolicy', createFunction: 'createSLAPolicyTable' }
];

// Dynamically create reset routes
tableResetRoutes.forEach(route => {
  app.get(route.path, async (req, res) => {
    try {
      console.log(`🔄 Resetting ${route.table} table structure...`);
      
      const connection = getConnection();
      
      // Drop existing table
      await connection.execute(`DROP TABLE IF EXISTS ${route.table}`);
      console.log(`✓ Dropped old ${route.table} table`);
      
      // Recreate table - we need to import the specific function
      const { [route.createFunction]: createFunction } = require('./database');
      await createFunction();
      console.log(`✓ Created new ${route.table} table`);
      
      res.json({
        success: true,
        message: `${route.table} table reset successfully with updated structure`,
        note: 'Table recreated with proper schema'
      });
    } catch (error) {
      console.error(`🔄 Failed to reset ${route.table} table:`, error);
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
  console.error('Unhandled error:', error);
  res.status(500).json({
    success: false,
    message: 'Internal server error',
    error: error.message
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: 'Endpoint not found',
    availableEndpoints: '/ for full API documentation'
  });
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  await closeDatabase();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('Received SIGTERM, shutting down gracefully...');
  await closeDatabase();
  process.exit(0);
});

// Start server
async function startServer() {
  try {
    await initDatabase();
    
    const server = app.listen(PORT, () => {
      console.log(`🚀 CSV Import Server v2.0.0 running on port ${PORT}`);
      console.log(`📖 Visit http://localhost:${PORT} for API documentation`);
      console.log(`🏥 Health check: http://localhost:${PORT}/health`);
      console.log(`🔍 Test APIs: http://localhost:${PORT}/test-api`);
    });
    
    server.on('error', (error) => {
      if (error.code === 'EADDRINUSE') {
        console.error(`❌ Port ${PORT} is already in use. Please:`);
        console.error('1. Check if another application is using this port');
        console.error('2. Kill any process using this port, or');
        console.error('3. Set EXPRESS_PORT=3001 in your .env file to use a different port');
        process.exit(1);
      } else {
        console.error('❌ Server error:', error);
        process.exit(1);
      }
    });
    
    return server;
  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
}

// Only start server if this file is run directly
if (require.main === module) {
  startServer();
}

module.exports = { app, startServer };