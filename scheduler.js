const cron = require('node-cron');
const fs = require('fs');
const path = require('path');
const { EXPORT_URLS, IMPORT_ORDER, MEMORY_CONFIG } = require('./config');
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
  createProcessingReport,
  logProcessingSummary,
  handleCSVError
} = require('./csvUtils'); // Use enhanced version

// Logging configuration
const LOG_CONFIG = {
  logDirectory: './logs',
  logFileName: 'import-scheduler.log',
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

// Enhanced logger with memory usage tracking
function logger(message, level = 'INFO') {
  const timestamp = new Date().toISOString();
  const memUsage = process.memoryUsage();
  const memMB = (memUsage.heapUsed / 1024 / 1024).toFixed(1);
  const logEntry = `[${timestamp}] [${level}] [MEM: ${memMB}MB] ${message}`;
  
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

// Memory management function
function forceGarbageCollection(datasetName, rowCount) {
  const threshold = MEMORY_CONFIG.gcThresholds[datasetName] || MEMORY_CONFIG.gcThresholds.default;
  
  if (rowCount % threshold === 0) {
    if (global.gc) {
      global.gc();
      const memUsage = process.memoryUsage();
      const memMB = (memUsage.heapUsed / 1024 / 1024).toFixed(1);
      logger(`🧹 Garbage collection triggered for ${datasetName} at ${rowCount} rows (Memory: ${memMB}MB)`);
    }
  }
}

// Enhanced memory monitoring
function logMemoryUsage(context = '') {
  const memUsage = process.memoryUsage();
  const formatMB = (bytes) => (bytes / 1024 / 1024).toFixed(1);
  
  logger(`💾 Memory Usage ${context}: Heap Used: ${formatMB(memUsage.heapUsed)}MB, Heap Total: ${formatMB(memUsage.heapTotal)}MB, RSS: ${formatMB(memUsage.rss)}MB`);
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

// Cron job configuration
const CRON_CONFIG = {
  // Run every hour (primary schedule)
  everyHour: '0 * * * *',
  
  // Run daily at 2:00 AM (backup option)
  dailyImport: '0 2 * * *',
  
  // Alternative schedules (uncomment the one you want):
  // every3Hours: '0 */3 * * *',
  // every6Hours: '0 */6 * * *',
  // every12Hours: '0 */12 * * *',
  // weekdays9AM: '0 9 * * 1-5',
  // sunday3AM: '0 3 * * 0',
  
  timezone: 'Africa/Johannesburg' // Set your timezone
};

// Map dataset names to their handlers
const DATASET_HANDLERS = {
  buildings: saveBuildingsData,
  cases: handleCasesData,
  conversations: handleConversationsData,
  interactions: handleInteractionsData,
  userStateInteractions: handleUserStateInteractionsData,
  users: handleUsersData,
  userSessionHistory: handleUserSessionHistoryData,
  schedule: handleScheduleData,
  slaPolicy: handleSLAPolicyData,
  nocInteractions: handleNOCInteractionsData
};

// Map dataset names to their emojis for logging
const DATASET_EMOJIS = {
  buildings: '🏢',
  cases: '📋',
  conversations: '💬',
  interactions: '🔄',
  userStateInteractions: '👤',
  users: '👥',
  userSessionHistory: '📅',
  schedule: '📋',
  slaPolicy: '📊',
  nocInteractions: '🔄'
};

// Enhanced import function with optimized order and error handling
async function performFullImport() {
  const overallStartTime = Date.now();
  const results = {};
  
  logger('\n🤖 === AUTOMATED HOURLY IMPORT STARTED ===');
  logger(`⏰ Started at: ${new Date().toLocaleString()}`);
  logMemoryUsage('(Import Start)');
  
  // Clean up old logs and force initial garbage collection
  cleanupOldLogs();
  if (global.gc) {
    global.gc();
    logger('🧹 Initial garbage collection completed');
  }
  
  // Use optimized import order (cases last)
  const orderedDatasets = IMPORT_ORDER.filter(name => EXPORT_URLS[name] && DATASET_HANDLERS[name]);
  
  logger(`📋 Import order: ${orderedDatasets.join(' → ')}`);
  logger(`🎯 Special handling for: cases (extended timeouts, retries, memory management)`);
  
  for (let i = 0; i < orderedDatasets.length; i++) {
    const datasetName = orderedDatasets[i];
    const taskStartTime = Date.now();
    const emoji = DATASET_EMOJIS[datasetName] || '📊';
    const handler = DATASET_HANDLERS[datasetName];
    const url = EXPORT_URLS[datasetName];
    
    try {
      logger(`${emoji} Phase ${i + 1}/${orderedDatasets.length}: Importing ${datasetName}...`);
      
      // Special logging for cases
      if (datasetName === 'cases') {
        logger(`🚨 Starting CASES import - this is typically the largest dataset and may take 5-10 minutes`);
        logger(`🔧 Using enhanced timeouts, retry logic, and memory management for cases`);
        logMemoryUsage('(Before Cases)');
      }
      
      // Download and parse data with enhanced error handling
      const data = await downloadAndParseCSV(url, datasetName);
      
      // Log memory usage for large datasets
      if (['cases', 'conversations', 'interactions'].includes(datasetName)) {
        logMemoryUsage(`(After ${datasetName} download)`);
      }
      
      // Process data with handler
      await handler(data);
      
      // Force garbage collection after large datasets
      if (['cases', 'conversations'].includes(datasetName) && global.gc) {
        global.gc();
        logMemoryUsage(`(After ${datasetName} processing)`);
      }
      
      const taskDuration = Date.now() - taskStartTime;
      results[datasetName] = {
        success: true,
        recordsProcessed: data.length,
        duration: taskDuration
      };
      
      logProcessingSummary(datasetName, taskStartTime, data.length);
      
      // Special success message for cases
      if (datasetName === 'cases') {
        logger(`🎉 CASES import completed successfully! This was the most challenging dataset.`);
        logger(`📊 Cases processed: ${data.length.toLocaleString()} records in ${(taskDuration/1000/60).toFixed(2)} minutes`);
      } else {
        logger(`✅ ${emoji} ${datasetName} completed successfully`);
      }
      
    } catch (error) {
      const errorInfo = handleCSVError(error, datasetName);
      
      // Enhanced error logging for cases
      if (datasetName === 'cases') {
        logError(`💥 CASES IMPORT FAILED - ${errorInfo.message}`, error);
        logger(`📋 Cases Error Details:`);
        logger(`   - Error Type: ${error.code || 'Unknown'}`);
        logger(`   - Suggestion: ${errorInfo.suggestion}`);
        logger(`   - Retry Recommendation: ${errorInfo.retryRecommendation}`);
        
        // Log current memory state in case of memory issues
        logMemoryUsage('(Cases Error State)');
      } else {
        logError(`❌ ${emoji} Failed to import ${datasetName}`, error);
      }
      
      results[datasetName] = {
        success: false,
        error: error.message,
        errorType: error.code || 'Unknown',
        recordsProcessed: 0,
        duration: Date.now() - taskStartTime
      };
      
      // For cases failure, add additional context
      if (datasetName === 'cases') {
        results[datasetName].errorContext = errorInfo;
        results[datasetName].criticalFailure = true;
      }
    }
    
    // Small delay between imports to prevent API overload
    if (i < orderedDatasets.length - 1) {
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
  
  const totalDuration = Date.now() - overallStartTime;
  const report = createProcessingReport(results);
  
  // Log final summary with special attention to cases
  logger('\n🎉 === AUTOMATED HOURLY IMPORT COMPLETED ===');
  logger(`⏰ Completed at: ${new Date().toLocaleString()}`);
  logger(`⏱️ Total duration: ${(totalDuration / 1000 / 60).toFixed(2)} minutes`);
  logger(`📊 Overall success rate: ${report.successRate}`);
  logger(`📈 Total records processed: ${report.totalRecords.toLocaleString()}`);
  
  // Special reporting for cases
  if (results.cases) {
    if (results.cases.success) {
      logger(`🏆 CASES SUCCESS: ${results.cases.recordsProcessed.toLocaleString()} cases imported in ${(results.cases.duration/1000/60).toFixed(2)} minutes`);
    } else {
      logger(`💔 CASES FAILED: ${results.cases.error}`);
      if (results.cases.errorContext) {
        logger(`💡 Next steps: ${results.cases.errorContext.suggestion}`);
      }
    }
  }
  
  // Log any failures
  const failures = Object.entries(results).filter(([_, result]) => !result.success);
  if (failures.length > 0) {
    logger(`⚠️ Failed imports: ${failures.map(([name]) => name).join(', ')}`);
    
    // Check if only cases failed
    if (failures.length === 1 && failures[0][0] === 'cases') {
      logger(`ℹ️ Only cases failed - all other imports were successful. This is a known issue with the cases dataset size.`);
    }
  }
  
  logMemoryUsage('(Import Complete)');
  logger('================================================\n');
  
  return {
    success: report.overallSuccess,
    report,
    results,
    duration: totalDuration,
    casesSpecificInfo: results.cases || null
  };
}

// Enhanced notification function with cases-specific messaging
async function sendNotification(importResult) {
  const { success, report, duration, casesSpecificInfo } = importResult;
  
  let message;
  if (success) {
    message = `✅ Hourly import completed successfully in ${(duration / 1000 / 60).toFixed(2)} minutes. ${report.totalRecords.toLocaleString()} records processed.`;
    
    if (casesSpecificInfo?.success) {
      message += ` Cases: ${casesSpecificInfo.recordsProcessed.toLocaleString()} records.`;
    }
  } else {
    message = `❌ Hourly import completed with errors. Success rate: ${report.successRate}.`;
    
    if (casesSpecificInfo && !casesSpecificInfo.success) {
      message += ` Cases failed: ${casesSpecificInfo.error}`;
    }
    
    message += ` Check logs for details.`;
  }
  
  logger(`📢 NOTIFICATION: ${message}`);
  
  // TODO: Add your notification logic here
  // Examples:
  // - Send email via nodemailer
  // - Post to Slack webhook
  // - Send to monitoring service
  // - Write to external log service
}

// Schedule the hourly import job
function startScheduledImports() {
  logger('📅 Setting up scheduled data imports...');
  logger(`🕐 Hourly import scheduled for: ${CRON_CONFIG.everyHour} (${CRON_CONFIG.timezone})`);
  logger('⏰ Import will run every hour at minute 0 (00:00, 01:00, 02:00, etc.)');
  logger('🎯 Cases import optimized with extended timeouts and retry logic');
  
  // Hourly import cron job
  const importJob = cron.schedule(CRON_CONFIG.everyHour, async () => {
    try {
      logger('\n🚀 Starting scheduled hourly import...');
      const result = await performFullImport();
      await sendNotification(result);
    } catch (error) {
      logError('💥 Scheduled import failed', error);
      await sendNotification({
        success: false,
        report: { successRate: '0%', totalRecords: 0 },
        duration: 0,
        error: error.message,
        casesSpecificInfo: { success: false, error: 'Import job crashed' }
      });
    }
  }, {
    scheduled: false, // Start manually
    timezone: CRON_CONFIG.timezone
  });
  
  // Start the job
  importJob.start();
  logger('✅ Hourly import scheduler started successfully');
  
  return importJob;
}

// Function to stop scheduled imports
function stopScheduledImports(job) {
  if (job) {
    job.destroy();
    logger('🛑 Scheduled imports stopped');
  }
}

// Manual trigger function (for testing)
async function triggerManualImport() {
  logger('🔧 Manual import triggered...');
  try {
    const result = await performFullImport();
    await sendNotification(result);
    return result;
  } catch (error) {
    logError('💥 Manual import failed', error);
    throw error;
  }
}

// Health check for scheduler
function getSchedulerStatus() {
  const memUsage = process.memoryUsage();
  const status = {
    isScheduled: cron.getTasks().size > 0,
    nextRun: 'Every hour at minute 0 (00:00, 01:00, 02:00, etc.) (Africa/Johannesburg)',
    timezone: CRON_CONFIG.timezone,
    schedule: CRON_CONFIG.everyHour,
    logDirectory: LOG_CONFIG.logDirectory,
    currentLogFile: getCurrentLogFilePath(),
    memoryUsage: {
      heapUsed: `${(memUsage.heapUsed / 1024 / 1024).toFixed(1)}MB`,
      heapTotal: `${(memUsage.heapTotal / 1024 / 1024).toFixed(1)}MB`,
      rss: `${(memUsage.rss / 1024 / 1024).toFixed(1)}MB`
    },
    casesOptimizations: {
      extendedTimeout: '5 minutes',
      retryAttempts: '3',
      memoryManagement: 'enabled',
      importOrder: 'cases processed last'
    }
  };
  
  logger(`📊 Scheduler Status: ${JSON.stringify(status, null, 2)}`);
  return status;
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

// Function to trigger cases-only import (for testing)
async function triggerCasesOnlyImport() {
  logger('🔧 Manual CASES-ONLY import triggered...');
  const taskStartTime = Date.now();
  
  try {
    logMemoryUsage('(Before Cases-Only Import)');
    
    logger('📋 Starting cases import with enhanced configuration...');
    const data = await downloadAndParseCSV(EXPORT_URLS.cases, 'cases');
    
    logMemoryUsage('(After Cases Download)');
    
    await handleCasesData(data);
    
    const duration = Date.now() - taskStartTime;
    
    logMemoryUsage('(After Cases Processing)');
    
    logger(`🎉 Cases-only import completed: ${data.length.toLocaleString()} records in ${(duration/1000/60).toFixed(2)} minutes`);
    
    return {
      success: true,
      recordsProcessed: data.length,
      duration: duration
    };
  } catch (error) {
    const duration = Date.now() - taskStartTime;
    logError('💥 Cases-only import failed', error);
    
    return {
      success: false,
      error: error.message,
      duration: duration
    };
  }
}

module.exports = {
  startScheduledImports,
  stopScheduledImports,
  triggerManualImport,
  triggerCasesOnlyImport,
  getSchedulerStatus,
  performFullImport,
  getRecentLogs,
  logger,
  logError,
  CRON_CONFIG,
  LOG_CONFIG
};