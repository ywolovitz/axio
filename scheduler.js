const cron = require('node-cron');
const fs = require('fs');
const path = require('path');
const { EXPORT_URLS } = require('./config');
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
  logProcessingSummary
} = require('./csvUtils');

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

// Cron job configuration
const CRON_CONFIG = {
  // Run every 3 hours (primary schedule)
  every3Hours: '0 */3 * * *',
  
  // Run daily at 2:00 AM (backup option)
  dailyImport: '0 2 * * *',
  
  // Alternative schedules (uncomment the one you want):
  // everyHour: '0 * * * *',
  // every6Hours: '0 */6 * * *',
  // every12Hours: '0 */12 * * *',
  // weekdays9AM: '0 9 * * 1-5',
  // sunday3AM: '0 3 * * 0',
  
  timezone: 'Africa/Johannesburg' // Set your timezone
};

// Import all data function (extracted from server route)
async function performFullImport() {
  const overallStartTime = Date.now();
  const results = {};
  
  logger('\n🤖 === AUTOMATED 3-HOURLY IMPORT STARTED ===');
  logger(`⏰ Started at: ${new Date().toLocaleString()}`);
  
  // Clean up old logs at the start of each import
  cleanupOldLogs();
  
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
      logger(`${task.emoji} Phase ${i + 1}/10: Importing ${task.name}...`);
      const data = await downloadAndParseCSV(task.url, task.name);
      await task.handler(data);
      
      const taskDuration = Date.now() - taskStartTime;
      results[task.name] = {
        success: true,
        recordsProcessed: data.length,
        duration: taskDuration
      };
      
      logProcessingSummary(task.name, taskStartTime, data.length);
      logger(`✅ ${task.emoji} ${task.name} completed successfully`);
      
    } catch (error) {
      logError(`❌ ${task.emoji} Failed to import ${task.name}`, error);
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
  
  // Log final summary
  logger('\n🎉 === AUTOMATED 3-HOURLY IMPORT COMPLETED ===');
  logger(`⏰ Completed at: ${new Date().toLocaleString()}`);
  logger(`⏱️ Total duration: ${(totalDuration / 1000 / 60).toFixed(2)} minutes`);
  logger(`📊 Overall success rate: ${report.successRate}`);
  logger(`📈 Total records processed: ${report.totalRecords}`);
  
  // Log any failures
  const failures = Object.entries(results).filter(([_, result]) => !result.success);
  if (failures.length > 0) {
    logger(`⚠️ Failed imports: ${failures.map(([name]) => name).join(', ')}`);
  }
  
  logger('================================================\n');
  
  return {
    success: report.overallSuccess,
    report,
    results,
    duration: totalDuration
  };
}

// Function to send notification (optional - can be extended)
async function sendNotification(importResult) {
  // You can extend this to send email, Slack, or webhook notifications
  const { success, report, duration } = importResult;
  
  const message = success 
    ? `✅ 3-hourly import completed successfully in ${(duration / 1000 / 60).toFixed(2)} minutes. ${report.totalRecords} records processed.`
    : `❌ 3-hourly import completed with errors. Success rate: ${report.successRate}. Check logs for details.`;
  
  logger(`📢 NOTIFICATION: ${message}`);
  
  // TODO: Add your notification logic here
  // Examples:
  // - Send email via nodemailer
  // - Post to Slack webhook
  // - Send to monitoring service
  // - Write to external log service
}

// Schedule the 3-hourly import job
function startScheduledImports() {
  logger('📅 Setting up scheduled data imports...');
  logger(`🕐 3-hourly import scheduled for: ${CRON_CONFIG.every3Hours} (${CRON_CONFIG.timezone})`);
  logger('⏰ Import will run at: 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00');
  
  // 3-hourly import cron job
  const importJob = cron.schedule(CRON_CONFIG.every3Hours, async () => {
    try {
      logger('\n🚀 Starting scheduled 3-hourly import...');
      const result = await performFullImport();
      await sendNotification(result);
    } catch (error) {
      logError('💥 Scheduled import failed', error);
      await sendNotification({
        success: false,
        report: { successRate: '0%', totalRecords: 0 },
        duration: 0,
        error: error.message
      });
    }
  }, {
    scheduled: false, // Start manually
    timezone: CRON_CONFIG.timezone
  });
  
  // Start the job
  importJob.start();
  logger('✅ 3-hourly import scheduler started successfully');
  
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
  const status = {
    isScheduled: cron.getTasks().size > 0,
    nextRun: 'Every 3 hours at 00:00, 03:00, 06:00, 09:00, 12:00, 15:00, 18:00, 21:00 (Africa/Johannesburg)',
    timezone: CRON_CONFIG.timezone,
    schedule: CRON_CONFIG.every3Hours,
    logDirectory: LOG_CONFIG.logDirectory,
    currentLogFile: getCurrentLogFilePath()
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

module.exports = {
  startScheduledImports,
  stopScheduledImports,
  triggerManualImport,
  getSchedulerStatus,
  performFullImport,
  getRecentLogs,
  logger,
  logError,
  CRON_CONFIG,
  LOG_CONFIG
};