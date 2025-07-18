const cron = require('node-cron');
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

// Cron job configuration
const CRON_CONFIG = {
  // Run daily at 2:00 AM
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
  
  console.log('\n🤖 === AUTOMATED DAILY IMPORT STARTED ===');
  console.log(`⏰ Started at: ${new Date().toLocaleString()}`);
  
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
      console.log(`${task.emoji} Phase ${i + 1}/10: Importing ${task.name}...`);
      const data = await downloadAndParseCSV(task.url, task.name);
      await task.handler(data);
      
      const taskDuration = Date.now() - taskStartTime;
      results[task.name] = {
        success: true,
        recordsProcessed: data.length,
        duration: taskDuration
      };
      
      logProcessingSummary(task.name, taskStartTime, data.length);
      console.log(`✅ ${task.emoji} ${task.name} completed successfully`);
      
    } catch (error) {
      console.error(`❌ ${task.emoji} Failed to import ${task.name}:`, error.message);
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
  console.log('\n🎉 === AUTOMATED DAILY IMPORT COMPLETED ===');
  console.log(`⏰ Completed at: ${new Date().toLocaleString()}`);
  console.log(`⏱️ Total duration: ${(totalDuration / 1000 / 60).toFixed(2)} minutes`);
  console.log(`📊 Overall success rate: ${report.successRate}`);
  console.log(`📈 Total records processed: ${report.totalRecords}`);
  
  // Log any failures
  const failures = Object.entries(results).filter(([_, result]) => !result.success);
  if (failures.length > 0) {
    console.log(`⚠️ Failed imports: ${failures.map(([name]) => name).join(', ')}`);
  }
  
  console.log('================================================\n');
  
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
    ? `✅ Daily import completed successfully in ${(duration / 1000 / 60).toFixed(2)} minutes. ${report.totalRecords} records processed.`
    : `❌ Daily import completed with errors. Success rate: ${report.successRate}. Check logs for details.`;
  
  console.log(`📢 NOTIFICATION: ${message}`);
  
  // TODO: Add your notification logic here
  // Examples:
  // - Send email via nodemailer
  // - Post to Slack webhook
  // - Send to monitoring service
  // - Write to external log service
}

// Schedule the daily import job
function startScheduledImports() {
  console.log('📅 Setting up scheduled data imports...');
  console.log(`🕐 Daily import scheduled for: ${CRON_CONFIG.dailyImport} (${CRON_CONFIG.timezone})`);
  
  // Daily import cron job
  const dailyImportJob = cron.schedule(CRON_CONFIG.dailyImport, async () => {
    try {
      console.log('\n🚀 Starting scheduled daily import...');
      const result = await performFullImport();
      await sendNotification(result);
    } catch (error) {
      console.error('💥 Scheduled import failed:', error);
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
  dailyImportJob.start();
  console.log('✅ Daily import scheduler started successfully');
  
  return dailyImportJob;
}

// Function to stop scheduled imports
function stopScheduledImports(job) {
  if (job) {
    job.destroy();
    console.log('🛑 Scheduled imports stopped');
  }
}

// Manual trigger function (for testing)
async function triggerManualImport() {
  console.log('🔧 Manual import triggered...');
  try {
    const result = await performFullImport();
    await sendNotification(result);
    return result;
  } catch (error) {
    console.error('💥 Manual import failed:', error);
    throw error;
  }
}

// Health check for scheduler
function getSchedulerStatus() {
  return {
    isScheduled: cron.getTasks().size > 0,
    nextRun: 'Daily at 2:00 AM (Africa/Johannesburg)',
    timezone: CRON_CONFIG.timezone,
    schedule: CRON_CONFIG.dailyImport
  };
}

module.exports = {
  startScheduledImports,
  stopScheduledImports,
  triggerManualImport,
  getSchedulerStatus,
  performFullImport,
  CRON_CONFIG
};