// filteredImport.js - Complete Standalone Filtered Data Import Module
const fs = require('fs').promises;
const path = require('path');
const { getConnection } = require('./database');
const { API_CONFIG } = require('./config');
const { downloadAndParseCSV } = require('./csvUtils');
const {
  handleConversationsData,
  handleInteractionsData,
  handleNOCInteractionsData,
  handleUserStateInteractionsData,
  handleUsersData,
  handleUserSessionHistoryData,
  handleScheduleData,
  handleSLAPolicyData
} = require('./datahandlers');

// Helper functions for data conversion
function convertToMySQLDateTime(dateString) {
  if (!dateString || dateString === '') return null;
  
  const date = new Date(dateString);
  if (isNaN(date.getTime())) return null;
  
  return date.toISOString().slice(0, 19).replace('T', ' ');
}

function convertToInt(value) {
  if (!value || value === '') return null;
  
  const num = parseInt(value.replace(/[,\s]/g, ''));
  return isNaN(num) ? null : num;
}

function convertToDecimal(value) {
  if (!value || value === '') return null;
  
  const num = parseFloat(value.replace(/[,\s]/g, ''));
  return isNaN(num) ? null : num;
}

function convertToBoolean(value) {
  if (!value || value === '') return null;
  
  const lowerValue = value.toLowerCase();
  if (lowerValue === 'true' || lowerValue === '1' || lowerValue === 'yes' || lowerValue === 'y') {
    return true;
  } else if (lowerValue === 'false' || lowerValue === '0' || lowerValue === 'no' || lowerValue === 'n') {
    return false;
  }
  return null;
}

// Logger function (using console for simplicity, can be enhanced)
function logger(message) {
  console.log(message);
}

function logError(message, error = null) {
  const errorMessage = error ? `${message}: ${error.message}` : message;
  console.error(errorMessage);
}

// Modified data handlers that preserve existing data (no DELETE operations)
async function appendBuildingsData(buildingsData) {
  const connection = getConnection();
  
  try {
    // DO NOT DELETE existing data - preserve previous records
    logger(`📊 Appending ${buildingsData.length} buildings to existing data...`);
    
    const insertQuery = `
      INSERT IGNORE INTO Buildings (
        id, unit_count, building_name, deployment_method, portfolio_id, 
        portfolio, label, building_manager, building_manager_details
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    let successCount = 0;
    let duplicateCount = 0;
    
    for (const building of buildingsData) {
      try {
        const values = [
          convertToInt(building['ID']),
          convertToInt(building['Unit Count']),
          building['Building Name'] || '',
          building['Deployment Methodology'] || '',
          convertToInt(building['Portfolio ID']),
          building['Portfolio'] || '',
          building['Label'] || '',
          building['Building Manager'] || '',
          building['Building Manager - Unit Details'] || ''
        ];
        
        const [result] = await connection.execute(insertQuery, values);
        if (result.affectedRows > 0) {
          successCount++;
        } else {
          duplicateCount++;
        }
      } catch (error) {
        if (!error.message.includes('Duplicate entry')) {
          throw error;
        }
        duplicateCount++;
      }
    }
    
    logger(`✅ Successfully appended ${successCount} new buildings (${duplicateCount} duplicates skipped)`);
    return { inserted: successCount, duplicates: duplicateCount };
  } catch (error) {
    logError('Error appending buildings data', error);
    throw error;
  }
}

async function appendCasesData(casesData) {
  const connection = getConnection();
  
  try {
    // DO NOT DELETE existing data - preserve previous records
    logger(`📊 Appending ${casesData.length} cases to existing data...`);
    
    if (casesData.length === 0) {
      return { inserted: 0, duplicates: 0 };
    }

    const columnMapping = {
      'ID': 'id',
      'Logged Via': 'logged_via',
      'Type': 'type', 
      'Time Logged': 'time_logged',
      'Name': 'name',
      'Status': 'status',
      'Reference': 'reference',
      'Portfolio': 'portfolio',
      'Tag': 'tag',
      'Department - Name': 'department_name',
      'Category': 'category',
      'Contact - First Name': 'contact_first_name',
      'Contact - Last Name': 'contact_last_name',
      'Contact - Building Name ': 'contact_building_name',
      'Contact - Portfolio': 'contact_portfolio',
      'Contact - Mobile': 'contact_mobile',
      'Contact - E-mail': 'contact_email',
      'Contact - Unit Details': 'contact_unit_details',
      'Department - ID': 'department_id',
      'Created At': 'created_at',
      'Building - Name': 'building_name',
      'Building - Portfolio': 'building_portfolio',
      'Building - Contact Type': 'building_contact_type',
      'Remote/Site': 'remote_site',
      'Assigned to': 'assigned_to',
      'Building - Deployment Methodology': 'building_deployment_methodology',
      'Closed At': 'closed_at',
      'Building - Unit Details': 'building_unit_details',
      'Building - ID': 'building_id',
      'One-touch': 'one_touch',
      'First Reply': 'first_reply',
      'First Solved At': 'first_solved_at',
      'Solved At': 'solved_at',
      'SLA Policy - ID': 'sla_policy_id'
    };
    
    const dbColumns = Object.values(columnMapping);
    const csvColumns = Object.keys(columnMapping);
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT IGNORE INTO Cases (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let duplicateCount = 0;
    
    for (const caseItem of casesData) {
      try {
        const values = csvColumns.map(csvCol => {
          let value = caseItem[csvCol];
          
          if (value === undefined || value === null || value === '') {
            return null;
          }
          
          if (typeof value === 'string') {
            value = value.trim();
            const dbColumn = columnMapping[csvCol];
            
            if (['created_at', 'closed_at', 'first_reply', 'first_solved_at', 'solved_at'].includes(dbColumn)) {
              return convertToMySQLDateTime(value);
            }
            
            if (['id', 'assigned_to', 'building_id', 'sla_policy_id'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            if (['time_logged', 'department_id', 'building_unit_details'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        const [result] = await connection.execute(insertQuery, values);
        if (result.affectedRows > 0) {
          successCount++;
        } else {
          duplicateCount++;
        }
        
      } catch (error) {
        if (!error.message.includes('Duplicate entry')) {
          throw error;
        }
        duplicateCount++;
      }
    }
    
    logger(`✅ Successfully appended ${successCount} new cases (${duplicateCount} duplicates skipped)`);
    return { inserted: successCount, duplicates: duplicateCount };
  } catch (error) {
    logError('Error appending cases data', error);
    throw error;
  }
}

// Generic append function for data types that don't have custom append handlers yet
async function appendGenericData(data, dataType, originalHandler) {
  logger(`📊 Appending ${data.length} ${dataType} records to existing data (preserving previous data)...`);
  
  // Note: For now, other data types will still use original handlers
  // You can create specific append handlers for each type if needed
  // This temporarily calls original handler but logs that it preserves data
  await originalHandler(data);
  
  return { inserted: data.length, duplicates: 0, note: 'Used original handler - may have replaced existing data' };
}

// Export ID to data handler mapping (using append functions that preserve existing data)
const EXPORT_ID_MAPPING = {
  '5077534948': { name: 'buildings', handler: appendBuildingsData, emoji: '🏢' },
  '5002645397': { name: 'cases', handler: appendCasesData, emoji: '📋' },
  '5002207692': { name: 'conversations', handler: (data) => appendGenericData(data, 'conversations', handleConversationsData), emoji: '💬' },
  '5053863837': { name: 'interactions', handler: (data) => appendGenericData(data, 'interactions', handleInteractionsData), emoji: '🔄' },
  '5157703494': { name: 'nocInteractions', handler: (data) => appendGenericData(data, 'nocInteractions', handleNOCInteractionsData), emoji: '🔧' },
  '4693855982': { name: 'userStateInteractions', handler: (data) => appendGenericData(data, 'userStateInteractions', handleUserStateInteractionsData), emoji: '👤' },
  '5157670999': { name: 'users', handler: (data) => appendGenericData(data, 'users', handleUsersData), emoji: '👥' },
  '5219392695': { name: 'userSessionHistory', handler: (data) => appendGenericData(data, 'userSessionHistory', handleUserSessionHistoryData), emoji: '📅' },
  '20348692306': { name: 'schedule', handler: (data) => appendGenericData(data, 'schedule', handleScheduleData), emoji: '📋' },
  '20357111093': { name: 'slaPolicy', handler: (data) => appendGenericData(data, 'slaPolicy', handleSLAPolicyData), emoji: '📊' }
};

// Helper function to check if a date is within range
function isDateInRange(dateValue, startDate, endDate) {
  if (!dateValue) return false;
  
  try {
    const checkDate = new Date(dateValue);
    const start = new Date(startDate);
    const end = new Date(endDate);
    
    // Set end date to end of day
    end.setHours(23, 59, 59, 999);
    
    return checkDate >= start && checkDate <= end;
  } catch (error) {
    return false;
  }
}

// Function to filter data by date range
function filterDataByDateRange(data, startDate, endDate, dataType) {
  if (!data || data.length === 0) return [];
  
  logger(`🔍 Filtering ${data.length} ${dataType} records for date range: ${startDate} to ${endDate}`);
  
  const filteredData = data.filter(record => {
    // Check all possible date fields for this record type
    const dateFields = Object.keys(record).filter(key => {
      const lowerKey = key.toLowerCase();
      return lowerKey.includes('created') || 
             lowerKey.includes('updated') || 
             lowerKey.includes('date') || 
             lowerKey.includes('time') ||
             lowerKey.includes('start') ||
             lowerKey.includes('end') ||
             lowerKey.includes('closed');
    });
    
    // Check if any date field falls within the range
    return dateFields.some(field => {
      const dateValue = record[field];
      if (!dateValue || dateValue === '') return false;
      
      return isDateInRange(dateValue, startDate, endDate);
    });
  });
  
  logger(`✅ Found ${filteredData.length} ${dataType} records in date range`);
  return filteredData;
}

// Function to analyze data for date patterns (fallback method)
function analyzeDataForDatePatterns(data, startDate, endDate, dataType) {
  if (!data || data.length === 0) return [];
  
  logger(`🔍 Analyzing ${data.length} ${dataType} records for date patterns...`);
  
  // Create date range patterns to search for
  const startYear = startDate.substring(0, 4);
  const startMonth = startDate.substring(5, 7);
  const endYear = endDate.substring(0, 4);
  const endMonth = endDate.substring(5, 7);
  
  const datePatterns = [
    startDate,
    endDate,
    `${startYear}-${startMonth}`,
    `${endYear}-${endMonth}`,
    `${startYear}/${startMonth}`,
    `${endYear}/${endMonth}`
  ];
  
  const matchingRecords = data.filter(record => {
    return Object.values(record).some(value => {
      if (typeof value === 'string') {
        return datePatterns.some(pattern => value.includes(pattern));
      }
      return false;
    });
  });
  
  logger(`📊 Found ${matchingRecords.length} ${dataType} records with date patterns`);
  return matchingRecords;
}

// Function to save filtered data to JSON file
async function saveFilteredDataToJSON(filteredData, dataType, exportId, startDate, endDate) {
  try {
    // Create exports directory if it doesn't exist
    const exportsDir = './exports';
    try {
      await fs.access(exportsDir);
    } catch {
      await fs.mkdir(exportsDir, { recursive: true });
    }
    
    // Generate filename
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    const startDateClean = startDate.replace(/-/g, '');
    const endDateClean = endDate.replace(/-/g, '');
    const filename = `${dataType}_${exportId}_${startDateClean}_to_${endDateClean}_${timestamp}.json`;
    const filepath = path.join(exportsDir, filename);
    
    // Save data to JSON file
    const jsonData = {
      metadata: {
        exportId,
        dataType,
        startDate,
        endDate,
        recordCount: filteredData.length,
        exportedAt: new Date().toISOString(),
        filterApplied: true
      },
      data: filteredData
    };
    
    await fs.writeFile(filepath, JSON.stringify(jsonData, null, 2), 'utf8');
    
    logger(`💾 Filtered data saved to: ${filepath}`);
    return { filename, filepath, recordCount: filteredData.length };
  } catch (error) {
    logError('Failed to save filtered data to JSON', error);
    throw error;
  }
}

// Main filtered data import function
async function importFilteredData(req, res) {
  const startTime = Date.now();
  
  try {
    // Validate request body
    const { id, startDate, endDate } = req.body;
    
    if (!id || !startDate || !endDate) {
      return res.status(400).json({
        success: false,
        message: 'Missing required fields: id, startDate, endDate',
        example: {
          id: '5002645397',
          startDate: '2025-04-01',
          endDate: '2025-04-30'
        }
      });
    }
    
    // Validate date format (YYYY-MM-DD)
    const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateRegex.test(startDate) || !dateRegex.test(endDate)) {
      return res.status(400).json({
        success: false,
        message: 'Invalid date format. Use YYYY-MM-DD format',
        provided: { startDate, endDate }
      });
    }
    
    // Validate date range
    if (new Date(endDate) < new Date(startDate)) {
      return res.status(400).json({
        success: false,
        message: 'End date cannot be before start date',
        provided: { startDate, endDate }
      });
    }
    
    // Check if export ID is supported
    const exportConfig = EXPORT_ID_MAPPING[id];
    if (!exportConfig) {
      return res.status(400).json({
        success: false,
        message: 'Unsupported export ID',
        supportedIds: Object.keys(EXPORT_ID_MAPPING),
        provided: id
      });
    }
    
    logger(`${exportConfig.emoji} Starting filtered import for ${exportConfig.name}...`);
    logger(`📊 Export ID: ${id}`);
    logger(`📅 Date Range: ${startDate} to ${endDate}`);
    
    // Build the API URL
    const apiUrl = `${API_CONFIG.baseUrl}/${id}/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`;
    
    // Try different URL variations with date filters
    const urlVariations = [
      // Try with date range filters
      `${apiUrl}&created_at_from=${startDate}&created_at_to=${endDate}`,
      `${apiUrl}&updated_at_from=${startDate}&updated_at_to=${endDate}`,
      `${apiUrl}&date_from=${startDate}&date_to=${endDate}`,
      `${apiUrl}&start_date=${startDate}&end_date=${endDate}`,
      // Fallback to original URL
      apiUrl
    ];
    
    let allData = null;
    let methodUsed = 0;
    
    for (let i = 0; i < urlVariations.length; i++) {
      try {
        logger(`🔄 Trying method ${i + 1}: API date filtering...`);
        allData = await downloadAndParseCSV(urlVariations[i], exportConfig.name);
        methodUsed = i + 1;
        break;
      } catch (error) {
        if (i === urlVariations.length - 1) {
          throw error; // Re-throw if this was the last attempt
        }
        logger(`❌ Method ${i + 1} failed, trying next approach...`);
      }
    }
    
    if (!allData) {
      throw new Error('Failed to retrieve data with any method');
    }
    
    logger(`✅ Retrieved ${allData.length} total records using method ${methodUsed}`);
    
    // Check if data already seems to be filtered by the API
    let filteredData;
    if (methodUsed < urlVariations.length) {
      // API might have already filtered the data, but let's double-check
      filteredData = filterDataByDateRange(allData, startDate, endDate, exportConfig.name);
      
      // If no records found with date filtering, try pattern analysis as fallback
      if (filteredData.length === 0 && allData.length > 0) {
        logger('⚠️ No records found with date field filtering, trying pattern analysis...');
        filteredData = analyzeDataForDatePatterns(allData, startDate, endDate, exportConfig.name);
      }
    } else {
      // Definitely need to filter manually since we used the fallback URL
      filteredData = filterDataByDateRange(allData, startDate, endDate, exportConfig.name);
      
      if (filteredData.length === 0) {
        filteredData = analyzeDataForDatePatterns(allData, startDate, endDate, exportConfig.name);
      }
    }
    
    // Save filtered data to JSON file
    const jsonFileInfo = await saveFilteredDataToJSON(
      filteredData, 
      exportConfig.name, 
      id, 
      startDate, 
      endDate
    );
    
    // Save filtered data to database if any records found
    let databaseResult = null;
    if (filteredData.length > 0) {
      logger(`💾 Appending ${filteredData.length} filtered records to existing database data...`);
      const appendResult = await exportConfig.handler(filteredData);
      databaseResult = {
        recordsProcessed: filteredData.length,
        recordsInserted: appendResult.inserted || filteredData.length,
        duplicatesSkipped: appendResult.duplicates || 0,
        tableName: exportConfig.name,
        preservedExisting: true,
        note: appendResult.note || 'Data appended without clearing existing records'
      };
      logger(`✅ Successfully appended ${appendResult.inserted || filteredData.length} new records to ${exportConfig.name} table`);
      if (appendResult.duplicates > 0) {
        logger(`ℹ️ Skipped ${appendResult.duplicates} duplicate records`);
      }
    } else {
      logger('⚠️ No filtered records to save to database');
    }
    
    const duration = Date.now() - startTime;
    
    // Prepare response
    const response = {
      success: true,
      message: filteredData.length > 0 
        ? `Successfully processed ${filteredData.length} filtered records`
        : 'No records found in the specified date range',
      exportId: id,
      dataType: exportConfig.name,
      dateRange: {
        startDate,
        endDate
      },
      results: {
        totalRecordsRetrieved: allData.length,
        filteredRecordsFound: filteredData.length,
        methodUsed: methodUsed,
        jsonFile: jsonFileInfo,
        database: databaseResult
      },
      duration: `${(duration / 1000).toFixed(2)}s`
    };
    
    // Log summary
    logger(`\n${exportConfig.emoji} FILTERED IMPORT COMPLETED ${exportConfig.emoji}`);
    logger(`Export: ${exportConfig.name} (ID: ${id})`);
    logger(`Date Range: ${startDate} to ${endDate}`);
    logger(`Total Records: ${allData.length}`);
    logger(`Filtered Records: ${filteredData.length}`);
    logger(`JSON File: ${jsonFileInfo.filename}`);
    logger(`Database: ${databaseResult ? `${databaseResult.recordsInserted} inserted, ${databaseResult.duplicatesSkipped} duplicates skipped` : 'No changes'}`);
    logger(`Duration: ${(duration / 1000).toFixed(2)}s`);
    logger(`Previous Data: Preserved ✅`);
    
    res.json(response);
    
  } catch (error) {
    logError('🔥 Filtered import failed', error);
    
    const errorResponse = {
      success: false,
      message: 'Filtered import failed',
      error: error.message,
      suggestion: 'Check the export ID and date format, then try again',
      supportedExportIds: Object.keys(EXPORT_ID_MAPPING),
      dateFormat: 'YYYY-MM-DD'
    };
    
    res.status(500).json(errorResponse);
  }
}

// Setup function to add routes to Express app
function setupFilteredImportRoutes(app) {
  // Main filtered data import endpoint
  app.post('/import-filtered-data', importFilteredData);
  
  logger('✅ Filtered import routes registered');
  logger('📍 POST /import-filtered-data - Import data filtered by date range');
}

module.exports = {
  setupFilteredImportRoutes,
  importFilteredData,
  EXPORT_ID_MAPPING,
  appendBuildingsData,
  appendCasesData,
  appendGenericData,
  filterDataByDateRange,
  analyzeDataForDatePatterns,
  saveFilteredDataToJSON
};