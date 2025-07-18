const axios = require('axios');
const csv = require('csv-parser');
const { HTTP_CONFIG, CSV_CONFIG, LOGGING_CONFIG } = require('./config');

// Function to download and parse CSV data
async function downloadAndParseCSV(url, dataType = 'data') {
  try {
    console.log(`Starting download of ${dataType} from: ${url}`);
    
    const response = await axios.get(url, {
      responseType: 'stream',
      timeout: HTTP_CONFIG.timeout,
      headers: HTTP_CONFIG.headers,
      maxRedirects: HTTP_CONFIG.maxRedirects
    });
    
    console.log(`✓ Connected to API, starting to download ${dataType}...`);
    
    return new Promise((resolve, reject) => {
      const results = [];
      let rowCount = 0;
      
      response.data
        .pipe(csv({
          skipEmptyLines: CSV_CONFIG.skipEmptyLines,
          skipLinesWithError: CSV_CONFIG.skipLinesWithError
        }))
        .on('data', (data) => {
          results.push(data);
          rowCount++;
          
          // Log progress every specified interval
          if (rowCount % LOGGING_CONFIG.progressInterval === 0) {
            console.log(`  → Processed ${rowCount} rows...`);
          }
        })
        .on('end', () => {
          console.log(`✓ CSV parsing completed. ${results.length} rows total.`);
          resolve(results);
        })
        .on('error', (error) => {
          console.error(`✗ CSV parsing error:`, error);
          reject(error);
        });
      
      // Add timeout for the parsing process
      setTimeout(() => {
        reject(new Error(`CSV parsing timeout after ${CSV_CONFIG.parsingTimeout / 1000} seconds for ${dataType}`));
      }, CSV_CONFIG.parsingTimeout);
    });
    
  } catch (error) {
    if (error.code === 'ECONNABORTED' || error.message.includes('timeout')) {
      console.error(`✗ Timeout downloading ${dataType}. The API might be slow or the file might be very large.`);
      throw new Error(`Download timeout for ${dataType}. Try again later or check if the API is responsive.`);
    } else if (error.response) {
      console.error(`✗ API returned error ${error.response.status}: ${error.response.statusText}`);
      throw new Error(`API error ${error.response.status}: ${error.response.statusText}`);
    } else if (error.request) {
      console.error(`✗ No response from API for ${dataType}`);
      throw new Error(`No response from API. Check your internet connection and API availability.`);
    } else {
      console.error(`✗ Error downloading ${dataType}:`, error.message);
      throw error;
    }
  }
}

// Function to test API endpoint connectivity
async function testAPIEndpoint(url, endpointName) {
  try {
    console.log(`Testing ${endpointName} API...`);
    const response = await axios.head(url, { 
      timeout: 10000,
      headers: HTTP_CONFIG.headers 
    });
    
    return {
      endpoint: endpointName,
      status: response.status,
      success: true,
      headers: response.headers['content-type'] || 'unknown'
    };
  } catch (error) {
    return {
      endpoint: endpointName,
      success: false,
      error: error.message,
      status: error.response?.status || 'timeout'
    };
  }
}

// Function to test all API endpoints
async function testAllAPIEndpoints(exportUrls) {
  console.log('🔍 Testing API connectivity...');
  
  const tests = [];
  
  // Test each endpoint
  for (const [key, url] of Object.entries(exportUrls)) {
    const result = await testAPIEndpoint(url, key);
    tests.push(result);
  }
  
  const allSuccessful = tests.every(test => test.success);
  
  return {
    success: allSuccessful,
    message: allSuccessful ? 'All API endpoints are reachable' : 'Some API endpoints have issues',
    tests,
    suggestion: allSuccessful ? 'APIs are working - you can proceed with imports' : 'Check API status or try again later'
  };
}

// Function to validate CSV data structure
function validateCSVData(data, dataType, expectedColumns = []) {
  if (!Array.isArray(data) || data.length === 0) {
    throw new Error(`No ${dataType} data found or invalid data format`);
  }
  
  const firstRow = data[0];
  const actualColumns = Object.keys(firstRow);
  
  // Log available columns for debugging
  console.log(`Available columns in ${dataType}:`, actualColumns);
  
  // Check if expected columns exist (if provided)
  if (expectedColumns.length > 0) {
    const missingColumns = expectedColumns.filter(col => !actualColumns.includes(col));
    if (missingColumns.length > 0) {
      console.warn(`Warning: Missing expected columns in ${dataType}:`, missingColumns);
    }
  }
  
  return {
    isValid: true,
    rowCount: data.length,
    columns: actualColumns,
    sampleData: data.slice(0, Math.min(3, data.length))
  };
}

// Function to handle CSV parsing errors gracefully
function handleCSVError(error, dataType) {
  let errorMessage = `Failed to process ${dataType} data`;
  let suggestion = 'Check server logs for details';
  
  if (error.message.includes('timeout')) {
    errorMessage = `Timeout processing ${dataType} data`;
    suggestion = 'Try again - the API might be temporarily slow';
  } else if (error.message.includes('ECONNREFUSED')) {
    errorMessage = `Cannot connect to API for ${dataType}`;
    suggestion = 'Check if the API server is running and accessible';
  } else if (error.message.includes('404')) {
    errorMessage = `${dataType} endpoint not found`;
    suggestion = 'Check if the API endpoint URL is correct';
  } else if (error.message.includes('403') || error.message.includes('401')) {
    errorMessage = `Access denied for ${dataType} endpoint`;
    suggestion = 'Check API credentials and permissions';
  }
  
  return {
    success: false,
    message: errorMessage,
    error: error.message,
    suggestion
  };
}

// Function to log CSV processing summary
function logProcessingSummary(dataType, startTime, recordCount, errors = []) {
  const endTime = Date.now();
  const duration = ((endTime - startTime) / 1000).toFixed(2);
  
  console.log(`\n=== ${dataType.toUpperCase()} PROCESSING SUMMARY ===`);
  console.log(`Duration: ${duration} seconds`);
  console.log(`Records processed: ${recordCount}`);
  
  if (errors.length > 0) {
    console.log(`Errors encountered: ${errors.length}`);
    console.log(`Error rate: ${((errors.length / recordCount) * 100).toFixed(2)}%`);
  } else {
    console.log(`✓ No errors - 100% success rate`);
  }
  
  console.log(`Processing rate: ${(recordCount / parseFloat(duration)).toFixed(2)} records/second`);
  console.log(`================================================\n`);
}

// Function to create a processing report
function createProcessingReport(results) {
  const report = {
    timestamp: new Date().toISOString(),
    totalDataTypes: Object.keys(results).length,
    summary: {},
    overallSuccess: true,
    totalRecords: 0,
    totalErrors: 0
  };
  
  for (const [dataType, result] of Object.entries(results)) {
    report.summary[dataType] = {
      success: result.success,
      recordsProcessed: result.recordsProcessed || 0,
      errors: result.errors || 0,
      duration: result.duration || 0
    };
    
    if (!result.success) {
      report.overallSuccess = false;
    }
    
    report.totalRecords += result.recordsProcessed || 0;
    report.totalErrors += result.errors || 0;
  }
  
  report.successRate = report.totalRecords > 0 ? 
    ((report.totalRecords - report.totalErrors) / report.totalRecords * 100).toFixed(2) + '%' : 
    '0%';
  
  return report;
}

module.exports = {
  downloadAndParseCSV,
  testAPIEndpoint,
  testAllAPIEndpoints,
  validateCSVData,
  handleCSVError,
  logProcessingSummary,
  createProcessingReport
};