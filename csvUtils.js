const axios = require('axios');
const csv = require('csv-parser');
const { HTTP_CONFIG, CSV_CONFIG, LOGGING_CONFIG } = require('./config');

// Enhanced configuration specifically for large datasets like cases
const ENHANCED_CONFIG = {
  cases: {
    timeout: 300000, // 5 minutes for cases
    parsingTimeout: 600000, // 10 minutes parsing timeout
    maxRetries: 3,
    retryDelay: 30000, // 30 seconds between retries
    chunkSize: 500, // Process in smaller chunks
    progressInterval: 100 // More frequent progress updates for cases
  },
  default: {
    timeout: HTTP_CONFIG.timeout,
    parsingTimeout: CSV_CONFIG.parsingTimeout,
    maxRetries: 2,
    retryDelay: 10000,
    chunkSize: 1000,
    progressInterval: LOGGING_CONFIG.progressInterval
  }
};

// Get configuration based on data type
function getConfigForDataType(dataType) {
  return ENHANCED_CONFIG[dataType] || ENHANCED_CONFIG.default;
}

// Enhanced download function with retry logic and data-type specific timeouts
async function downloadAndParseCSV(url, dataType = 'data') {
  const config = getConfigForDataType(dataType);
  let lastError;
  
  console.log(`Starting download of ${dataType} (attempt 1/${config.maxRetries + 1})`);
  
  for (let attempt = 0; attempt <= config.maxRetries; attempt++) {
    try {
      if (attempt > 0) {
        console.log(`Retrying ${dataType} download (attempt ${attempt + 1}/${config.maxRetries + 1}) after ${config.retryDelay/1000}s delay...`);
        await new Promise(resolve => setTimeout(resolve, config.retryDelay));
      }
      
      console.log(`Connecting to ${dataType} API with ${config.timeout/1000}s timeout...`);
      
      const response = await axios.get(url, {
        responseType: 'stream',
        timeout: config.timeout,
        headers: {
          ...HTTP_CONFIG.headers,
          'Connection': 'keep-alive',
          'Accept-Encoding': 'gzip, deflate'
        },
        maxRedirects: HTTP_CONFIG.maxRedirects,
        // Add specific settings for large downloads
        maxContentLength: Infinity,
        maxBodyLength: Infinity
      });
      
      console.log(`✓ Connected to ${dataType} API, starting download...`);
      
      return await parseCSVStream(response.data, dataType, config);
      
    } catch (error) {
      lastError = error;
      
      if (error.code === 'ECONNABORTED' || error.message.includes('timeout')) {
        console.error(`✗ Attempt ${attempt + 1} failed: Timeout downloading ${dataType} (${config.timeout/1000}s)`);
        
        if (attempt < config.maxRetries) {
          console.log(`Will retry with increased timeout...`);
          // Increase timeout for next attempt
          config.timeout = Math.min(config.timeout * 1.5, 600000); // Max 10 minutes
        }
        continue;
        
      } else if (error.response?.status === 504) {
        console.error(`✗ Attempt ${attempt + 1} failed: Gateway timeout for ${dataType}`);
        
        if (attempt < config.maxRetries) {
          console.log(`Retrying - gateway timeout might be temporary...`);
        }
        continue;
        
      } else if (error.response?.status >= 500) {
        console.error(`✗ Attempt ${attempt + 1} failed: Server error ${error.response.status} for ${dataType}`);
        
        if (attempt < config.maxRetries) {
          console.log(`Retrying - server error might be temporary...`);
        }
        continue;
        
      } else {
        // Non-retryable error
        console.error(`✗ Non-retryable error for ${dataType}:`, error.message);
        throw error;
      }
    }
  }
  
  // All retries exhausted
  throw new Error(`Failed to download ${dataType} after ${config.maxRetries + 1} attempts. Last error: ${lastError.message}`);
}

// Enhanced CSV parsing with chunked processing and better memory management
async function parseCSVStream(stream, dataType, config) {
  return new Promise((resolve, reject) => {
    const results = [];
    let rowCount = 0;
    let lastProgressTime = Date.now();
    const startTime = Date.now();
    
    // Add timeout for the parsing process
    const timeoutId = setTimeout(() => {
      reject(new Error(`CSV parsing timeout after ${config.parsingTimeout / 1000} seconds for ${dataType}`));
    }, config.parsingTimeout);
    
    stream
      .pipe(csv({
        skipEmptyLines: CSV_CONFIG.skipEmptyLines,
        skipLinesWithError: CSV_CONFIG.skipLinesWithError,
        // Additional options for better parsing
        mapHeaders: ({ header }) => header.trim(), // Clean headers
        maxRowBytes: 1048576 // 1MB max row size
      }))
      .on('data', (data) => {
        results.push(data);
        rowCount++;
        
        // Enhanced progress logging with time estimation
        if (rowCount % config.progressInterval === 0) {
          const now = Date.now();
          const elapsedSeconds = (now - startTime) / 1000;
          const rate = rowCount / elapsedSeconds;
          const timeSinceLastProgress = (now - lastProgressTime) / 1000;
          
          console.log(`  → ${dataType}: ${rowCount} rows processed (${rate.toFixed(1)} rows/sec, +${config.progressInterval} in ${timeSinceLastProgress.toFixed(1)}s)`);
          lastProgressTime = now;
        }
        
        // Memory management for large datasets
        if (dataType === 'cases' && rowCount % 5000 === 0) {
          // Force garbage collection hint for large datasets
          if (global.gc) {
            global.gc();
          }
        }
      })
      .on('end', () => {
        clearTimeout(timeoutId);
        const duration = (Date.now() - startTime) / 1000;
        console.log(`✓ ${dataType} CSV parsing completed: ${results.length} rows in ${duration.toFixed(2)}s (${(results.length/duration).toFixed(1)} rows/sec)`);
        resolve(results);
      })
      .on('error', (error) => {
        clearTimeout(timeoutId);
        console.error(`✗ ${dataType} CSV parsing error:`, error.message);
        reject(error);
      });
  });
}

// Enhanced API endpoint testing with better diagnostics
async function testAPIEndpoint(url, endpointName) {
  const config = getConfigForDataType(endpointName);
  
  try {
    console.log(`Testing ${endpointName} API (timeout: ${config.timeout/1000}s)...`);
    
    const startTime = Date.now();
    const response = await axios.head(url, { 
      timeout: Math.min(config.timeout / 2, 30000), // Use shorter timeout for testing
      headers: HTTP_CONFIG.headers 
    });
    
    const responseTime = Date.now() - startTime;
    
    return {
      endpoint: endpointName,
      status: response.status,
      success: true,
      responseTime: `${responseTime}ms`,
      headers: response.headers['content-type'] || 'unknown',
      recommendation: responseTime > 10000 ? 'Slow response - may need extended timeout' : 'Good response time'
    };
  } catch (error) {
    return {
      endpoint: endpointName,
      success: false,
      error: error.message,
      status: error.response?.status || 'timeout',
      recommendation: error.code === 'ECONNABORTED' ? 'Consider increasing timeout' : 'Check API availability'
    };
  }
}

// Function to test all API endpoints with enhanced reporting
async function testAllAPIEndpoints(exportUrls) {
  console.log('🔍 Testing API connectivity with enhanced diagnostics...');
  
  const tests = [];
  
  // Test each endpoint
  for (const [key, url] of Object.entries(exportUrls)) {
    const result = await testAPIEndpoint(url, key);
    tests.push(result);
    
    // Log individual results
    const status = result.success ? '✅' : '❌';
    console.log(`${status} ${key}: ${result.status} (${result.responseTime || 'failed'}) - ${result.recommendation || result.error}`);
  }
  
  const allSuccessful = tests.every(test => test.success);
  const slowEndpoints = tests.filter(test => test.success && parseInt(test.responseTime) > 10000);
  
  let suggestion = 'APIs are working normally';
  if (!allSuccessful) {
    suggestion = 'Some endpoints are failing - check API status';
  } else if (slowEndpoints.length > 0) {
    suggestion = `Slow endpoints detected: ${slowEndpoints.map(t => t.endpoint).join(', ')} - consider extended timeouts`;
  }
  
  return {
    success: allSuccessful,
    message: allSuccessful ? 'All API endpoints are reachable' : 'Some API endpoints have issues',
    tests,
    slowEndpoints: slowEndpoints.map(t => t.endpoint),
    suggestion
  };
}

// Enhanced error handling with specific suggestions
function handleCSVError(error, dataType) {
  let errorMessage = `Failed to process ${dataType} data`;
  let suggestion = 'Check server logs for details';
  let retryRecommendation = 'Try again later';
  
  if (error.message.includes('timeout')) {
    errorMessage = `Timeout processing ${dataType} data`;
    suggestion = dataType === 'cases' ? 
      'Cases dataset is large - increase timeout or try during off-peak hours' : 
      'Try again - the API might be temporarily slow';
    retryRecommendation = 'Automatic retry with increased timeout will be attempted';
    
  } else if (error.message.includes('ECONNREFUSED')) {
    errorMessage = `Cannot connect to API for ${dataType}`;
    suggestion = 'Check if the API server is running and accessible';
    
  } else if (error.message.includes('504')) {
    errorMessage = `Gateway timeout for ${dataType} endpoint`;
    suggestion = dataType === 'cases' ? 
      'Cases API is overloaded - try again in a few minutes' : 
      'API gateway timeout - try again shortly';
    retryRecommendation = 'Will retry automatically with backoff';
    
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
    suggestion,
    retryRecommendation,
    dataType
  };
}

// Enhanced processing summary with performance metrics
function logProcessingSummary(dataType, startTime, recordCount, errors = []) {
  const endTime = Date.now();
  const duration = ((endTime - startTime) / 1000).toFixed(2);
  const rate = (recordCount / parseFloat(duration)).toFixed(2);
  
  console.log(`\n=== ${dataType.toUpperCase()} PROCESSING SUMMARY ===`);
  console.log(`Duration: ${duration} seconds`);
  console.log(`Records processed: ${recordCount.toLocaleString()}`);
  console.log(`Processing rate: ${rate} records/second`);
  
  // Performance assessment
  const expectedRate = dataType === 'cases' ? 50 : 100; // Lower expectation for cases
  if (parseFloat(rate) < expectedRate) {
    console.log(`⚠️ Performance: Below expected rate of ${expectedRate} records/second`);
  } else {
    console.log(`✓ Performance: Good processing rate`);
  }
  
  if (errors.length > 0) {
    console.log(`Errors encountered: ${errors.length}`);
    console.log(`Error rate: ${((errors.length / recordCount) * 100).toFixed(2)}%`);
  } else {
    console.log(`✓ No errors - 100% success rate`);
  }
  
  console.log(`================================================\n`);
}

// Enhanced processing report with detailed metrics
function createProcessingReport(results) {
  const report = {
    timestamp: new Date().toISOString(),
    totalDataTypes: Object.keys(results).length,
    summary: {},
    overallSuccess: true,
    totalRecords: 0,
    totalErrors: 0,
    performanceMetrics: {}
  };
  
  for (const [dataType, result] of Object.entries(results)) {
    const duration = (result.duration || 0) / 1000;
    const rate = duration > 0 ? ((result.recordsProcessed || 0) / duration).toFixed(2) : '0';
    
    report.summary[dataType] = {
      success: result.success,
      recordsProcessed: result.recordsProcessed || 0,
      errors: result.errors || 0,
      duration: duration,
      processingRate: `${rate} records/sec`
    };
    
    report.performanceMetrics[dataType] = {
      duration: duration,
      rate: parseFloat(rate),
      status: result.success ? 'success' : 'failed'
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
  
  // Add overall performance assessment
  const avgRate = Object.values(report.performanceMetrics)
    .filter(m => m.status === 'success')
    .reduce((sum, m) => sum + m.rate, 0) / Object.keys(report.performanceMetrics).length;
  
  report.overallPerformance = {
    averageRate: avgRate.toFixed(2),
    assessment: avgRate > 75 ? 'Excellent' : avgRate > 50 ? 'Good' : avgRate > 25 ? 'Fair' : 'Poor'
  };
  
  return report;
}

module.exports = {
  downloadAndParseCSV,
  testAPIEndpoint,
  testAllAPIEndpoints,
  validateCSVData: require('./csvUtils').validateCSVData, // Keep original
  handleCSVError,
  logProcessingSummary,
  createProcessingReport,
  getConfigForDataType
};