require('dotenv').config();

// Server Configuration
const SERVER_CONFIG = {
  port: process.env.EXPRESS_PORT || process.env.PORT || 3000
};

// Database Configuration
const DB_CONFIG = {
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 3306,
  user: process.env.DB_USER || 'root',
  password: process.env.DB_PASSWORD || '',
  database: process.env.DB_NAME || 'ysw_data'
};

// API Configuration
const API_CONFIG = {
  token: process.env.API_TOKEN || '',
  baseUrl: 'https://axioconnect.qcontact.com/api/v2/entities/DataExport',
  uid: process.env.API_UID || '',
  client: 'reporting'
};

// Export URLs for data retrieval
const EXPORT_URLS = {
  buildings: `${API_CONFIG.baseUrl}/5077534948/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`,
  cases: `${API_CONFIG.baseUrl}/5002645397/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`,
  conversations: `${API_CONFIG.baseUrl}/5002207692/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`,
  interactions: `${API_CONFIG.baseUrl}/5053863837/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`,
  userStateInteractions: `${API_CONFIG.baseUrl}/4693855982/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`,
  users: `${API_CONFIG.baseUrl}/5157670999/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`,
  userSessionHistory: `${API_CONFIG.baseUrl}/5219392695/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`,
  schedule: `${API_CONFIG.baseUrl}/20348692306/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`,
  slaPolicy: `${API_CONFIG.baseUrl}/20357111093/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`,
  nocInteractions: `${API_CONFIG.baseUrl}/5157703494/actions/run?csv=true&access-token=${API_CONFIG.token}&client=${API_CONFIG.client}&uid=${API_CONFIG.uid}`
};

// Enhanced HTTP Request Configuration with dataset-specific settings
const HTTP_CONFIG = {
  timeout: 120000, // Default 2 minutes
  headers: {
    'User-Agent': 'CSV-Import-Server/1.0',
    'Accept': 'text/csv,application/csv,text/plain',
    'Connection': 'keep-alive',
    'Accept-Encoding': 'gzip, deflate'
  },
  maxRedirects: 5,
  maxContentLength: Infinity,
  maxBodyLength: Infinity
};

// Enhanced CSV Parsing Configuration
const CSV_CONFIG = {
  skipEmptyLines: true,
  skipLinesWithError: true,
  parsingTimeout: 180000, // Default 3 minutes
  maxRowBytes: 1048576 // 1MB max row size
};

// Enhanced Progress Logging Configuration
const LOGGING_CONFIG = {
  progressInterval: 1000, // Default: Log progress every 1000 rows
  maxErrorLogs: 3, // Only log first 3 errors to avoid spam
  sampleDataLogs: 2 // Number of sample records to log
};

// Dataset-specific configurations for problematic imports
const DATASET_CONFIGS = {
  cases: {
    // Cases often has the most data and causes timeouts
    timeout: 300000, // 5 minutes
    parsingTimeout: 600000, // 10 minutes
    progressInterval: 100, // More frequent updates
    maxRetries: 3,
    retryDelay: 30000, // 30 seconds between retries
    description: 'Large dataset - support tickets and case data'
  },
  conversations: {
    timeout: 180000, // 3 minutes
    parsingTimeout: 300000, // 5 minutes
    progressInterval: 500,
    maxRetries: 2,
    retryDelay: 15000,
    description: 'Conversation history data'
  },
  interactions: {
    timeout: 150000, // 2.5 minutes
    parsingTimeout: 240000, // 4 minutes
    progressInterval: 500,
    maxRetries: 2,
    retryDelay: 15000,
    description: 'User interaction logs'
  },
  // Default config for other datasets
  default: {
    timeout: HTTP_CONFIG.timeout,
    parsingTimeout: CSV_CONFIG.parsingTimeout,
    progressInterval: LOGGING_CONFIG.progressInterval,
    maxRetries: 2,
    retryDelay: 10000,
    description: 'Standard dataset'
  }
};

// Function to get dataset-specific configuration
function getDatasetConfig(datasetName) {
  return DATASET_CONFIGS[datasetName] || DATASET_CONFIGS.default;
}

// Memory management configuration for large imports
const MEMORY_CONFIG = {
  // Force garbage collection after processing these row counts
  gcThresholds: {
    cases: 5000,
    conversations: 10000,
    default: 20000
  },
  // Log memory usage at these intervals
  memoryLogInterval: {
    cases: 1000,
    default: 5000
  }
};

// Import order configuration - process smaller datasets first to free up resources
const IMPORT_ORDER = [
  'buildings',      // Usually small and fast
  'users',          // User data - typically manageable size
  'schedule',       // Schedule data - usually small
  'slaPolicy',      // Policy data - small
  'userStateInteractions', // User state - medium size
  'userSessionHistory',    // Session history - medium size
  'interactions',   // Interaction logs - can be large
  'nocInteractions', // NOC interactions - medium size
  'conversations',  // Conversations - large dataset
  'cases'          // Cases last - largest and most problematic
];

// Retry configuration for different error types
const RETRY_CONFIG = {
  maxRetries: 3,
  retryDelays: [10000, 30000, 60000], // Progressive delays: 10s, 30s, 60s
  retryableErrors: [
    'ECONNABORTED',  // Timeout
    'ECONNRESET',    // Connection reset
    'ENOTFOUND',     // DNS issues
    'EAI_AGAIN',     // DNS temporary failure
    'ETIMEDOUT',     // Timeout
    '500',           // Internal server error
    '502',           // Bad gateway
    '503',           // Service unavailable
    '504'            // Gateway timeout
  ],
  nonRetryableErrors: [
    '401',           // Unauthorized
    '403',           // Forbidden
    '404',           // Not found
    '400'            // Bad request
  ]
};

module.exports = {
  SERVER_CONFIG,
  DB_CONFIG,
  API_CONFIG,
  EXPORT_URLS,
  HTTP_CONFIG,
  CSV_CONFIG,
  LOGGING_CONFIG,
  DATASET_CONFIGS,
  MEMORY_CONFIG,
  IMPORT_ORDER,
  RETRY_CONFIG,
  getDatasetConfig
};