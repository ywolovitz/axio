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
  token: 'EiA6h979ICXCQilA3W46Vw',
  baseUrl: 'https://axioconnect.qcontact.com/api/v2/entities/DataExport',
  uid: 'yoni@queado.co.za',
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

// HTTP Request Configuration
const HTTP_CONFIG = {
  timeout: 120000, // 2 minutes
  headers: {
    'User-Agent': 'CSV-Import-Server/1.0',
    'Accept': 'text/csv,application/csv,text/plain'
  },
  maxRedirects: 5
};

// CSV Parsing Configuration
const CSV_CONFIG = {
  skipEmptyLines: true,
  skipLinesWithError: true,
  parsingTimeout: 180000 // 3 minutes
};

// Progress Logging Configuration
const LOGGING_CONFIG = {
  progressInterval: 1000, // Log progress every 1000 rows
  maxErrorLogs: 3, // Only log first 3 errors to avoid spam
  sampleDataLogs: 2 // Number of sample records to log
};

module.exports = {
  SERVER_CONFIG,
  DB_CONFIG,
  API_CONFIG,
  EXPORT_URLS,
  HTTP_CONFIG,
  CSV_CONFIG,
  LOGGING_CONFIG
};