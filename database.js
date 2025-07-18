const mysql = require('mysql2/promise');
const { DB_CONFIG } = require('./config');

let connection;

// Initialize database connection and create all tables
async function initDatabase() {
  try {
    connection = await mysql.createConnection(DB_CONFIG);
    console.log('Connected to MySQL database');
    
    // Create all tables
    await createBuildingsTable();
    await createCasesTable();
    await createConversationsTable();
    await createInteractionsTable();
    await createUserStateInteractionsTable();
    await createUsersTable();
    await createUserSessionHistoryTable();
    await createScheduleTable();
    await createSLAPolicyTable();
    
    return connection;
  } catch (error) {
    console.error('Database connection failed:', error);
    process.exit(1);
  }
}

// Get database connection
function getConnection() {
  if (!connection) {
    throw new Error('Database not initialized. Call initDatabase() first.');
  }
  return connection;
}

// Buildings table creation
async function createBuildingsTable() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS Buildings (
      id INT AUTO_INCREMENT PRIMARY KEY,
      unit_count INT,
      building_name VARCHAR(255),
      deployment_method VARCHAR(255),
      portfolio_id BIGINT,
      portfolio VARCHAR(255),
      label VARCHAR(255),
      building_manager VARCHAR(255),
      building_manager_details VARCHAR(255),
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `;
  
  try {
    await connection.execute(createTableQuery);
    console.log('Buildings table created or already exists');
  } catch (error) {
    console.error('Error creating Buildings table:', error);
    throw error;
  }
}

// Cases table creation
async function createCasesTable() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS Cases (
      id INT AUTO_INCREMENT PRIMARY KEY,
      logged_via VARCHAR(255),
      type VARCHAR(255),
      time_logged DECIMAL(15,2),
      name VARCHAR(255),
      status VARCHAR(255),
      reference VARCHAR(255),
      portfolio VARCHAR(255),
      tag VARCHAR(255),
      department_name VARCHAR(255),
      category VARCHAR(255),
      contact_first_name VARCHAR(255),
      contact_last_name VARCHAR(255),
      contact_building_name VARCHAR(255),
      contact_portfolio VARCHAR(255),
      contact_mobile VARCHAR(255),
      contact_email VARCHAR(255),
      contact_unit_details TEXT,
      department_id DECIMAL(15,2),
      case_id BIGINT,
      created_at DATETIME,
      building_name VARCHAR(255),
      building_portfolio VARCHAR(255),
      building_contact_type VARCHAR(255),
      remote_site VARCHAR(255),
      assigned_to BIGINT,
      building_deployment_methodology VARCHAR(255),
      closed_at DATETIME,
      building_unit_details DECIMAL(15,2),
      building_id BIGINT,
      one_touch VARCHAR(255),
      first_reply DATETIME,
      first_solved_at DATETIME,
      solved_at DATETIME,
      sla_policy_id BIGINT,
      imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `;
  
  try {
    await connection.execute(createTableQuery);
    console.log('Cases table created or already exists');
  } catch (error) {
    console.error('Error creating Cases table:', error);
    throw error;
  }
}

// Conversations table creation
async function createConversationsTable() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS Conversations (
      id BIGINT PRIMARY KEY,
      start_at DATETIME,
      ended_at DATETIME,
      completed DATETIME,
      contact_method VARCHAR(255),
      inbound VARCHAR(255),
      channel_types VARCHAR(255),
      label VARCHAR(255),
      created_at VARCHAR(255),
      updated_at VARCHAR(255),
      contact_building_name VARCHAR(255),
      contact_portfolio VARCHAR(255),
      imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      table_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `;
  
  try {
    await connection.execute(createTableQuery);
    console.log('Conversations table created or already exists');
  } catch (error) {
    console.error('Error creating Conversations table:', error);
    throw error;
  }
}

// Interactions table creation
async function createInteractionsTable() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS Interactions (
      id INT AUTO_INCREMENT PRIMARY KEY,
      user_id BIGINT,
      answered VARCHAR(255),
      channel_type VARCHAR(255),
      clicked_at DATETIME,
      completed VARCHAR(255),
      consulation VARCHAR(255),
      created_at DATETIME,
      delivered_time DATETIME,
      duration DECIMAL(15,2),
      ended DATETIME,
      from_name VARCHAR(255),
      from_handle VARCHAR(255),
      time_field DATETIME,
      interaction_id BIGINT,
      interaction_direction VARCHAR(255),
      interaction_type VARCHAR(255),
      label VARCHAR(255),
      participant_name VARCHAR(255),
      read_time DATETIME,
      recording VARCHAR(255),
      state VARCHAR(255),
      reference VARCHAR(255),
      sent_time DATETIME,
      started DATETIME,
      status VARCHAR(255),
      subject TEXT,
      time_to_reply DECIMAL(15,2),
      time_to_reply_tracked DECIMAL(15,2),
      transfer_type VARCHAR(255),
      updated_at DATETIME,
      user_last_name VARCHAR(255),
      user_first_name VARCHAR(255),
      wait_for_agent DECIMAL(15,2),
      wait_for_customer DECIMAL(15,2),
      wait_for_customer_2 DECIMAL(15,2),
      imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      table_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `;
  
  try {
    await connection.execute(createTableQuery);
    console.log('Interactions table created or already exists');
  } catch (error) {
    console.error('Error creating Interactions table:', error);
    throw error;
  }
}

// User State Interactions table creation
async function createUserStateInteractionsTable() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS UserStateInteractions (
      id BIGINT PRIMARY KEY,
      start_at DATETIME,
      user_status VARCHAR(255),
      acd_status VARCHAR(255),
      pause_reason VARCHAR(255),
      user_first_name VARCHAR(255),
      user_last_name VARCHAR(255),
      user_job_title VARCHAR(255),
      acd_available VARCHAR(255),
      user_pause_acd_when_voice_fails VARCHAR(255),
      user_pause_acd_when_voice_fails_2 VARCHAR(255),
      user_not_responding_auto_recovery VARCHAR(255),
      user_not_responding_auto_recovery_timeout DECIMAL(15,2),
      user_ice_timeout DECIMAL(15,2),
      user_force_turn VARCHAR(255),
      user_force_turn_tcp VARCHAR(255),
      created_at DATETIME,
      updated_at DATETIME,
      conversation_id BIGINT,
      conversation_label VARCHAR(255),
      conversation_on_hold VARCHAR(255),
      conversation_completed VARCHAR(255),
      conversation_ended_at DATETIME,
      conversation_answer_at DATETIME,
      conversation_start_at DATETIME,
      user_id BIGINT,
      user_label VARCHAR(255),
      duration DECIMAL(15,2),
      imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      table_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `;
  
  try {
    await connection.execute(createTableQuery);
    console.log('UserStateInteractions table created or already exists');
  } catch (error) {
    console.error('Error creating UserStateInteractions table:', error);
    throw error;
  }
}

// Users table creation
async function createUsersTable() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS Users (
      id BIGINT PRIMARY KEY,
      first_name VARCHAR(255),
      last_name VARCHAR(255),
      email VARCHAR(255),
      job_title VARCHAR(255),
      allow_report_access DECIMAL(15,2),
      allow_supervisor_access DECIMAL(15,2),
      allow_messaging_access DECIMAL(15,2),
      allow_settings_access DECIMAL(15,2),
      user_type VARCHAR(255),
      pbx_user BOOLEAN,
      enabled BOOLEAN,
      label VARCHAR(255),
      imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `;
  
  try {
    await connection.execute(createTableQuery);
    console.log('Users table created or already exists');
  } catch (error) {
    console.error('Error creating Users table:', error);
    throw error;
  }
}

// User Session History table creation
async function createUserSessionHistoryTable() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS UserSessionHistory (
      id BIGINT PRIMARY KEY,
      user_id BIGINT,
      label VARCHAR(255),
      user_label VARCHAR(255),
      updated_at DATETIME,
      created_at DATETIME,
      user_last_sign_in_at DATETIME,
      imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      table_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `;
  
  try {
    await connection.execute(createTableQuery);
    console.log('UserSessionHistory table created or already exists');
  } catch (error) {
    console.error('Error creating UserSessionHistory table:', error);
    throw error;
  }
}

// Schedule table creation
async function createScheduleTable() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS Schedule (
      id BIGINT PRIMARY KEY,
      closed_time_slots TEXT,
      label VARCHAR(255),
      name VARCHAR(255),
      opening_time_slots TEXT,
      imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `;
  
  try {
    await connection.execute(createTableQuery);
    console.log('Schedule table created or already exists');
  } catch (error) {
    console.error('Error creating Schedule table:', error);
    throw error;
  }
}

// SLA Policy table creation
async function createSLAPolicyTable() {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS SLAPolicy (
      id BIGINT PRIMARY KEY,
      enabled BOOLEAN,
      escalation_assigned_to DECIMAL(15,2),
      escalation_assigned_to_id DECIMAL(15,2),
      escalation_assigned_to_label VARCHAR(255),
      escalation_rules TEXT,
      escalation_rules_use_schedule BOOLEAN,
      first_reply_target INT,
      label VARCHAR(255),
      last_reply_target DECIMAL(15,2),
      name VARCHAR(255),
      reset_targets_on_assignment BOOLEAN,
      solved_target DECIMAL(15,2),
      imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
  `;
  
  try {
    await connection.execute(createTableQuery);
    console.log('SLAPolicy table created or already exists');
  } catch (error) {
    console.error('Error creating SLAPolicy table:', error);
    throw error;
  }
}

// Graceful database closure
async function closeDatabase() {
  if (connection) {
    await connection.end();
    console.log('Database connection closed');
  }
}

module.exports = {
  initDatabase,
  getConnection,
  closeDatabase,
  createBuildingsTable,
  createCasesTable,
  createConversationsTable,
  createInteractionsTable,
  createUserStateInteractionsTable,
  createUsersTable,
  createUserSessionHistoryTable,
  createScheduleTable,
  createSLAPolicyTable
};