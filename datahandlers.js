const { getConnection } = require('./database');
const { LOGGING_CONFIG } = require('./config');

// Enhanced helper function to convert date strings to MySQL datetime format
function convertToMySQLDateTime(dateString) {
  if (!dateString || dateString === '') return null;
  
  try {
    let date = new Date(dateString);
    
    if (isNaN(date.getTime())) {
      const cleanedDate = dateString.replace(/[^\d\s:/-]/g, '');
      date = new Date(cleanedDate);
    }
    
    if (isNaN(date.getTime())) {
      console.warn(`Could not parse date: ${dateString}`);
      return null;
    }
    
    return date.toISOString().slice(0, 19).replace('T', ' ');
  } catch (error) {
    console.warn(`Error parsing date "${dateString}":`, error.message);
    return null;
  }
}

// Enhanced helper function to convert string to integer
function convertToInt(value) {
  if (!value || value === '') return null;
  
  try {
    const cleanedValue = value.toString().replace(/[^\d-]/g, '');
    const num = parseInt(cleanedValue);
    return isNaN(num) ? null : num;
  } catch (error) {
    console.warn(`Error parsing integer "${value}":`, error.message);
    return null;
  }
}

// Enhanced helper function to convert string to decimal
function convertToDecimal(value) {
  if (!value || value === '') return null;
  
  try {
    const cleanedValue = value.toString().replace(/[^\d.-]/g, '');
    const num = parseFloat(cleanedValue);
    return isNaN(num) ? null : num;
  } catch (error) {
    console.warn(`Error parsing decimal "${value}":`, error.message);
    return null;
  }
}

// Enhanced helper function to convert string to boolean
function convertToBoolean(value) {
  if (!value || value === '') return null;
  
  const lowerValue = value.toString().toLowerCase();
  if (lowerValue === 'true' || lowerValue === '1' || lowerValue === 'yes' || lowerValue === 'y') {
    return true;
  } else if (lowerValue === 'false' || lowerValue === '0' || lowerValue === 'no' || lowerValue === 'n') {
    return false;
  }
  return null;
}

// FIXED: Buildings data handler - Preserves existing data
async function saveBuildingsData(buildingsData) {
  const connection = getConnection();
  
  try {
    console.log('=== BUILDINGS DATA ===');
    console.log(`Total buildings: ${buildingsData.length}`);
    
    if (buildingsData.length === 0) {
      console.log('No buildings data to process');
      return;
    }

    // DO NOT DELETE - Preserve existing data
    console.log('Preserving existing buildings data...');
    
    const insertQuery = `
      INSERT IGNORE INTO Buildings (
        id, unit_count, building_name, deployment_method, portfolio_id, 
        portfolio, label, building_manager, building_manager_details
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    
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
        
        const result = await connection.execute(insertQuery, values);
        
        if (result[0].affectedRows > 0) {
          successCount++;
        } else {
          skippedCount++;
        }
      } catch (error) {
        errorCount++;
        if (errorCount <= 3) {
          console.error(`Error inserting building:`, error.message);
        }
      }
    }
    
    // Get total count in database
    const [countResult] = await connection.execute('SELECT COUNT(*) as total FROM Buildings');
    const totalInDB = countResult[0].total;
    
    console.log(`✅ Successfully inserted ${successCount} NEW buildings`);
    if (skippedCount > 0) console.log(`↺ Skipped ${skippedCount} duplicate buildings`);
    if (errorCount > 0) console.log(`❌ Failed to process ${errorCount} buildings`);
    console.log(`📊 Total buildings now in database: ${totalInDB.toLocaleString()}`);
    console.log('===================');
    
    return {
      success: errorCount === 0,
      successCount,
      skippedCount,
      errorCount,
      totalInDB: totalInDB
    };
  } catch (error) {
    console.error('Error saving buildings data:', error);
    throw error;
  }
}

// FIXED: Cases data handler - Preserves existing data
async function handleCasesData(casesData) {
  const connection = getConnection();
  
  try {
    console.log('=== CASES DATA ===');
    console.log(`Total cases: ${casesData.length}`);
    
    if (casesData.length === 0) {
      console.log('No cases data to process');
      return;
    }

    console.log('Sample cases data:');
    casesData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((caseItem, index) => {
      console.log(`Case ${index + 1}:`, JSON.stringify(caseItem, null, 2));
    });
    
    // DO NOT DELETE - Preserve existing data
    console.log('Preserving existing cases data...');
    
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
    
    console.log(`Processing cases with data preservation...`);
    
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    
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
        
        const result = await connection.execute(insertQuery, values);
        
        if (result[0].affectedRows > 0) {
          successCount++;
        } else {
          skippedCount++;
        }
        
        if ((successCount + skippedCount) % LOGGING_CONFIG.progressInterval === 0) {
          console.log(`  → Processed ${successCount + skippedCount} cases (${successCount} new, ${skippedCount} duplicates)...`);
        }
        
      } catch (error) {
        errorCount++;
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting case row ${successCount + errorCount}:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(caseItem, null, 2));
          }
        }
      }
    }
    
    // Get total count in database
    const [countResult] = await connection.execute('SELECT COUNT(*) as total FROM Cases');
    const totalInDB = countResult[0].total;
    
    console.log(`✅ Successfully inserted ${successCount} NEW cases`);
    if (skippedCount > 0) console.log(`↺ Skipped ${skippedCount} duplicate cases`);
    if (errorCount > 0) console.log(`❌ Failed to process ${errorCount} cases`);
    console.log(`📊 Total cases now in database: ${totalInDB.toLocaleString()}`);
    console.log('==================');
    
    return {
      success: errorCount === 0,
      successCount,
      skippedCount,
      errorCount,
      totalInDB: totalInDB
    };
  } catch (error) {
    console.error('Error handling cases data:', error);
    throw error;
  }
}

// FIXED: Interactions data handler - Preserves existing data
async function handleInteractionsData(interactionsData) {
  const connection = getConnection();
  
  try {
    console.log('=== INTERACTIONS DATA ===');
    console.log(`Total interactions: ${interactionsData.length}`);
    
    if (interactionsData.length === 0) {
      console.log('No interactions data to process');
      return;
    }

    if (interactionsData.length > 0) {
      console.log('Available CSV columns:', Object.keys(interactionsData[0]));
    }

    console.log('Sample interactions data:');
    interactionsData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((interactionItem, index) => {
      console.log(`Interaction ${index + 1}:`, JSON.stringify(interactionItem, null, 2));
    });
    
    // DO NOT DELETE - Preserve existing data
    console.log('Preserving existing interactions data...');
    
    const columnMapping = {
      'ID': 'id',
      'User - ID': 'user_id',
      'Answered': 'answered',
      'Channel Type': 'channel_type',
      'Clicked at': 'clicked_at',
      'Completed': 'completed',
      'Consulation': 'consulation',
      'Created At': 'created_at',
      'Delivered time': 'delivered_time',
      'Duration': 'duration',
      'Ended': 'ended',
      'From Name': 'from_name',
      'From Handle': 'from_handle',
      'Time': 'time_field',
      'Interaction Direction': 'interaction_direction',
      'Interaction Type': 'interaction_type',
      'Label': 'label',
      'Participant Name': 'participant_name',
      'Read time': 'read_time',
      'Recording': 'recording',
      'State': 'state',
      'Reference': 'reference',
      'Sent time': 'sent_time',
      'Started': 'started',
      'Status': 'status',
      'Subject': 'subject',
      'Time To Reply': 'time_to_reply',
      'Time To Reply Tracked': 'time_to_reply_tracked',
      'Transfer type': 'transfer_type',
      'Updated At': 'updated_at',
      'User - Last Name': 'user_last_name',
      'User - First Name': 'user_first_name',
      'Wait For Agent': 'wait_for_agent',
      'Wait For Customer': 'wait_for_customer'
    };
    
    // Handle duplicate column names
    const csvKeys = Object.keys(interactionsData[0] || {});
    const waitForCustomerColumns = csvKeys.filter(key => key.includes('Wait For Customer'));
    
    if (waitForCustomerColumns.length > 1) {
      columnMapping[waitForCustomerColumns[1]] = 'wait_for_customer_2';
    }
    
    // Filter valid columns
    const validColumnMapping = {};
    Object.keys(columnMapping).forEach(csvCol => {
      if (csvKeys.includes(csvCol)) {
        validColumnMapping[csvCol] = columnMapping[csvCol];
      }
    });
    
    const dbColumns = Object.values(validColumnMapping);
    const csvColumns = Object.keys(validColumnMapping);
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT IGNORE INTO Interactions (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    console.log(`Processing interactions with data preservation...`);
    
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    
    for (let i = 0; i < interactionsData.length; i++) {
      const interactionItem = interactionsData[i];
      
      try {
        const values = csvColumns.map(csvCol => {
          let value = interactionItem[csvCol];
          
          if (value === undefined || value === null || value === '') {
            return null;
          }
          
          if (typeof value === 'string') {
            value = value.trim();
            
            if (value === '') return null;
            
            const dbColumn = validColumnMapping[csvCol];
            
            if (['answered', 'clicked_at', 'created_at', 'delivered_time', 'ended', 'time_field', 'read_time', 'sent_time', 'started', 'updated_at'].includes(dbColumn)) {
              return convertToMySQLDateTime(value);
            }
            
            if (['id', 'user_id'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            if (['duration', 'time_to_reply', 'time_to_reply_tracked', 'wait_for_agent', 'wait_for_customer', 'wait_for_customer_2'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        const result = await connection.execute(insertQuery, values);
        
        if (result[0].affectedRows > 0) {
          successCount++;
        } else {
          skippedCount++;
        }
        
        if ((successCount + skippedCount) % LOGGING_CONFIG.progressInterval === 0) {
          console.log(`  → Processed ${successCount + skippedCount} interactions (${successCount} new, ${skippedCount} duplicates)...`);
        }
        
      } catch (error) {
        errorCount++;
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting interaction row ${i + 1}:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(interactionItem, null, 2));
          }
        }
      }
    }
    
    // Get total count in database
    const [countResult] = await connection.execute('SELECT COUNT(*) as total FROM Interactions');
    const totalInDB = countResult[0].total;
    
    console.log(`✅ Successfully inserted ${successCount} NEW interactions`);
    if (skippedCount > 0) console.log(`↺ Skipped ${skippedCount} duplicate interactions`);
    if (errorCount > 0) console.log(`❌ Failed to process ${errorCount} interactions`);
    console.log(`📊 Total interactions now in database: ${totalInDB.toLocaleString()}`);
    console.log('============================');
    
    return {
      success: errorCount === 0,
      successCount,
      skippedCount,
      errorCount,
      totalInDB: totalInDB
    };
    
  } catch (error) {
    console.error('Error handling interactions data:', error);
    throw error;
  }
}

// FIXED: User State Interactions data handler - Preserves existing data
async function handleUserStateInteractionsData(userStateInteractionsData) {
  const connection = getConnection();
  
  try {
    console.log('=== USER STATE INTERACTIONS DATA ===');
    console.log(`Total user state interactions: ${userStateInteractionsData.length}`);
    
    if (userStateInteractionsData.length === 0) {
      console.log('No user state interactions data to process');
      return;
    }

    console.log('Sample user state interactions data:');
    userStateInteractionsData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((userStateItem, index) => {
      console.log(`User State Interaction ${index + 1}:`, JSON.stringify(userStateItem, null, 2));
    });
    
    // DO NOT DELETE - Preserve existing data
    console.log('Preserving existing user state interactions data...');
    
    const columnMapping = {
      'Start At': 'start_at',
      'User Status': 'user_status',
      'ACD Status': 'acd_status',
      'Pause Reason': 'pause_reason',
      'User - First Name': 'user_first_name',
      'User - Last Name': 'user_last_name',
      'User - Job Title': 'user_job_title',
      'ACD available': 'acd_available',
      'User - Pause ACD when Voice Fails': 'user_pause_acd_when_voice_fails',
      'User - Not Responding Auto Recovery': 'user_not_responding_auto_recovery',
      'User - Not Responding Auto Recovery Timeout': 'user_not_responding_auto_recovery_timeout',
      'User - Ice timeout': 'user_ice_timeout',
      'User - Force turn': 'user_force_turn',
      'User - Force turn TCP': 'user_force_turn_tcp',
      'Created At': 'created_at',
      'Updated At': 'updated_at',
      'Conversation - ID': 'conversation_id',
      'ID': 'id',
      'Conversation - Label': 'conversation_label',
      'Conversation - On Hold': 'conversation_on_hold',
      'Conversation - Completed': 'conversation_completed',
      'Conversation - Ended At': 'conversation_ended_at',
      'Conversation - Answer At': 'conversation_answer_at',
      'Conversation - Start At': 'conversation_start_at',
      'User - ID': 'user_id',
      'User - Label': 'user_label',
      'Duration': 'duration'
    };
    
    // Handle duplicate columns
    const csvKeys = Object.keys(userStateInteractionsData[0] || {});
    const pauseAcdColumns = csvKeys.filter(key => key.includes('User - Pause ACD when Voice Fails'));
    if (pauseAcdColumns.length > 1) {
      columnMapping[pauseAcdColumns[1]] = 'user_pause_acd_when_voice_fails_2';
    }
    
    const dbColumns = Object.values(columnMapping);
    const csvColumns = Object.keys(columnMapping);
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT IGNORE INTO UserStateInteractions (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    
    for (const userStateItem of userStateInteractionsData) {
      try {
        const values = csvColumns.map(csvCol => {
          let value = userStateItem[csvCol];
          
          if (value === undefined || value === null || value === '') {
            return null;
          }
          
          if (typeof value === 'string') {
            value = value.trim();
            
            const dbColumn = columnMapping[csvCol];
            
            if (['start_at', 'created_at', 'updated_at', 'conversation_ended_at', 'conversation_answer_at', 'conversation_start_at'].includes(dbColumn)) {
              return convertToMySQLDateTime(value);
            }
            
            if (['id', 'conversation_id', 'user_id'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            if (['user_not_responding_auto_recovery_timeout', 'user_ice_timeout', 'duration'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        const result = await connection.execute(insertQuery, values);
        
        if (result[0].affectedRows > 0) {
          successCount++;
        } else {
          skippedCount++;
        }
        
        if ((successCount + skippedCount) % LOGGING_CONFIG.progressInterval === 0) {
          console.log(`  → Processed ${successCount + skippedCount} user state interactions...`);
        }
        
      } catch (error) {
        errorCount++;
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting user state interaction:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(userStateItem, null, 2));
          }
        }
      }
    }
    
    // Get total count in database
    const [countResult] = await connection.execute('SELECT COUNT(*) as total FROM UserStateInteractions');
    const totalInDB = countResult[0].total;
    
    console.log(`✅ Successfully inserted ${successCount} NEW user state interactions`);
    if (skippedCount > 0) console.log(`↺ Skipped ${skippedCount} duplicate user state interactions`);
    if (errorCount > 0) console.log(`❌ Failed to process ${errorCount} user state interactions`);
    console.log(`📊 Total user state interactions now in database: ${totalInDB.toLocaleString()}`);
    console.log('=====================================');
    
    return {
      success: errorCount === 0,
      successCount,
      skippedCount,
      errorCount,
      totalInDB: totalInDB
    };
  } catch (error) {
    console.error('Error handling user state interactions data:', error);
    throw error;
  }
}

// FIXED: Users data handler - Preserves existing data
async function handleUsersData(usersData) {
  const connection = getConnection();
  
  try {
    console.log('=== USERS DATA ===');
    console.log(`Total users: ${usersData.length}`);
    
    if (usersData.length === 0) {
      console.log('No users data to process');
      return;
    }

    console.log('Sample users data:');
    usersData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((userItem, index) => {
      console.log(`User ${index + 1}:`, JSON.stringify(userItem, null, 2));
    });
    
    // DO NOT DELETE - Preserve existing data
    console.log('Preserving existing users data...');
    
    const columnMapping = {
      'First Name': 'first_name',
      'Last Name': 'last_name',
      'Email': 'email',
      'Job Title': 'job_title',
      'Allow Report Access': 'allow_report_access',
      'Allow Supervisor Access': 'allow_supervisor_access',
      'Allow Messaging Access': 'allow_messaging_access',
      'Allow Settings Access': 'allow_settings_access',
      'User Type': 'user_type',
      'PBX User': 'pbx_user',
      'Enabled': 'enabled',
      'ID': 'id',
      'Label': 'label'
    };
    
    const dbColumns = Object.values(columnMapping);
    const csvColumns = Object.keys(columnMapping);
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT IGNORE INTO Users (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    
    for (const userItem of usersData) {
      try {
        const values = csvColumns.map(csvCol => {
          let value = userItem[csvCol];
          
          if (value === undefined || value === null || value === '') {
            return null;
          }
          
          if (typeof value === 'string') {
            value = value.trim();
            
            const dbColumn = columnMapping[csvCol];
            
            if (['pbx_user', 'enabled'].includes(dbColumn)) {
              return convertToBoolean(value);
            }
            
            if (['id'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            if (['allow_report_access', 'allow_supervisor_access', 'allow_messaging_access', 'allow_settings_access'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        const result = await connection.execute(insertQuery, values);
        
        if (result[0].affectedRows > 0) {
          successCount++;
        } else {
          skippedCount++;
        }
        
        if ((successCount + skippedCount) % 100 === 0) {
          console.log(`  → Processed ${successCount + skippedCount} users...`);
        }
        
      } catch (error) {
        errorCount++;
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting user:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(userItem, null, 2));
          }
        }
      }
    }
    
    // Get total count in database
    const [countResult] = await connection.execute('SELECT COUNT(*) as total FROM Users');
    const totalInDB = countResult[0].total;
    
    console.log(`✅ Successfully inserted ${successCount} NEW users`);
    if (skippedCount > 0) console.log(`↺ Skipped ${skippedCount} duplicate users`);
    if (errorCount > 0) console.log(`❌ Failed to process ${errorCount} users`);
    console.log(`📊 Total users now in database: ${totalInDB.toLocaleString()}`);
    console.log('==================');
    
    return {
      success: errorCount === 0,
      successCount,
      skippedCount,
      errorCount,
      totalInDB: totalInDB
    };
  } catch (error) {
    console.error('Error handling users data:', error);
    throw error;
  }
}

// FIXED: User Session History data handler - Preserves existing data
async function handleUserSessionHistoryData(userSessionHistoryData) {
  const connection = getConnection();
  
  try {
    console.log('=== USER SESSION HISTORY DATA ===');
    console.log(`Total user session history records: ${userSessionHistoryData.length}`);
    
    if (userSessionHistoryData.length === 0) {
      console.log('No user session history data to process');
      return;
    }

    console.log('Sample user session history data:');
    userSessionHistoryData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((sessionItem, index) => {
      console.log(`Session ${index + 1}:`, JSON.stringify(sessionItem, null, 2));
    });
    
    // DO NOT DELETE - Preserve existing data
    console.log('Preserving existing user session history data...');
    
    const columnMapping = {
      'ID': 'id',
      'User - ID': 'user_id',
      'Label': 'label',
      'User - Label': 'user_label',
      'Updated At': 'updated_at',
      'Created At': 'created_at',
      'User - Last Sign In At': 'user_last_sign_in_at'
    };
    
    const dbColumns = Object.values(columnMapping);
    const csvColumns = Object.keys(columnMapping);
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT IGNORE INTO UserSessionHistory (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    
    for (const sessionItem of userSessionHistoryData) {
      try {
        const values = csvColumns.map(csvCol => {
          let value = sessionItem[csvCol];
          
          if (value === undefined || value === null || value === '') {
            return null;
          }
          
          if (typeof value === 'string') {
            value = value.trim();
            
            const dbColumn = columnMapping[csvCol];
            
            if (['updated_at', 'created_at', 'user_last_sign_in_at'].includes(dbColumn)) {
              return convertToMySQLDateTime(value);
            }
            
            if (['id', 'user_id'].includes(dbColumn)) {
              return convertToInt(value);
            }
          }
          
          return value;
        });
        
        const result = await connection.execute(insertQuery, values);
        
        if (result[0].affectedRows > 0) {
          successCount++;
        } else {
          skippedCount++;
        }
        
        if ((successCount + skippedCount) % LOGGING_CONFIG.progressInterval === 0) {
          console.log(`  → Processed ${successCount + skippedCount} user session history records...`);
        }
        
      } catch (error) {
        errorCount++;
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting user session history:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(sessionItem, null, 2));
          }
        }
      }
    }
    
    // Get total count in database
    const [countResult] = await connection.execute('SELECT COUNT(*) as total FROM UserSessionHistory');
    const totalInDB = countResult[0].total;
    
    console.log(`✅ Successfully inserted ${successCount} NEW user session history records`);
    if (skippedCount > 0) console.log(`↺ Skipped ${skippedCount} duplicate user session history records`);
    if (errorCount > 0) console.log(`❌ Failed to process ${errorCount} user session history records`);
    console.log(`📊 Total user session history records now in database: ${totalInDB.toLocaleString()}`);
    console.log('=====================================');
    
    return {
      success: errorCount === 0,
      successCount,
      skippedCount,
      errorCount,
      totalInDB: totalInDB
    };
  } catch (error) {
    console.error('Error handling user session history data:', error);
    throw error;
  }
}

// FIXED: Schedule data handler - Preserves existing data
async function handleScheduleData(scheduleData) {
  const connection = getConnection();
  
  try {
    console.log('=== SCHEDULE DATA ===');
    console.log(`Total schedule records: ${scheduleData.length}`);
    
    if (scheduleData.length === 0) {
      console.log('No schedule data to process');
      return;
    }

    console.log('Sample schedule data:');
    scheduleData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((scheduleItem, index) => {
      console.log(`Schedule ${index + 1}:`, JSON.stringify(scheduleItem, null, 2));
    });
    
    // DO NOT DELETE - Preserve existing data
    console.log('Preserving existing schedule data...');
    
    const columnMapping = {
      'Closed Time Slots': 'closed_time_slots',
      'ID': 'id',
      'Label': 'label',
      'Name': 'name',
      'Opening Time Slots': 'opening_time_slots'
    };
    
    const dbColumns = Object.values(columnMapping);
    const csvColumns = Object.keys(columnMapping);
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT IGNORE INTO Schedule (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    
    for (const scheduleItem of scheduleData) {
      try {
        const values = csvColumns.map(csvCol => {
          let value = scheduleItem[csvCol];
          
          if (value === undefined || value === null || value === '') {
            return null;
          }
          
          if (typeof value === 'string') {
            value = value.trim();
            
            const dbColumn = columnMapping[csvCol];
            
            if (['id'].includes(dbColumn)) {
              return convertToInt(value);
            }
          }
          
          return value;
        });
        
        const result = await connection.execute(insertQuery, values);
        
        if (result[0].affectedRows > 0) {
          successCount++;
          console.log(`  → Inserted NEW schedule record: ${scheduleItem['Name'] || scheduleItem['Label'] || 'Unknown'}...`);
        } else {
          skippedCount++;
          console.log(`  → Skipped duplicate schedule record: ${scheduleItem['Name'] || scheduleItem['Label'] || 'Unknown'}...`);
        }
        
      } catch (error) {
        errorCount++;
        console.error(`Error inserting schedule:`, error.message);
        console.error('Sample problematic data:', JSON.stringify(scheduleItem, null, 2));
      }
    }
    
    // Get total count in database
    const [countResult] = await connection.execute('SELECT COUNT(*) as total FROM Schedule');
    const totalInDB = countResult[0].total;
    
    console.log(`✅ Successfully inserted ${successCount} NEW schedule records`);
    if (skippedCount > 0) console.log(`↺ Skipped ${skippedCount} duplicate schedule records`);
    if (errorCount > 0) console.log(`❌ Failed to process ${errorCount} schedule records`);
    console.log(`📊 Total schedule records now in database: ${totalInDB.toLocaleString()}`);
    console.log('=====================');
    
    return {
      success: errorCount === 0,
      successCount,
      skippedCount,
      errorCount,
      totalInDB: totalInDB
    };
  } catch (error) {
    console.error('Error handling schedule data:', error);
    throw error;
  }
}

// FIXED: SLA Policy data handler - Preserves existing data
async function handleSLAPolicyData(slaPolicyData) {
  const connection = getConnection();
  
  try {
    console.log('=== SLA POLICY DATA ===');
    console.log(`Total SLA Policy records: ${slaPolicyData.length}`);
    
    if (slaPolicyData.length === 0) {
      console.log('No SLA Policy data to process');
      return;
    }

    console.log('Sample SLA Policy data:');
    slaPolicyData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((slaPolicyItem, index) => {
      console.log(`SLA Policy ${index + 1}:`, JSON.stringify(slaPolicyItem, null, 2));
    });
    
    // DO NOT DELETE - Preserve existing data
    console.log('Preserving existing SLA Policy data...');
    
    const columnMapping = {
      'Enabled': 'enabled',
      'Escalation Assigned To': 'escalation_assigned_to',
      'Escalation Assigned To - ID': 'escalation_assigned_to_id',
      'Escalation Assigned To - Label': 'escalation_assigned_to_label',
      'Escalation Rules': 'escalation_rules',
      'Escalation rules use schedule': 'escalation_rules_use_schedule',
      'First Reply Target': 'first_reply_target',
      'ID': 'id',
      'Label': 'label',
      'Last Reply Target': 'last_reply_target',
      'Name': 'name',
      'Reset targets on assignment': 'reset_targets_on_assignment',
      'Solved target': 'solved_target'
    };
    
    const dbColumns = Object.values(columnMapping);
    const csvColumns = Object.keys(columnMapping);
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT IGNORE INTO SLAPolicy (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    
    for (const slaPolicyItem of slaPolicyData) {
      try {
        const values = csvColumns.map(csvCol => {
          let value = slaPolicyItem[csvCol];
          
          if (value === undefined || value === null || value === '') {
            return null;
          }
          
          if (typeof value === 'string') {
            value = value.trim();
            
            const dbColumn = columnMapping[csvCol];
            
            if (['enabled', 'escalation_rules_use_schedule', 'reset_targets_on_assignment'].includes(dbColumn)) {
              return convertToBoolean(value);
            }
            
            if (['id', 'first_reply_target'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            if (['escalation_assigned_to', 'escalation_assigned_to_id', 'last_reply_target', 'solved_target'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        const result = await connection.execute(insertQuery, values);
        
        if (result[0].affectedRows > 0) {
          successCount++;
          console.log(`  → Inserted NEW SLA Policy: ${slaPolicyItem['Name'] || slaPolicyItem['Label'] || 'Unknown'}...`);
        } else {
          skippedCount++;
          console.log(`  → Skipped duplicate SLA Policy: ${slaPolicyItem['Name'] || slaPolicyItem['Label'] || 'Unknown'}...`);
        }
        
      } catch (error) {
        errorCount++;
        console.error(`Error inserting SLA Policy:`, error.message);
        console.error('Sample problematic data:', JSON.stringify(slaPolicyItem, null, 2));
      }
    }
    
    // Get total count in database
    const [countResult] = await connection.execute('SELECT COUNT(*) as total FROM SLAPolicy');
    const totalInDB = countResult[0].total;
    
    console.log(`✅ Successfully inserted ${successCount} NEW SLA Policy records`);
    if (skippedCount > 0) console.log(`↺ Skipped ${skippedCount} duplicate SLA Policy records`);
    if (errorCount > 0) console.log(`❌ Failed to process ${errorCount} SLA Policy records`);
    console.log(`📊 Total SLA Policy records now in database: ${totalInDB.toLocaleString()}`);
    console.log('======================');
    
    return {
      success: errorCount === 0,
      successCount,
      skippedCount,
      errorCount,
      totalInDB: totalInDB
    };
  } catch (error) {
    console.error('Error handling SLA Policy data:', error);
    throw error;
  }
}

// FIXED: NOC Interactions data handler - Preserves existing data
async function handleNOCInteractionsData(nocInteractionsData) {
  const connection = getConnection();
  
  try {
    console.log('=== NOC INTERACTIONS DATA ===');
    console.log(`Total NOC interactions: ${nocInteractionsData.length}`);
    
    if (nocInteractionsData.length === 0) {
      console.log('No NOC interactions data to process');
      return;
    }

    console.log('Sample NOC interactions data:');
    nocInteractionsData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((nocInteractionItem, index) => {
      console.log(`NOC Interaction ${index + 1}:`, JSON.stringify(nocInteractionItem, null, 2));
    });
    
    // DO NOT DELETE - Preserve existing data
    console.log('Preserving existing NOC interactions data...');
    
    const columnMapping = {
      'ID': 'id',
      'Answered': 'answered',
      'Auto answer': 'auto_answer',
      'Channel Type': 'channel_type',
      'Completed': 'completed',
      'Conversation - ID': 'conversation_id',
      'Created At': 'created_at',
      'Created By - ID': 'created_by_id',
      'Created By - First Name': 'created_by_first_name',
      'Created By - Label': 'created_by_label',
      'Created By - Last Name': 'created_by_last_name',
      'Department - ID': 'department_id',
      'Department - Label': 'department_label',
      'Disposition - ID': 'disposition_id',
      'Disposition - Label': 'disposition_label',
      'Disposition - Name': 'disposition_name',
      'Disposition Required': 'disposition_required',
      'Ended': 'ended',
      'From': 'from_field',
      'From Name': 'from_name',
      'Hangup Cause': 'hangup_cause',
      'Hangup Party': 'hangup_party',
      'Interaction Direction': 'interaction_direction',
      'Interaction Type': 'interaction_type',
      'Label': 'label',
      'On Hold': 'on_hold',
      'Pending outcome': 'pending_outcome',
      'Started': 'started',
      'Status': 'status',
      'Time To Reply': 'time_to_reply',
      'Tracked': 'tracked',
      'Updated At': 'updated_at',
      'User - ID': 'user_id',
      'User - Label': 'user_label',
      'User - Job Title': 'user_job_title',
      'User - User Type': 'user_user_type',
      'Wait For Agent': 'wait_for_agent',
      'Wait For Customer': 'wait_for_customer',
      'Contact - ID': 'contact_id',
      'Transfer type': 'transfer_type',
      'Read time': 'read_time',
      'Original channel - Source': 'original_channel_source',
      'Original channel - Lead Source': 'original_channel_lead_source',
      'Original channel - Channel Types': 'original_channel_channel_types',
      'Handle Time': 'handle_time',
      'Endpoint - Name': 'endpoint_name',
      'Endpoint - Lead Source': 'endpoint_lead_source',
      'Duration': 'duration',
      'Conversation - Channel Types': 'conversation_channel_types',
      'B Leg Interaction - Status': 'b_leg_interaction_status',
      'B Leg Interaction - CSAT': 'b_leg_interaction_csat',
      'B Leg Interaction - Time To Reply': 'b_leg_interaction_time_to_reply',
      'B Leg Interaction - Time To Reply_1': 'b_leg_interaction_time_to_reply_2'
    };
    
    const dbColumns = Object.values(columnMapping);
    const csvColumns = Object.keys(columnMapping);
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT IGNORE INTO NOCInteractions (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    
    for (const nocInteractionItem of nocInteractionsData) {
      try {
        const values = csvColumns.map(csvCol => {
          let value = nocInteractionItem[csvCol];
          
          if (value === undefined || value === null || value === '') {
            return null;
          }
          
          if (typeof value === 'string') {
            value = value.trim();
            
            const dbColumn = columnMapping[csvCol];
            
            if (['answered', 'completed', 'created_at', 'ended', 'started', 'updated_at', 'read_time'].includes(dbColumn)) {
              return convertToMySQLDateTime(value);
            }
            
            if (['auto_answer', 'disposition_required', 'on_hold', 'pending_outcome', 'tracked'].includes(dbColumn)) {
              return convertToBoolean(value);
            }
            
            if (['id', 'conversation_id', 'created_by_id', 'department_id', 'disposition_id', 'user_id', 'contact_id'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            if (['time_to_reply', 'wait_for_agent', 'wait_for_customer', 'handle_time', 'duration', 'b_leg_interaction_csat', 'b_leg_interaction_time_to_reply', 'b_leg_interaction_time_to_reply_2'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        const result = await connection.execute(insertQuery, values);
        
        if (result[0].affectedRows > 0) {
          successCount++;
        } else {
          skippedCount++;
        }
        
        if ((successCount + skippedCount) % 50 === 0) {
          console.log(`  → Processed ${successCount + skippedCount} NOC interactions (${successCount} new, ${skippedCount} duplicates)...`);
        }
        
      } catch (error) {
        errorCount++;
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting NOC interaction:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(nocInteractionItem, null, 2));
          }
        }
      }
    }
    
    // Get total count in database
    const [countResult] = await connection.execute('SELECT COUNT(*) as total FROM NOCInteractions');
    const totalInDB = countResult[0].total;
    
    console.log(`✅ Successfully inserted ${successCount} NEW NOC interactions`);
    if (skippedCount > 0) console.log(`↺ Skipped ${skippedCount} duplicate NOC interactions`);
    if (errorCount > 0) console.log(`❌ Failed to process ${errorCount} NOC interactions`);
    console.log(`📊 Total NOC interactions now in database: ${totalInDB.toLocaleString()}`);
    console.log('=============================');
    
    return {
      success: errorCount === 0,
      successCount,
      skippedCount,
      errorCount,
      totalInDB: totalInDB
    };
  } catch (error) {
    console.error('Error handling NOC interactions data:', error);
    throw error;
  }
}

// FIXED: Conversations data handler - Preserves existing data (Updated version)
async function handleConversationsData(conversationsData) {
  const connection = getConnection();
  
  try {
    console.log('=== CONVERSATIONS DATA ===');
    console.log(`Total conversations: ${conversationsData.length}`);
    
    // Early return for empty data
    if (conversationsData.length === 0) {
      console.log('No conversations data to process');
      return {
        success: true,
        successCount: 0,
        skippedCount: 0,
        errorCount: 0,
        totalRecords: 0,
        totalInDB: 0,
        message: 'No data to process'
      };
    }

    // Validate data structure
    const firstRecord = conversationsData[0];
    if (!firstRecord || typeof firstRecord !== 'object') {
      console.error('❌ Invalid data structure - first record is not a valid object');
      console.error('First record:', firstRecord);
      throw new Error('Invalid conversations data structure');
    }

    const csvKeys = Object.keys(firstRecord);
    if (csvKeys.length === 0) {
      console.error('❌ Invalid data structure - no columns found in CSV data');
      console.error('Sample of first few records:');
      conversationsData.slice(0, 3).forEach((record, index) => {
        console.error(`Record ${index}:`, record);
      });
      throw new Error('No columns found in conversations CSV data');
    }

    console.log(`✅ Valid CSV structure detected with ${csvKeys.length} columns`);
    console.log('Available CSV columns:', csvKeys);

    // Show sample data for debugging
    console.log('Sample conversations data:');
    conversationsData.slice(0, Math.min(2, conversationsData.length)).forEach((conversationItem, index) => {
      console.log(`Conversation ${index + 1}:`, JSON.stringify(conversationItem, null, 2));
    });
    
    // DO NOT DELETE - Preserve existing data
    console.log('Preserving existing conversations data...');
    
    const columnMapping = {
      'ID': 'id',
      'Start At': 'start_at',
      'Ended At': 'ended_at',
      'Completed': 'completed',
      'Contact Method': 'contact_method',
      'Inbound': 'inbound',
      'Channel Types': 'channel_types',
      'Label': 'label',
      'Created At': 'created_at',
      'Updated At': 'updated_at',
      'Contact - Building Name ': 'contact_building_name',  // Note the trailing space
      'Contact - Building Name': 'contact_building_name',   // Without trailing space
      'Contact - Portfolio': 'contact_portfolio'
    };
    
    // Create robust column mapping
    const validColumnMapping = {};
    
    // First pass: exact matches
    Object.keys(columnMapping).forEach(csvCol => {
      if (csvKeys.includes(csvCol)) {
        validColumnMapping[csvCol] = columnMapping[csvCol];
        console.log(`✓ Exact match: "${csvCol}"`);
      }
    });
    
    // Second pass: fuzzy matching for missing columns
    const missingColumns = Object.keys(columnMapping).filter(col => !csvKeys.includes(col));
    missingColumns.forEach(missingCol => {
      const normalizedMissing = missingCol.toLowerCase().replace(/\s+/g, '').replace(/[^a-z0-9]/g, '');
      const matchingKey = csvKeys.find(key => {
        const normalizedKey = key.toLowerCase().replace(/\s+/g, '').replace(/[^a-z0-9]/g, '');
        return normalizedKey === normalizedMissing || 
               normalizedKey.includes(normalizedMissing) || 
               normalizedMissing.includes(normalizedKey);
      });
      
      if (matchingKey && !Object.values(validColumnMapping).includes(columnMapping[missingCol])) {
        validColumnMapping[matchingKey] = columnMapping[missingCol];
        console.log(`✓ Fuzzy match: "${missingCol}" → "${matchingKey}"`);
      }
    });
    
    // Critical: Ensure ID field exists
    let idFieldFound = false;
    
    // Check if we have an ID field mapped
    for (const [csvCol, dbCol] of Object.entries(validColumnMapping)) {
      if (dbCol === 'id') {
        idFieldFound = true;
        console.log(`✅ ID field confirmed: "${csvCol}" → "${dbCol}"`);
        break;
      }
    }
    
    // If no ID field found, search more aggressively
    if (!idFieldFound) {
      console.log('⚠️ No ID field found in initial mapping, searching alternatives...');
      
      // Look for any field that might be an ID
      const idCandidates = csvKeys.filter(key => {
        const normalizedKey = key.toLowerCase();
        return normalizedKey.includes('id') || 
               normalizedKey === 'conversation' ||
               normalizedKey.includes('conv');
      });
      
      console.log('ID candidates found:', idCandidates);
      
      if (idCandidates.length > 0) {
        // Prefer exact "ID" match, then shortest name
        const selectedId = idCandidates.find(key => key === 'ID') || 
                          idCandidates.find(key => key.toLowerCase() === 'id') ||
                          idCandidates.sort((a, b) => a.length - b.length)[0];
        
        validColumnMapping[selectedId] = 'id';
        console.log(`✅ Selected ID field: "${selectedId}"`);
        idFieldFound = true;
      }
    }
    
    // Final fallback: use first column as ID
    if (!idFieldFound && csvKeys.length > 0) {
      const firstColumn = csvKeys[0];
      validColumnMapping[firstColumn] = 'id';
      console.log(`⚠️ Using first column as ID: "${firstColumn}"`);
      idFieldFound = true;
    }
    
    // Absolute last resort
    if (!idFieldFound) {
      console.error('❌ Critical: No suitable ID field found anywhere');
      console.error('Available columns:', csvKeys);
      throw new Error(`No ID field could be identified in conversations data. Available columns: ${csvKeys.join(', ')}`);
    }
    
    console.log('✅ Final column mapping:', validColumnMapping);
    
    // Validate we have at least an ID field
    const dbColumns = Object.values(validColumnMapping);
    const csvColumns = Object.keys(validColumnMapping);
    
    if (!dbColumns.includes('id')) {
      throw new Error('Critical error: ID field not properly mapped');
    }
    
    console.log(`📋 Mapping ${csvColumns.length} CSV columns to ${dbColumns.length} database columns`);
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT IGNORE INTO Conversations (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    console.log('Using query:', insertQuery);
    
    // Process data in batches
    const BATCH_SIZE = 1000;
    const totalBatches = Math.ceil(conversationsData.length / BATCH_SIZE);
    
    console.log(`Processing ${conversationsData.length} conversations in ${totalBatches} batches of ${BATCH_SIZE}`);
    
    let totalSuccessCount = 0;
    let totalErrorCount = 0;
    let totalSkippedCount = 0;
    
    for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
      const startIndex = batchIndex * BATCH_SIZE;
      const endIndex = Math.min(startIndex + BATCH_SIZE, conversationsData.length);
      const batch = conversationsData.slice(startIndex, endIndex);
      
      console.log(`Processing batch ${batchIndex + 1}/${totalBatches} (records ${startIndex + 1}-${endIndex})...`);
      
      let batchSuccessCount = 0;
      let batchErrorCount = 0;
      
      for (let i = 0; i < batch.length; i++) {
        const conversationItem = batch[i];
        const globalIndex = startIndex + i;
        
        try {
          const values = csvColumns.map(csvCol => {
            let value = conversationItem[csvCol];
            
            if (value === undefined || value === null || value === '') {
              return null;
            }
            
            if (typeof value === 'string') {
              value = value.trim();
              if (value === '') return null;
              
              const dbColumn = validColumnMapping[csvCol];
              
              // Handle datetime fields
              if (['start_at', 'ended_at', 'completed', 'created_at', 'updated_at'].includes(dbColumn)) {
                return convertToMySQLDateTime(value);
              }
              
              // Handle ID field
              if (dbColumn === 'id') {
                const intValue = convertToInt(value);
                if (intValue === null) {
                  console.warn(`Warning row ${globalIndex + 1}: Could not convert ID "${value}" to integer`);
                  return null; // Let database handle this
                }
                return intValue;
              }
            }
            
            return value;
          });
          
          const result = await connection.execute(insertQuery, values);
          
          if (result[0].affectedRows > 0) {
            batchSuccessCount++;
            totalSuccessCount++;
          } else {
            totalSkippedCount++;
          }
          
        } catch (error) {
          batchErrorCount++;
          totalErrorCount++;
          
          if (totalErrorCount <= 3) { // Only log first 3 errors
            console.error(`Error inserting conversation row ${globalIndex + 1}:`, error.message);
            if (totalErrorCount === 1) {
              console.error('Problematic data sample:', JSON.stringify(conversationItem, null, 2));
            }
          }
        }
      }
      
      console.log(`  ✓ Batch ${batchIndex + 1}: ${batchSuccessCount} inserted, ${batchErrorCount} errors, ${batch.length - batchSuccessCount - batchErrorCount} skipped`);
      
      // Memory management for large datasets
      if (conversationsData.length > 10000 && global.gc && batchIndex % 5 === 0) {
        global.gc();
      }
      
      const percentComplete = ((batchIndex + 1) / totalBatches * 100).toFixed(1);
      console.log(`Progress: ${percentComplete}% complete`);
    }
    
    // Get final count
    const [countResult] = await connection.execute('SELECT COUNT(*) as total FROM Conversations');
    const totalInDB = countResult[0].total;
    
    console.log(`✅ Processing completed:`);
    console.log(`  - Successfully inserted: ${totalSuccessCount} NEW conversations`);
    console.log(`  - Skipped duplicates: ${totalSkippedCount} conversations`);
    console.log(`  - Errors encountered: ${totalErrorCount} conversations`);
    console.log(`  - Total in database: ${totalInDB.toLocaleString()} conversations`);
    console.log('=========================');
    
    return {
      success: totalErrorCount === 0,
      successCount: totalSuccessCount,
      skippedCount: totalSkippedCount,
      errorCount: totalErrorCount,
      totalRecords: conversationsData.length,
      totalInDB: totalInDB,
      preservedExistingData: true
    };
    
  } catch (error) {
    console.error('❌ Fatal error in handleConversationsData:', error.message);
    console.error('Error details:', error);
    
    // Return a proper error result instead of throwing
    return {
      success: false,
      successCount: 0,
      skippedCount: 0,
      errorCount: conversationsData ? conversationsData.length : 0,
      totalRecords: conversationsData ? conversationsData.length : 0,
      totalInDB: 0,
      error: error.message,
      preservedExistingData: true
    };
  }
}
module.exports = {
  saveBuildingsData,
  handleCasesData,
  handleConversationsData,
  handleInteractionsData,
  handleUserStateInteractionsData,
  handleUsersData,
  handleUserSessionHistoryData,
  handleScheduleData,
  handleSLAPolicyData,
  handleNOCInteractionsData,
  convertToMySQLDateTime,
  convertToInt,
  convertToDecimal,
  convertToBoolean
};