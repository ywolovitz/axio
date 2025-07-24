const { getConnection } = require('./database');
const { LOGGING_CONFIG } = require('./config');

// Enhanced helper function to convert date strings to MySQL datetime format
function convertToMySQLDateTime(dateString) {
  if (!dateString || dateString === '') return null;
  
  try {
    // Handle various date formats
    let date;
    
    // Try parsing as-is first
    date = new Date(dateString);
    
    // If that fails, try some common formats
    if (isNaN(date.getTime())) {
      // Try parsing with different separators
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
    // Remove any non-digit characters except for negative sign
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
    // Remove any non-digit characters except for decimal point and negative sign
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

// Buildings data handler
async function saveBuildingsData(buildingsData) {
  const connection = getConnection();
  
  try {
    // Clear existing data (optional - remove this if you want to append)
    await connection.execute('DELETE FROM Buildings');
    
    const insertQuery = `
      INSERT INTO Buildings (
        id, unit_count, building_name, deployment_method, portfolio_id, 
        portfolio, label, building_manager, building_manager_details
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    for (const building of buildingsData) {
      // Map CSV columns to database fields (adjust column names as needed)
      const values = [
        convertToInt(building['ID']), // Use CSV ID as primary key
        convertToInt(building['Unit Count']),
        building['Building Name'] || '',
        building['Deployment Methodology'] || '',
        convertToInt(building['Portfolio ID']),
        building['Portfolio'] || '',
        building['Label'] || '',
        building['Building Manager'] || '',
        building['Building Manager - Unit Details'] || ''
      ];
      
      await connection.execute(insertQuery, values);
    }
    
    console.log(`Successfully saved ${buildingsData.length} buildings to database`);
  } catch (error) {
    console.error('Error saving buildings data:', error);
    throw error;
  }
}

// Cases data handler
async function handleCasesData(casesData) {
  const connection = getConnection();
  
  try {
    console.log('=== CASES DATA ===');
    console.log(`Total cases: ${casesData.length}`);
    
    if (casesData.length === 0) {
      console.log('No cases data to process');
      return;
    }

    // Log sample cases data
    console.log('Sample cases data:');
    casesData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((caseItem, index) => {
      console.log(`Case ${index + 1}:`, JSON.stringify(caseItem, null, 2));
    });
    
    // Clear existing data
    await connection.execute('DELETE FROM Cases');
    
    // CSV column name to database column mapping
    const columnMapping = {
      'ID': 'id', // Use CSV ID as primary key
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
    const insertQuery = `INSERT INTO Cases (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    console.log(`Inserting data into Cases table with ${dbColumns.length} columns`);
    
    let successCount = 0;
    let errorCount = 0;
    
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
            
            // Handle date fields
            if (['created_at', 'closed_at', 'first_reply', 'first_solved_at', 'solved_at'].includes(dbColumn)) {
              return convertToMySQLDateTime(value);
            }
            
            // Handle integer fields
            if (['id', 'assigned_to', 'building_id', 'sla_policy_id'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            // Handle decimal fields
            if (['time_logged', 'department_id', 'building_unit_details'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        await connection.execute(insertQuery, values);
        successCount++;
        
        if (successCount % LOGGING_CONFIG.progressInterval === 0) {
          console.log(`  → Inserted ${successCount} cases...`);
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
    
    console.log(`✓ Successfully inserted ${successCount} cases into database`);
    if (errorCount > 0) {
      console.log(`✗ Failed to insert ${errorCount} cases`);
    }
    console.log('==================');
  } catch (error) {
    console.error('Error handling cases data:', error);
    throw error;
  }
}

// Optimized Conversations data handler for large datasets
async function handleConversationsData(conversationsData) {
  const connection = getConnection();
  
  try {
    console.log('=== CONVERSATIONS DATA ===');
    console.log(`Total conversations: ${conversationsData.length}`);
    
    if (conversationsData.length === 0) {
      console.log('No conversations data to process');
      return;
    }

    // Log available CSV columns for debugging
    if (conversationsData.length > 0) {
      console.log('Available CSV columns:', Object.keys(conversationsData[0]));
    }

    console.log('Sample conversations data:');
    conversationsData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((conversationItem, index) => {
      console.log(`Conversation ${index + 1}:`, JSON.stringify(conversationItem, null, 2));
    });
    
    // Clear existing data
    console.log('Clearing existing conversations data...');
    await connection.execute('DELETE FROM Conversations');
    console.log('✓ Existing data cleared');
    
    // Updated column mapping to match actual CSV structure
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
      'Contact - Building Name ': 'contact_building_name', // Note the trailing space
      'Contact - Building Name': 'contact_building_name',   // Alternative without space
      'Contact - Portfolio': 'contact_portfolio'
    };
    
    // Get actual CSV columns and filter valid mappings
    const csvKeys = Object.keys(conversationsData[0] || {});
    const validColumnMapping = {};
    const missingColumns = [];
    
    Object.keys(columnMapping).forEach(csvCol => {
      if (csvKeys.includes(csvCol)) {
        validColumnMapping[csvCol] = columnMapping[csvCol];
      } else {
        missingColumns.push(csvCol);
      }
    });
    
    if (missingColumns.length > 0) {
      console.log('Missing CSV columns (will be skipped):', missingColumns);
    }
    
    console.log('Valid column mapping:', validColumnMapping);
    
    const dbColumns = Object.values(validColumnMapping);
    const csvColumns = Object.keys(validColumnMapping);
    
    // Use batch processing for large datasets
    const BATCH_SIZE = 1000; // Process 1000 records at a time
    const totalBatches = Math.ceil(conversationsData.length / BATCH_SIZE);
    
    console.log(`Processing ${conversationsData.length} conversations in ${totalBatches} batches of ${BATCH_SIZE}`);
    
    let totalSuccessCount = 0;
    let totalErrorCount = 0;
    const errors = [];
    
    // Prepare the insert query once
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT INTO Conversations (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    console.log('Insert query:', insertQuery);
    
    // Process in batches
    for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
      const startIndex = batchIndex * BATCH_SIZE;
      const endIndex = Math.min(startIndex + BATCH_SIZE, conversationsData.length);
      const batch = conversationsData.slice(startIndex, endIndex);
      
      console.log(`Processing batch ${batchIndex + 1}/${totalBatches} (records ${startIndex + 1}-${endIndex})...`);
      
      // Use transaction for each batch to improve performance and provide rollback capability
      await connection.beginTransaction();
      
      try {
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
                
                // Skip empty strings
                if (value === '') {
                  return null;
                }
                
                const dbColumn = validColumnMapping[csvCol];
                
                // Handle date fields
                if (['start_at', 'ended_at', 'completed'].includes(dbColumn)) {
                  return convertToMySQLDateTime(value);
                }
                
                // Handle integer fields
                if (['id'].includes(dbColumn)) {
                  return convertToInt(value);
                }
              }
              
              return value;
            });
            
            await connection.execute(insertQuery, values);
            batchSuccessCount++;
            totalSuccessCount++;
            
          } catch (error) {
            batchErrorCount++;
            totalErrorCount++;
            
            const errorDetail = {
              batch: batchIndex + 1,
              row: globalIndex + 1,
              error: error.message,
              data: totalErrorCount <= 3 ? conversationItem : null // Only log first 3 problematic records
            };
            errors.push(errorDetail);
            
            if (totalErrorCount <= LOGGING_CONFIG.maxErrorLogs) {
              console.error(`Error inserting conversation row ${globalIndex + 1}:`, error.message);
              if (totalErrorCount === 1) {
                console.error('Sample problematic data:', JSON.stringify(conversationItem, null, 2));
                console.error('Prepared values:', values);
              }
            }
          }
        }
        
        // Commit the batch transaction
        await connection.commit();
        
        console.log(`  ✓ Batch ${batchIndex + 1} completed: ${batchSuccessCount} success, ${batchErrorCount} errors`);
        
        // Force garbage collection for large datasets if available
        if (conversationsData.length > 10000 && global.gc) {
          global.gc();
        }
        
      } catch (batchError) {
        // Rollback the batch transaction on error
        await connection.rollback();
        console.error(`✗ Batch ${batchIndex + 1} failed, rolling back:`, batchError.message);
        
        // Count all records in this batch as errors
        const batchSize = batch.length;
        totalErrorCount += batchSize;
        
        errors.push({
          batch: batchIndex + 1,
          error: `Batch failed: ${batchError.message}`,
          recordsAffected: batchSize
        });
      }
      
      // Progress update
      const percentComplete = ((batchIndex + 1) / totalBatches * 100).toFixed(1);
      console.log(`Progress: ${percentComplete}% (${totalSuccessCount} records processed)`);
    }
    
    console.log(`✓ Successfully inserted ${totalSuccessCount} conversations into database`);
    if (totalErrorCount > 0) {
      console.log(`✗ Failed to insert ${totalErrorCount} conversations`);
      console.log('Error summary:', errors.slice(0, 5)); // Show first 5 errors
    }
    
    // Final statistics
    const successRate = totalSuccessCount > 0 ? ((totalSuccessCount / conversationsData.length) * 100).toFixed(2) : '0';
    console.log(`Success rate: ${successRate}% (${totalSuccessCount}/${conversationsData.length})`);
    console.log('=========================');
    
    return {
      success: totalErrorCount === 0,
      successCount: totalSuccessCount,
      errorCount: totalErrorCount,
      totalRecords: conversationsData.length,
      successRate: `${successRate}%`,
      errors: errors.slice(0, 10), // Return first 10 errors for debugging
      batchesProcessed: totalBatches
    };
    
  } catch (error) {
    console.error('Error handling conversations data:', error);
    throw error;
  }
}

// Fixed Interactions data handler
async function handleInteractionsData(interactionsData) {
  const connection = getConnection();
  
  try {
    console.log('=== INTERACTIONS DATA ===');
    console.log(`Total interactions: ${interactionsData.length}`);
    
    if (interactionsData.length === 0) {
      console.log('No interactions data to process');
      return;
    }

    // Log the actual CSV headers to debug
    if (interactionsData.length > 0) {
      console.log('Available CSV columns:', Object.keys(interactionsData[0]));
    }

    console.log('Sample interactions data:');
    interactionsData.slice(0, LOGGING_CONFIG.sampleDataLogs).forEach((interactionItem, index) => {
      console.log(`Interaction ${index + 1}:`, JSON.stringify(interactionItem, null, 2));
    });
    
    await connection.execute('DELETE FROM Interactions');
    
    // Updated column mapping - fixed the ID field mapping
    const columnMapping = {
      'ID': 'id',  // Fixed: was 'interaction_id', should be 'id'
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
    
    // Handle duplicate column names dynamically
    const csvKeys = Object.keys(interactionsData[0] || {});
    console.log('Checking for duplicate columns...');
    
    // Find all "Wait For Customer" columns
    const waitForCustomerColumns = csvKeys.filter(key => key.includes('Wait For Customer'));
    console.log('Found Wait For Customer columns:', waitForCustomerColumns);
    
    if (waitForCustomerColumns.length > 1) {
      // Map the second occurrence to wait_for_customer_2
      columnMapping[waitForCustomerColumns[1]] = 'wait_for_customer_2';
      console.log(`Mapped duplicate column "${waitForCustomerColumns[1]}" to wait_for_customer_2`);
    }
    
    // Filter column mapping to only include columns that exist in the CSV
    const validColumnMapping = {};
    const missingColumns = [];
    
    Object.keys(columnMapping).forEach(csvCol => {
      if (csvKeys.includes(csvCol)) {
        validColumnMapping[csvCol] = columnMapping[csvCol];
      } else {
        missingColumns.push(csvCol);
      }
    });
    
    if (missingColumns.length > 0) {
      console.log('Missing CSV columns (will be skipped):', missingColumns);
    }
    
    console.log('Valid column mapping:', validColumnMapping);
    
    const dbColumns = Object.values(validColumnMapping);
    const csvColumns = Object.keys(validColumnMapping);
    
    // Verify all database columns exist in the table schema
    try {
      const [tableSchema] = await connection.execute('DESCRIBE Interactions');
      const tableColumns = tableSchema.map(col => col.Field);
      
      const invalidDbColumns = dbColumns.filter(col => !tableColumns.includes(col));
      if (invalidDbColumns.length > 0) {
        console.warn('Database columns not found in table schema:', invalidDbColumns);
      }
    } catch (schemaError) {
      console.warn('Could not verify table schema:', schemaError.message);
    }
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT INTO Interactions (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    console.log(`Preparing to insert ${interactionsData.length} interactions with ${dbColumns.length} columns`);
    console.log('Insert query:', insertQuery);
    
    let successCount = 0;
    let errorCount = 0;
    const errors = [];
    
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
            
            // Skip empty strings
            if (value === '') {
              return null;
            }
            
            const dbColumn = validColumnMapping[csvCol];
            
            // Handle date fields
            if (['answered', 'clicked_at', 'created_at', 'delivered_time', 'ended', 'time_field', 'read_time', 'sent_time', 'started', 'updated_at'].includes(dbColumn)) {
              return convertToMySQLDateTime(value);
            }
            
            // Handle integer fields
            if (['id', 'user_id'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            // Handle decimal fields
            if (['duration', 'time_to_reply', 'time_to_reply_tracked', 'wait_for_agent', 'wait_for_customer', 'wait_for_customer_2'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        await connection.execute(insertQuery, values);
        successCount++;
        
        if (successCount % LOGGING_CONFIG.progressInterval === 0) {
          console.log(`  → Inserted ${successCount} interactions...`);
        }
        
      } catch (error) {
        errorCount++;
        const errorDetail = {
          row: i + 1,
          error: error.message,
          data: errorCount <= 3 ? interactionItem : null // Only log first 3 problematic records
        };
        errors.push(errorDetail);
        
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting interaction row ${i + 1}:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(interactionItem, null, 2));
            console.error('Prepared values:', values);
          }
        }
      }
    }
    
    console.log(`✓ Successfully inserted ${successCount} interactions into database`);
    if (errorCount > 0) {
      console.log(`✗ Failed to insert ${errorCount} interactions`);
      console.log('Error summary:', errors.slice(0, 5)); // Show first 5 errors
    }
    console.log('============================');
    
    return {
      success: errorCount === 0,
      successCount,
      errorCount,
      errors: errors.slice(0, 10) // Return first 10 errors for debugging
    };
    
  } catch (error) {
    console.error('Error handling interactions data:', error);
    throw error;
  }
}

// User State Interactions data handler
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
    
    await connection.execute('DELETE FROM UserStateInteractions');
    
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
    
    // Handle duplicate column names
    const csvKeys = Object.keys(userStateInteractionsData[0] || {});
    const pauseAcdColumns = csvKeys.filter(key => key.includes('User - Pause ACD when Voice Fails'));
    if (pauseAcdColumns.length > 1) {
      columnMapping[pauseAcdColumns[1]] = 'user_pause_acd_when_voice_fails_2';
    }
    
    const dbColumns = Object.values(columnMapping);
    const csvColumns = Object.keys(columnMapping);
    
    const placeholders = dbColumns.map(() => '?').join(', ');
    const insertQuery = `INSERT INTO UserStateInteractions (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    
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
            
            // Handle date fields
            if (['start_at', 'created_at', 'updated_at', 'conversation_ended_at', 'conversation_answer_at', 'conversation_start_at'].includes(dbColumn)) {
              return convertToMySQLDateTime(value);
            }
            
            // Handle integer fields
            if (['id', 'conversation_id', 'user_id'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            // Handle decimal fields
            if (['user_not_responding_auto_recovery_timeout', 'user_ice_timeout', 'duration'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        await connection.execute(insertQuery, values);
        successCount++;
        
        if (successCount % LOGGING_CONFIG.progressInterval === 0) {
          console.log(`  → Inserted ${successCount} user state interactions...`);
        }
        
      } catch (error) {
        errorCount++;
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting user state interaction row ${successCount + errorCount}:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(userStateItem, null, 2));
          }
        }
      }
    }
    
    console.log(`✓ Successfully inserted ${successCount} user state interactions into database`);
    if (errorCount > 0) {
      console.log(`✗ Failed to insert ${errorCount} user state interactions`);
    }
    console.log('=====================================');
  } catch (error) {
    console.error('Error handling user state interactions data:', error);
    throw error;
  }
}

// Users data handler
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
    
    await connection.execute('DELETE FROM Users');
    
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
    const insertQuery = `INSERT INTO Users (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    
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
            
            // Handle boolean fields
            if (['pbx_user', 'enabled'].includes(dbColumn)) {
              return convertToBoolean(value);
            }
            
            // Handle integer fields
            if (['id'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            // Handle decimal fields
            if (['allow_report_access', 'allow_supervisor_access', 'allow_messaging_access', 'allow_settings_access'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        await connection.execute(insertQuery, values);
        successCount++;
        
        if (successCount % 100 === 0) {
          console.log(`  → Inserted ${successCount} users...`);
        }
        
      } catch (error) {
        errorCount++;
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting user row ${successCount + errorCount}:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(userItem, null, 2));
          }
        }
      }
    }
    
    console.log(`✓ Successfully inserted ${successCount} users into database`);
    if (errorCount > 0) {
      console.log(`✗ Failed to insert ${errorCount} users`);
    }
    console.log('==================');
  } catch (error) {
    console.error('Error handling users data:', error);
    throw error;
  }
}

// User Session History data handler
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
    
    await connection.execute('DELETE FROM UserSessionHistory');
    
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
    const insertQuery = `INSERT INTO UserSessionHistory (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    
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
            
            // Handle date fields
            if (['updated_at', 'created_at', 'user_last_sign_in_at'].includes(dbColumn)) {
              return convertToMySQLDateTime(value);
            }
            
            // Handle integer fields
            if (['id', 'user_id'].includes(dbColumn)) {
              return convertToInt(value);
            }
          }
          
          return value;
        });
        
        await connection.execute(insertQuery, values);
        successCount++;
        
        if (successCount % LOGGING_CONFIG.progressInterval === 0) {
          console.log(`  → Inserted ${successCount} user session history records...`);
        }
        
      } catch (error) {
        errorCount++;
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting user session history row ${successCount + errorCount}:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(sessionItem, null, 2));
          }
        }
      }
    }
    
    console.log(`✓ Successfully inserted ${successCount} user session history records into database`);
    if (errorCount > 0) {
      console.log(`✗ Failed to insert ${errorCount} user session history records`);
    }
    console.log('=====================================');
  } catch (error) {
    console.error('Error handling user session history data:', error);
    throw error;
  }
}

// Schedule data handler
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
    
    await connection.execute('DELETE FROM Schedule');
    
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
    const insertQuery = `INSERT INTO Schedule (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    
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
            
            // Handle integer fields
            if (['id'].includes(dbColumn)) {
              return convertToInt(value);
            }
          }
          
          return value;
        });
        
        await connection.execute(insertQuery, values);
        successCount++;
        
        console.log(`  → Inserted schedule record ${successCount}...`);
        
      } catch (error) {
        errorCount++;
        console.error(`Error inserting schedule row ${successCount + errorCount}:`, error.message);
        console.error('Sample problematic data:', JSON.stringify(scheduleItem, null, 2));
      }
    }
    
    console.log(`✓ Successfully inserted ${successCount} schedule records into database`);
    if (errorCount > 0) {
      console.log(`✗ Failed to insert ${errorCount} schedule records`);
    }
    console.log('=====================');
  } catch (error) {
    console.error('Error handling schedule data:', error);
    throw error;
  }
}

// SLA Policy data handler
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
    
    await connection.execute('DELETE FROM SLAPolicy');
    
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
    const insertQuery = `INSERT INTO SLAPolicy (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    
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
            
            // Handle boolean fields
            if (['enabled', 'escalation_rules_use_schedule', 'reset_targets_on_assignment'].includes(dbColumn)) {
              return convertToBoolean(value);
            }
            
            // Handle integer fields
            if (['id', 'first_reply_target'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            // Handle decimal fields
            if (['escalation_assigned_to', 'escalation_assigned_to_id', 'last_reply_target', 'solved_target'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        await connection.execute(insertQuery, values);
        successCount++;
        
        console.log(`  → Inserted SLA Policy record ${successCount}: ${slaPolicyItem['Name'] || slaPolicyItem['Label'] || 'Unknown'}...`);
        
      } catch (error) {
        errorCount++;
        console.error(`Error inserting SLA Policy row ${successCount + errorCount}:`, error.message);
        console.error('Sample problematic data:', JSON.stringify(slaPolicyItem, null, 2));
      }
    }
    
    console.log(`✓ Successfully inserted ${successCount} SLA Policy records into database`);
    if (errorCount > 0) {
      console.log(`✗ Failed to insert ${errorCount} SLA Policy records`);
    }
    console.log('======================');
  } catch (error) {
    console.error('Error handling SLA Policy data:', error);
    throw error;
  }
}

// NOC Interactions data handler
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
    
    await connection.execute('DELETE FROM NOCInteractions');
    
    const columnMapping = {
      'ID': 'id', // Use CSV ID as primary key
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
    const insertQuery = `INSERT INTO NOCInteractions (${dbColumns.join(', ')}) VALUES (${placeholders})`;
    
    let successCount = 0;
    let errorCount = 0;
    
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
            
            // Handle date fields
            if (['answered', 'completed', 'created_at', 'ended', 'started', 'updated_at', 'read_time'].includes(dbColumn)) {
              return convertToMySQLDateTime(value);
            }
            
            // Handle boolean fields
            if (['auto_answer', 'disposition_required', 'on_hold', 'pending_outcome', 'tracked'].includes(dbColumn)) {
              return convertToBoolean(value);
            }
            
            // Handle integer fields
            if (['id', 'conversation_id', 'created_by_id', 'department_id', 'disposition_id', 'user_id', 'contact_id'].includes(dbColumn)) {
              return convertToInt(value);
            }
            
            // Handle decimal fields
            if (['time_to_reply', 'wait_for_agent', 'wait_for_customer', 'handle_time', 'duration', 'b_leg_interaction_csat', 'b_leg_interaction_time_to_reply', 'b_leg_interaction_time_to_reply_2'].includes(dbColumn)) {
              return convertToDecimal(value);
            }
          }
          
          return value;
        });
        
        await connection.execute(insertQuery, values);
        successCount++;
        
        if (successCount % 50 === 0) {
          console.log(`  → Inserted ${successCount} NOC interactions...`);
        }
        
      } catch (error) {
        errorCount++;
        if (errorCount <= LOGGING_CONFIG.maxErrorLogs) {
          console.error(`Error inserting NOC interaction row ${successCount + errorCount}:`, error.message);
          if (errorCount === 1) {
            console.error('Sample problematic data:', JSON.stringify(nocInteractionItem, null, 2));
          }
        }
      }
    }
    
    console.log(`✓ Successfully inserted ${successCount} NOC interactions into database`);
    if (errorCount > 0) {
      console.log(`✗ Failed to insert ${errorCount} NOC interactions`);
    }
    console.log('=============================');
  } catch (error) {
    console.error('Error handling NOC interactions data:', error);
    throw error;
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