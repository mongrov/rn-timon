import React, { useState } from 'react';
import { StyleSheet, Text, View, Button, StatusBar, Alert } from 'react-native';
import {
  initBucketFromSecureStorage,
  fetchAndStoreCredentials,
  createDatabase,
  createTable,
  // listDatabases,
  // listTables,
  deleteDatabase,
  deleteTable,
  query,
  insert,
  cloudFetchParquetBatch,
} from './test-rust-module';

// SECURITY: Credentials are now stored securely in Android Keystore
// Initialize bucket using credentials from secure storage
(async () => {
  try {
    // Try to initialize from secure storage
    await initBucketFromSecureStorage();
    console.log('✅ Bucket initialized from secure storage');
  } catch (error: any) {
    console.error('❌ Failed to initialize from secure storage:', error);
    // If credentials not found, handle appropriately
    if (error?.message?.includes('CREDENTIALS_NOT_FOUND') ||
        error?.message?.includes('No credentials found')) {
      console.warn('⚠️  Credentials not found in secure storage.');
      console.warn('📝 To store credentials, call storeCredentialsSecurely() with credentials from your secure backend.');
      console.warn('🔒 In production, fetch credentials from a secure backend API, never hardcode them!');

      // For Android Emulator: use 10.0.2.2 to access host machine's localhost
      // For Physical Device: use your machine's IP address (e.g., http://192.168.1.100:3000)
      const CREDENTIAL_SERVER_URL = 'http://10.0.2.2:3000'; // Android emulator special IP
      const API_KEY = '892c3770f2c79e2c09099420eac8cececbf8940087a8cff595628a4505b49643'; // ⚠️ Get this from secure storage or backend

      // For development: Try fetching from server if configured
      if (CREDENTIAL_SERVER_URL && API_KEY) {
        try {
          console.log('📡 Attempting to fetch credentials from server...');
          await fetchAndStoreCredentials(CREDENTIAL_SERVER_URL, API_KEY);

          // Retry initialization after storing
          await initBucketFromSecureStorage();
          console.log('✅ Bucket initialized successfully from server credentials');
          return;
        } catch (serverError: any) {
          console.warn('⚠️  Failed to fetch from server:', serverError.message);
        }
      }
    }
  }
})();

export default function App() {
  const [onProcess, setOnProcess] = useState(false);

  const dbName = 'zivaring';

  // SPO2 Table Constants
  const DB_NAME = dbName;
  const SPO2_TABLE = 'spo2';
  const TABLES_SCHEMA = {
    spo2: {
      date: {
        type: 'int',
        required: true,
        unique: true,
        datetime: true,
      },
      automaticSpo2Data: {
        type: 'int',
        required: true,
      },
      is_sync: {
        type: 'bool',
      },
    },
  };

  // SPO2 Table Operations
  const createSpo2Table = async () => {
    setOnProcess(true);
    try {
      const result = await createTable(DB_NAME, SPO2_TABLE, JSON.stringify(TABLES_SCHEMA[SPO2_TABLE]));
      console.log('✅ Create SPO2 Table Result:', result);
      Alert.alert('Success', 'SPO2 table created successfully!');
    } catch (error) {
      console.error('❌ Error creating SPO2 table:', error);
      Alert.alert('Error', `Failed to create table: ${error}`);
    }
    setOnProcess(false);
  };

  const insertSpo2Data = async () => {
    setOnProcess(true);
    try {
      const insertResult = await insert(DB_NAME, SPO2_TABLE, [
        { date: '2025.10.23 10:00:00', automaticSpo2Data: 90 },
        { date: '2025.10.23 11:00:00', automaticSpo2Data: 85 },
        { date: '2025.10.24 05:00:17', automaticSpo2Data: 100 },
        { date: '2025.10.24 05:30:17', automaticSpo2Data: 97 },
      ]);
      console.log('✅ Insert Result:', insertResult);
      Alert.alert('Success', 'SPO2 data inserted successfully!');
    } catch (error) {
      console.error('❌ Error inserting SPO2 data:', error);
      Alert.alert('Error', `Failed to insert data: ${error}`);
    }
    setOnProcess(false);
  };

  const deleteSpo2Table = async () => {
    setOnProcess(true);
    try {
      const result = await deleteTable(DB_NAME, SPO2_TABLE);
      console.log('✅ Delete SPO2 Table Result:', result);
      Alert.alert('Success', 'SPO2 table deleted successfully!');
    } catch (error) {
      console.error('❌ Error deleting SPO2 table:', error);
      Alert.alert('Error', `Failed to delete table: ${error}`);
    }
    setOnProcess(false);
  };

  // Test parallel queries with Promise.all to verify race condition fix
  const testParallelQueries = async () => {
    setOnProcess(true);
    try {
      const startTime = new Date();

      const querySQL = `
        SELECT subquery.*,
          TO_CHAR(TO_TIMESTAMP(date)::TIMESTAMP AT TIME ZONE 'UTC', '%Y.%m.%d %H:%M:%S') AS utc_date,
          TO_CHAR(TO_TIMESTAMP(date)::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London', '%Y.%m.%d %H:%M:%S') AS formatted_date,
          TO_CHAR(TO_TIMESTAMP(date)::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London', '%I:%M %P') AS formatted_time
        FROM (
          SELECT * FROM spo2 ORDER BY "date" DESC LIMIT 2
        ) subquery
      `;

      console.log('🔄 Running 2 parallel queries with Promise.all...');

      const [latestResult1, latestResult2] = await Promise.all([
        query(DB_NAME, querySQL, null),
        query(DB_NAME, querySQL, null),
      ]);

      const endTime = new Date();
      const durationSeconds = (endTime.getTime() - startTime.getTime()) / 1000;

      console.log('✅ Result 1:', latestResult1);
      console.log('✅ Result 2:', latestResult2);
      console.log(`⏱️ Execution time: ${durationSeconds.toFixed(3)} seconds`);

    } catch (error) {
      console.error('❌ Error in parallel queries:', error);
      Alert.alert('Error', `Parallel query failed: ${error}`);
    }
    setOnProcess(false);
  };


  const _onPressQueryData = async () => {
    const queryResult = await query(DB_NAME, `SELECT * FROM ${SPO2_TABLE}`, null, 0);
    const latestResult = await query(DB_NAME, `
    SELECT subquery.*,
    TO_CHAR(TO_TIMESTAMP(date)::TIMESTAMP AT TIME ZONE 'UTC', '%Y.%m.%d %H:%M:%S') AS utc_date,
    TO_CHAR(TO_TIMESTAMP(date)::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London', '%Y.%m.%d %H:%M:%S') AS formatted_date,
    TO_CHAR(TO_TIMESTAMP(date)::TIMESTAMP AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/London', '%I:%M %P') as formatted_time
    FROM (
    SELECT * FROM spo2 ORDER BY "date" DESC LIMIT 1
    ) subquery
    `, null, 0);

    console.log('latestResult.status', latestResult);
    console.log('queryResult.status', queryResult);
  };

  // Test cloudFetchParquetBatch for multiple users
  const testCloudFetchParquetBatch = async () => {
    setOnProcess(true);
    try {
      const usernames = ['rADQkoFBr4Pks9Y2H_sriram', '7TQBn6aSe49wfnuox_roshann'];
      const dbNames = ['zivaring'];
      const tableNames = ['activitydetails', 'heartrate', 'hrv_table', 'spo2'];
      const dateRange = {
        start: '2025-01-01',
        end: '2025-12-30',
      };

      console.log('🔄 Testing cloudFetchParquetBatch...');
      console.log('Usernames:', usernames);
      console.log('Tables:', tableNames);
      console.log('Date Range:', dateRange);

      const startTime = new Date();
      const result = await cloudFetchParquetBatch(usernames, dbNames, tableNames, dateRange);
      const endTime = new Date();
      const durationSeconds = (endTime.getTime() - startTime.getTime()) / 1000;

      console.log('✅ cloudFetchParquetBatch Result:', result);
      console.log(`⏱️ Execution time: ${durationSeconds.toFixed(3)} seconds`);

      if (result?.json_value) {
        const jsonValue = result.json_value;
        const successCount = jsonValue.success_count || 0;
        const errorCount = jsonValue.error_count || 0;
        const totalTasks = jsonValue.total_tasks || 0;

        Alert.alert(
          'Cloud Fetch Complete',
          `Success: ${successCount}/${totalTasks}\nErrors: ${errorCount}\nTime: ${durationSeconds.toFixed(2)}s`
        );
      } else {
        Alert.alert('Success', 'cloudFetchParquetBatch completed!');
      }
    } catch (error) {
      console.error('❌ Error in cloudFetchParquetBatch:', error);
      Alert.alert('Error', `cloudFetchParquetBatch failed: ${error}`);
    }
    setOnProcess(false);
  };

  // Test querying data for multiple users
  const testMultiUserQueries = async () => {
    setOnProcess(true);
    try {
      const querySQL = 'SELECT COUNT(*) as total FROM spo2';
      const usernames = [null, 'rADQkoFBr4Pks9Y2H_sriram', '7TQBn6aSe49wfnuox_roshann'];
      const usernameLabels = ['None (default)', 'User1', 'User2'];

      console.log('🔄 Testing queries for multiple users...');
      console.log('Query:', querySQL);

      const startTime = new Date();
      const results = await Promise.all(
        usernames.map(async (username, index) => {
          const result = await query('zivaring', querySQL, username, 0);
          return {
            username: usernameLabels[index],
            usernameValue: username,
            result: result,
          };
        })
      );

      const endTime = new Date();
      const durationSeconds = (endTime.getTime() - startTime.getTime()) / 1000;

      console.log('✅ Multi-User Query Results:');
      results.forEach(({ username, result }) => {
        const count = result?.[0]?.total || 0;
        console.log(`  ${username}: ${count} rows`);
      });
      console.log(`⏱️ Total execution time: ${durationSeconds.toFixed(3)} seconds`);

      // Show results in alert
      const resultSummary = results
        .map(({ username, result }) => {
          const count = result?.[0]?.total || 0;
          return `${username}: ${count} rows`;
        })
        .join('\n');

      Alert.alert(
        'Multi-User Query Results',
        `${resultSummary}\n\nTime: ${durationSeconds.toFixed(2)}s`
      );
    } catch (error) {
      console.error('❌ Error in multi-user queries:', error);
      Alert.alert('Error', `Multi-user query failed: ${error}`);
    }
    setOnProcess(false);
  };

  const _onPressInsert = async () => {
    const tempData = [{ date: '2025.12.15 12:00:00', automaticSpo2Data: 85 }, { date: '2025.12.15 12:05:00', automaticSpo2Data: 90 }];

    const insertResult = await insert(DB_NAME, 'spo2', tempData);
    console.log({ insertResult });

    const queryResult = await query(DB_NAME, 'SELECT * FROM spo2', null, 0);
    console.log({ queryResulttttttt: queryResult });
  };

  return (
    <View style={styles.container}>
      <Button title="Insert Heart Rate Data and Query" onPress={_onPressInsert} />
      <Text>*************** create dbs and tables ***************</Text>
      <Button
        title={`Create ${dbName} Database`}
        color="green"
        onPress={() => {
          try {
            createDatabase(dbName);
          } catch (error) {
            console.error('Error creating database:', error);
          }
        }}
      />

      <Text>*************** delete dbs and tables ***************</Text>
      <Button
        title={`Delete ${dbName} database`}
        color="red"
        onPress={async () => {
          try {
            await deleteDatabase(dbName);
          } catch (error) {
            console.error('Error deleting database:', error);
          }
        }}
      />

      <Text>*************** Check No field named "date" ***************</Text>
      <Button
        title="Check No field named 'date'"
        color="orange"
        onPress={_onPressQueryData}
        disabled={onProcess}
      />

      <Text>*************** SPO2 Parallel Query Test ***************</Text>
      <Button
        title="1. Create SPO2 Table"
        color="green"
        onPress={createSpo2Table}
        disabled={onProcess}
      />
      <Button
        title="2. Insert SPO2 Data"
        color="blue"
        onPress={insertSpo2Data}
        disabled={onProcess}
      />
      <Button
        title="3. Test Parallel Queries (Promise.all)"
        color="purple"
        onPress={testParallelQueries}
        disabled={onProcess}
      />
      <Button
        title="4. Delete SPO2 Table"
        color="red"
        onPress={deleteSpo2Table}
        disabled={onProcess}
      />

      <Text>*************** Cloud Fetch & Multi-User Queries ***************</Text>
      <Button
        title="Test cloudFetchParquetBatch"
        color="teal"
        onPress={testCloudFetchParquetBatch}
        disabled={onProcess}
      />
      <Button
        title="Test Multi-User Queries"
        color="navy"
        onPress={testMultiUserQueries}
        disabled={onProcess}
      />

      <StatusBar barStyle="default" />
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    marginTop: 24,
    flex: 1,
    backgroundColor: '#fff',
    alignItems: 'center',
    justifyContent: 'center',
  },
  item: {
    padding: 10,
    borderBottomWidth: 1,
    borderBottomColor: '#ccc',
    width: '100%',
  },
});
