import React, { useState } from 'react';
import { StyleSheet, Text, View, Button, StatusBar, Alert } from 'react-native';
import {
  createDatabase,
  createTable,
  // listDatabases,
  // listTables,
  // preloadTables,
  deleteDatabase,
  deleteTable,
  query,
  insert,
} from './test-rust-module';

export default function App() {
  const [onProcess, setOnProcess] = useState(false);

  const dbName = 'test';

  // SPO2 Table Constants
  const DB_NAME = dbName;
  const SPO2_TABLE = 'spo2_readings';
  const TABLES_SCHEMA = {
    spo2_readings: {
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
          SELECT * FROM spo2_readings ORDER BY "date" DESC LIMIT 2
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
    SELECT * FROM spo2_readings ORDER BY \"date\" DESC LIMIT 1
    ) subquery
    `, null, 0);

    console.log('latestResult.status', latestResult);
    console.log('queryResult.status', queryResult);
  };

  return (
    <View style={styles.container}>
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
