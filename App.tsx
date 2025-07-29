import React, { useState } from 'react';
import { StyleSheet, Text, View, FlatList, Button, StatusBar, Alert } from 'react-native';
import {
  createDatabase,
  createTable,
  listDatabases,
  listTables,
  deleteDatabase,
  deleteTable,
  query,
  insert,
  initBucket,
  cloudSinkParquet,
  cloudFetchParquet,
  getSyncMetadata,
  getAllSyncMetadata,
} from './test-rust-module';

const initBucket_result = initBucket('https://s3.us-west-2.amazonaws.com', 'zivaoneapp', 'xx', 'xx', 'us-west-2');
console.log('initBucket Result: ', initBucket_result);

export default function App() {
  const [temperatureList, setTemperatureList] = useState([]);
  const [onProcess, setOnProcess] = useState(false);

  const [databasesList, setDatabasesList] = useState([]);
  const [tablesList, setTablesList] = useState([]);
  const dbName = 'test';
  const userName = 'rntimon123';
  const RECORDS_COUNT = 100_000;

  const onRefreshTemperatureList = async () => {
    setOnProcess(true);
    try {
      const startTime = new Date(); // ************************************ startTime ************************************

      // const results = await Promise.all([
      //   query(dbName, 'SELECT * FROM temperature ORDER BY date DESC LIMIT 25', null),
      //   query(dbName, 'SELECT * FROM temperature ORDER BY date ASC LIMIT 25', null),
      //   query(dbName, 'SELECT AVG(humidity) FROM temperature', null),
      //   query(dbName, 'SELECT MIN(humidity) FROM temperature', null),
      //   query(dbName, 'SELECT MAX(humidity) FROM temperature', null),
      // ]);

      const sqlQuery = 'SELECT * FROM temperature ORDER BY date DESC LIMIT 25';
      const localTemperatureData = await query(dbName, sqlQuery, null);

      const endTime = new Date(); // ************************************ endTime ************************************
      const durationSeconds = (endTime.getTime() - startTime.getTime()) / 1000;
      console.log(`Execution time: ${durationSeconds.toFixed(3)} seconds`);

      setTemperatureList(localTemperatureData);
    } catch (error) {
      console.error('Error fetching temperature list:', error);
    }
    setOnProcess(false);
  };

  const insertRandomTempData = async () => {
    setOnProcess(true);
    await new Promise((res) => setTimeout(res, 500));
    const currentTime = Date.now();
    const randomData = Array.from({ length: RECORDS_COUNT }, (_, index) => { 
      return {
        date: new Date(currentTime + (index * 1000)).toISOString(),
        temperature: 80 + Math.random() * 20,
        humidity: 40 + Math.random() * 20,
      };
    });

    try {
      await insert(dbName, 'temperature', randomData);
    } catch (error) {
      console.error('Error inserting data:', error);
    }
    setOnProcess(false);
  };

  const cloudSinkData = async () => {
    setOnProcess(true);
    try {
      const response = await cloudSinkParquet(dbName, 'temperature');
      console.info(response);
    } catch (error) {
      console.error('Error sinking monthly data:', error);
    }
    setOnProcess(false);
  };

  const cloudFetchUserData = async () => {
    setOnProcess(true);
    try {
      const dateRange = { start: '2025-02-01', end: '2025-02-28' };
      const response = await cloudFetchParquet(userName, dbName, 'temperature', dateRange);
      console.info(response);
    } catch (error) {
      console.error('Error sinking monthly data:', error);
    }
    setOnProcess(false);
  };

  const testGetSyncMetadata = async () => {
    setOnProcess(true);
    try {
      const response = await getSyncMetadata(dbName, 'temperature');
      console.info('Sync Metadata for temperature table:', response);
      Alert.alert('Sync Metadata', `Sync Metadata: ${JSON.stringify(response, null, 2)}`);
    } catch (error) {
      console.error('Error getting sync metadata:', error);
    }
    setOnProcess(false);
  };

  const testGetAllSyncMetadata = async () => {
    setOnProcess(true);
    try {
      const response = await getAllSyncMetadata(dbName);
      console.info('All Sync Metadata for database:', response);
      Alert.alert('All Sync Metadata', `All Sync Metadata: ${JSON.stringify(response, null, 2)}`);
    } catch (error) {
      console.error('Error getting all sync metadata:', error);
    }
    setOnProcess(false);
  };

  const renderTemperatureItem = ({ item }: any) => (
    <View style={styles.item}>
      <Text style={styles.timestamp}>
        {new Date(item.date * 1000).toLocaleDateString()} {new Date(item.date * 1000).toLocaleTimeString()}
      </Text>
      <Text>
        Temp: {item.temperature.toFixed(2)} | Humidity: {item.humidity.toFixed(2)}
      </Text>
    </View>
  );

  const renderDBItem = ({ item }: any) => <Text>{item}</Text>;
  const renderTableItem = ({ item }: any) => <Text>{item}</Text>;

  return (
    <View style={styles.container}>
      <View style={{ marginBottom: 20 }}>
        <Button
          title="Refresh Temperature List"
          onPress={onRefreshTemperatureList}
          disabled={onProcess}
        />
      </View>
      <Button
        title="Insert Random Temperature"
        color="red"
        onPress={insertRandomTempData}
        disabled={onProcess}
      />
      <FlatList
        data={temperatureList}
        renderItem={renderTemperatureItem}
        keyExtractor={(_: any, index) => index.toString()}
      />

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
      <Button
        title='Create "temperature" Table'
        color="gray"
        onPress={async () => {
          try {
            const schema = JSON.stringify({
              date: { type: 'int', required: true, unique: true, datetime: true},
              temperature: { type: 'float', required: true },
              humidity: { type: 'float', required: true },
              status: { type: 'string', required: false },
            });
            await createTable(dbName, 'temperature', schema);
          } catch (error) {
            console.error('Error creating table:', error);
          }
        }}
      />
      <Text>*************** list dbs and tables ***************</Text>

      <Button
        title={`List ${dbName} databases`}
        color="orange"
        onPress={async () => {
          try {
            const dbList = await listDatabases();
            setDatabasesList(dbList);
          } catch (error) {
            console.error('Error listing databases:', error);
          }
        }}
      />
      <FlatList
        data={databasesList}
        renderItem={renderDBItem}
        keyExtractor={(_, index) => index.toString()}
      />

      <Button
        title={`List ${dbName} tables`}
        color="purple"
        onPress={async () => {
          try {
            const tableList = await listTables(dbName);
            setTablesList(tableList);
          } catch (error) {
            console.error('Error listing tables:', error);
          }
        }}
      />
      <FlatList
        data={tablesList}
        renderItem={renderTableItem}
        keyExtractor={(_, index) => index.toString()}
      />

      <Text>*************** delete dbs and tables ***************</Text>
      <Button
        title='Delete "iot" table'
        color="grey"
        onPress={async () => {
          try {
            const result = await deleteTable(dbName, 'iot');
            console.info(result);
          } catch (error) {
            console.error('Error deleting table:', error);
          }
        }}
      />
      <Button
        title={`Delete ${dbName} database`}
        color="green"
        onPress={async () => {
          try {
            await deleteDatabase(dbName);
          } catch (error) {
            console.error('Error deleting database:', error);
          }
        }}
      />

      <Button
        title="Sink data to S3"
        color="black"
        onPress={cloudSinkData}
        disabled={onProcess}
      />

      <Button
        title="fetch data from S3"
        color="pink"
        onPress={cloudFetchUserData}
        disabled={onProcess}
      />

      <Text>*************** Sync Metadata Testing ***************</Text>
      <Button
        title="Get Sync Metadata (temperature table)"
        color="blue"
        onPress={testGetSyncMetadata}
        disabled={onProcess}
      />
      <Button
        title="Get All Sync Metadata (database)"
        color="teal"
        onPress={testGetAllSyncMetadata}
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
  timestamp: {
    fontWeight: 'bold',
    marginBottom: 5,
  },
});
