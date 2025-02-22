import { NativeModules } from 'react-native';
const { TimonModule } = NativeModules;

(async () => {
  try {
    const BASE_PATH = '/data/data/com.rustexample/files/timon';
    await TimonModule.initTimon(BASE_PATH, 5);
  } catch(error) {
    console.error('Error initializing timon:', error);
  }
})();

export const parseJson = (obj: any) => {
  try {
    if (!obj) {throw Error('Null response');}
    return JSON.parse(obj);
  } catch (err) {
    console.error('Error in parseJson: ', err);
    return {};
  }
};

// ******************************** File Storage ********************************
export const createDatabase = async (dbName: string): Promise<string|undefined> => {
  try {
    const result = await TimonModule.createDatabase(dbName);
    console.info(result, 'createDatabase');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling createDatabase: ', error);
  }
};

export const createTable = async (dbName: string, tableName: string, _schema?: String) => {
  try {
    const result = await TimonModule.createTable(dbName, tableName, _schema);
    console.info(result, 'createTable');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling createTable: ', error);
  }
};

export const listDatabases = async () => {
  try {
    const result = await TimonModule.listDatabases();
    console.info(result, 'listDatabases');
    const json_value = parseJson(result).json_value;
    return json_value;
  } catch(error) {
    console.error('Error calling listDatabases: ', error);
  }
};

export const listTables = async (dbName: string) => {
  try {
    const result = await TimonModule.listTables(dbName);
    console.info(result, 'listTables');
    const json_value = parseJson(result).json_value;
    return json_value;
  } catch(error) {
    console.error('Error calling listTables: ', error);
  }
};

export const deleteDatabase = async (dbName: string) => {
  try {
    const result = await TimonModule.deleteDatabase(dbName);
    console.info(result, 'deleteDatabase');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling deleteDatabase: ', error);
  }
};

export const deleteTable = async (dbName: string, tableName: string) => {
  try {
    const result = await TimonModule.deleteTable(dbName, tableName);
    console.info(result, 'deleteTable');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling deleteTable: ', error);
  }
};

export const query = async (dbName: string, sqlQuery: string, userName: string | null) => {
  try {
    const result = await TimonModule.query(dbName, sqlQuery, userName);
    console.info(result, 'query');
    const json_value = parseJson(result).json_value;
    return json_value;
  } catch (error) {
    console.error('Error calling query: ', error);
  }
};

export const insert = async (dbName: string, tableName: string, jsonData: Array<object>) => {
  try {
    const result = await TimonModule.insert(dbName, tableName, JSON.stringify(jsonData));
    console.info(result, 'insert');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling insert: ', error);
  }
};

// ******************************** S3 Compatible Storage ********************************
export const initBucket = async (bucket_endpoint: string, bucket_name: string, access_key_id: string, secret_access_key: string, bucket_region: string) => {
  try {
    const result = await TimonModule.initBucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key, bucket_region);
    console.info(result, 'initBucket');
    return result;
  } catch(error) {
    console.error('Error calling initBucket: ', error);
  }
};

export const queryBucket = async (userName: string, dbName: string, sqlQuery: string, dateRange: { start: string, end: string }) => {
  try {
    const result = await TimonModule.queryBucket(userName, dbName, sqlQuery, dateRange);
    console.info(result, 'queryBucket');
    const json_value = parseJson(result).json_value;
    return json_value;
  } catch (error) {
    console.error('Error calling queryBucket: ', error);
  }
};

export const cloudSinkParquet = async (userName: String, dbName: String, tableName: String) => {
  try {
    const result = await TimonModule.cloudSinkParquet(userName, dbName, tableName);
    console.info(result, 'cloudSinkParquet');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling cloudSinkParquet: ', error);
  }
};

export const cloudFetchParquet = async (userName: String, dbName: String, tableName: String, dateRange: { start: string, end: string }) => {
  try {
    const result = await TimonModule.cloudFetchParquet(userName, dbName, tableName, dateRange);
    console.info(result, 'cloudFetchParquet');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling cloudFetchParquet: ', error);
  }
};
