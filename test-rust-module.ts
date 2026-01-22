import { NativeModules } from 'react-native';
const { TimonModule } = NativeModules;

(async () => {
  try {
    const BASE_PATH = '/data/data/com.rustexample/files/timon';
    let init_result = await TimonModule.nativeInitTimon(BASE_PATH, 1440, 'ahmed_testuser');
    console.log('initTimon Result:', init_result);
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
    const result = await TimonModule.nativeCreateDatabase(dbName);
    console.info(result, 'createDatabase');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling createDatabase: ', error);
  }
};

export const createTable = async (dbName: string, tableName: string, _schema?: String) => {
  try {
    const result = await TimonModule.nativeCreateTable(dbName, tableName, _schema);
    console.info(result, 'createTable');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling createTable: ', error);
  }
};

export const listDatabases = async () => {
  try {
    const result = await TimonModule.nativeListDatabases();
    console.info(result, 'listDatabases');
    const json_value = parseJson(result).json_value;
    return json_value;
  } catch(error) {
    console.error('Error calling listDatabases: ', error);
  }
};

export const listTables = async (dbName: string) => {
  try {
    const result = await TimonModule.nativeListTables(dbName);
    console.info(result, 'listTables');
    const json_value = parseJson(result).json_value;
    return json_value;
  } catch(error) {
    console.error('Error calling listTables: ', error);
  }
};

export const deleteDatabase = async (dbName: string) => {
  try {
    const result = await TimonModule.nativeDeleteDatabase(dbName);
    console.info(result, 'deleteDatabase');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling deleteDatabase: ', error);
  }
};

export const deleteTable = async (dbName: string, tableName: string) => {
  try {
    const result = await TimonModule.nativeDeleteTable(dbName, tableName);
    console.info(result, 'deleteTable');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling deleteTable: ', error);
  }
};

export const query = async (dbName: string, sqlQuery: string, userName: string | null, limitPartitions: number = 0) => {
  try {
    const result = await TimonModule.nativeQuery(dbName, sqlQuery, userName, limitPartitions);
    console.info(result, 'query');
    const json_value = parseJson(result).json_value;
    return json_value;
  } catch (error) {
    console.error('Error calling query: ', error);
  }
};


export const insert = async (dbName: string, tableName: string, jsonData: Array<object>) => {
  try {
    const result = await TimonModule.nativeInsert(dbName, tableName, JSON.stringify(jsonData));
    console.info(result, 'insert');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling insert: ', error);
  }
};

// ******************************** S3 Compatible Storage ********************************

/**
 * Initialize bucket using credentials from secure storage (Android Keystore)
 * This is the recommended method as credentials are stored securely.
 */
export const initBucketFromSecureStorage = async () => {
  try {
    const result = await TimonModule.initBucketFromSecureStorage();
    console.info(result, 'initBucketFromSecureStorage');
    return result;
  } catch(error) {
    console.error('Error calling initBucketFromSecureStorage: ', error);
    throw error;
  }
};

/**
 * Store credentials securely in Android Keystore
 * This should only be called during initial setup or credential rotation.
 * Credentials are encrypted using AES-256-GCM and stored in Android Keystore.
 */
export const storeCredentialsSecurely = async (
  bucket_endpoint: string,
  bucket_name: string,
  access_key_id: string,
  secret_access_key: string,
  bucket_region: string
) => {
  try {
    const result = await TimonModule.storeCredentialsSecurely(
      bucket_endpoint,
      bucket_name,
      access_key_id,
      secret_access_key,
      bucket_region
    );
    console.info(result, 'storeCredentialsSecurely');
    return result;
  } catch(error) {
    console.error('Error calling storeCredentialsSecurely: ', error);
    throw error;
  }
};

/**
 * Fetch AWS credentials from secure backend server
 * @param serverUrl - Base URL of the credential server (e.g., 'http://localhost:3000' or 'https://api.example.com')
 * @param apiKey - API key for authentication
 * @returns Promise with credentials object
 */
export const fetchCredentialsFromServer = async (
  serverUrl: string,
  apiKey: string
): Promise<{
  bucketEndpoint: string;
  bucketName: string;
  accessKeyId: string;
  secretAccessKey: string;
  bucketRegion: string;
}> => {
  try {
    const url = `${serverUrl.replace(/\/$/, '')}/api/aws-credentials`;
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'X-API-Key': apiKey,
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(
        errorData.message || `Server returned ${response.status}: ${response.statusText}`
      );
    }

    const data = await response.json();

    if (!data.success || !data.credentials) {
      throw new Error('Invalid response format from server');
    }

    const { credentials } = data;

    // Validate all required fields are present
    const required = ['bucketEndpoint', 'bucketName', 'accessKeyId', 'secretAccessKey', 'bucketRegion'];
    const missing = required.filter(field => !credentials[field]);

    if (missing.length > 0) {
      throw new Error(`Missing credentials: ${missing.join(', ')}`);
    }

    return credentials;
  } catch (error: any) {
    console.error('Error fetching credentials from server:', error);
    throw new Error(`Failed to fetch credentials: ${error.message}`);
  }
};

/**
 * Fetch credentials from server and store them securely
 * @param serverUrl - Base URL of the credential server
 * @param apiKey - API key for authentication
 */
export const fetchAndStoreCredentials = async (
  serverUrl: string,
  apiKey: string
): Promise<void> => {
  try {
    console.log('📡 Fetching credentials from server...');
    const credentials = await fetchCredentialsFromServer(serverUrl, apiKey);

    console.log('🔒 Storing credentials securely...');
    await storeCredentialsSecurely(
      credentials.bucketEndpoint,
      credentials.bucketName,
      credentials.accessKeyId,
      credentials.secretAccessKey,
      credentials.bucketRegion
    );

    console.log('✅ Credentials fetched and stored successfully');
  } catch (error: any) {
    console.error('❌ Failed to fetch and store credentials:', error);
    throw error;
  }
};

/**
 * Initialize bucket with credentials (legacy method - use initBucketFromSecureStorage instead)
 * WARNING: This method passes credentials in plain text through the JNI layer.
 * Only use this for testing or migration purposes.
 * @deprecated Use initBucketFromSecureStorage() instead
 */
export const initBucket = async (bucket_endpoint: string, bucket_name: string, access_key_id: string, secret_access_key: string, bucket_region: string) => {
  try {
    const result = await TimonModule.nativeInitBucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key, bucket_region);
    console.info(result, 'initBucket');
    return result;
  } catch(error) {
    console.error('Error calling initBucket: ', error);
    throw error;
  }
};


export const cloudSinkParquet = async (dbName: String, tableName: String) => {
  try {
    const result = await TimonModule.nativeCloudSinkParquet(dbName, tableName);
    console.info(result, 'cloudSinkParquet');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling cloudSinkParquet: ', error);
  }
};

export const cloudFetchParquet = async (userName: String, dbName: String, tableName: String, dateRange: { start: string, end: string }) => {
  try {
    const result = await TimonModule.nativeCloudFetchParquet(userName, dbName, tableName, dateRange);
    console.info(result, 'cloudFetchParquet');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling cloudFetchParquet: ', error);
  }
};

export const cloudFetchParquetBatch = async (usernames: string[], dbNames: string[], tableNames: string[], dateRange: { start: string, end: string }) => {
  try {
    const result = await TimonModule.nativeCloudFetchParquetBatch(usernames, dbNames, tableNames, dateRange);
    console.info(result, 'cloudFetchParquetBatch');
    return parseJson(result);
  } catch(error) {
    console.error('Error calling cloudFetchParquetBatch: ', error);
  }
};
