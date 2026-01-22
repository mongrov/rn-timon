package com.rustexample

import com.facebook.react.bridge.*
import com.facebook.react.module.annotations.ReactModule
import java.lang.reflect.Method
import android.os.Debug

@ReactModule(name = TimonModule.NAME)
class TimonModule(reactContext: ReactApplicationContext) : ReactContextBaseJavaModule(reactContext) {

    companion object {
        const val NAME = "TimonModule"

        // Load the native libraries
        init {
            try {
                System.loadLibrary("tsdb_timon")
            } catch (e: UnsatisfiedLinkError) {
                e.printStackTrace()
            }
        }
    }

    // ******************************** File Storage ********************************
    external fun nativeInitTimon(storagePath: String, bucketInterval: Int, userName: String): String
    external fun nativeCreateDatabase(dbName: String): String
    external fun nativeCreateTable(dbName: String, tableName: String, schema: String): String
    external fun nativeListDatabases(): String
    external fun nativeListTables(dbName: String): String
    external fun nativeDeleteDatabase(dbName: String): String
    external fun nativeDeleteTable(dbName: String, tableName: String): String
    external fun nativeInsert(dbName: String, tableName: String, jsonData: String): String
    external fun nativeQuery(dbName: String, sqlQuery: String, userName: String?, limitPartitions: Int): String


    // ******************************** S3 Compatible Storage ********************************
    external fun nativeInitBucket(bucket_endpoint: String, bucket_name: String, access_key_id: String, secret_access_key: String, bucket_region: String): String
    external fun nativeCloudSyncParquet(dbName: String, tableName: String, dateRange: Map<String, String>, userName: String?): String
    external fun nativeCloudSinkParquet(dbName: String, tableName: String): String
    external fun nativeCloudFetchParquet(userName: String, dbName: String, tableName: String, dateRange: Map<String, String>): String
    external fun nativeCloudFetchParquetBatch(usernames: Array<String>, dbNames: Array<String>, tableNames: Array<String>, dateRange: Map<String, String>): String

    // ******************************** Sync Metadata ********************************
    external fun nativeGetSyncMetadata(dbName: String, tableName: String): String
    external fun nativeGetAllSyncMetadata(dbName: String): String

    override fun getName(): String {
        return NAME
    }

    // ******************************** File Storage Methods ********************************

    @ReactMethod
    fun nativeInitTimon(storagePath: String, bucketInterval: Int, userName: String, promise: Promise) {
        try {
            val result = nativeInitTimon(storagePath, bucketInterval, userName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error initializing Timon", e)
        }
    }

    @ReactMethod
    fun nativeCreateDatabase(dbName: String, promise: Promise) {
        try {
            val result = nativeCreateDatabase(dbName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error creating database", e)
        }
    }

    @ReactMethod
    fun nativeCreateTable(dbName: String, tableName: String, schema: String, promise: Promise) {
        try {
            val result = nativeCreateTable(dbName, tableName, schema)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error creating table", e)
        }
    }

    @ReactMethod
    fun nativeListDatabases(promise: Promise) {
        try {
            val result = nativeListDatabases()
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error listing databases", e)
        }
    }

    @ReactMethod
    fun nativeListTables(dbName: String, promise: Promise) {
        try {
            val result = nativeListTables(dbName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error listing tables", e)
        }
    }

    @ReactMethod
    fun nativeDeleteDatabase(dbName: String, promise: Promise) {
        try {
            val result = nativeDeleteDatabase(dbName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error deleting database", e)
        }
    }

    @ReactMethod
    fun nativeDeleteTable(dbName: String, tableName: String, promise: Promise) {
        try {
            val result = nativeDeleteTable(dbName, tableName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error deleting table", e)
        }
    }

    @ReactMethod
    fun nativeInsert(dbName: String, tableName: String, jsonData: String, promise: Promise) {
        try {
            val result = nativeInsert(dbName, tableName, jsonData)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error nativeInserting data", e)
        }
    }

    @ReactMethod
    fun nativeQuery(dbName: String, sqlQuery: String, userName: String?, limitPartitions: Int, promise: Promise) {
        Thread {
            try {
                val result = nativeQuery(dbName, sqlQuery, userName, limitPartitions)
                promise.resolve(result)
            } catch (e: Exception) {
                promise.reject("Error querying", e)
            }
        }.start()
    }


    // ******************************** S3 Compatible Storage Methods ********************************

    @ReactMethod
    fun nativeInitBucket(bucket_endpoint: String, bucket_name: String, access_key_id: String, secret_access_key: String, bucket_region: String, promise: Promise) {
        try {
            // Security check: Detect debugging/tampering
            if (isDebuggingDetected()) {
                promise.reject("SECURITY_ERROR", "Debugging or tampering detected. Operation refused.", null)
                return
            }
            
            val result = nativeInitBucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key, bucket_region)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error initializing bucket", e)
        }
    }
    
    /**
     * Initialize bucket using credentials from secure storage
     */
    @ReactMethod
    fun initBucketFromSecureStorage(promise: Promise) {
        try {
            // Security check: Detect debugging/tampering
            if (isDebuggingDetected()) {
                promise.reject("SECURITY_ERROR", "Debugging or tampering detected. Operation refused.", null)
                return
            }
            
            val credentialManager = SecureCredentialManager.getInstance(reactApplicationContext)
            val credentials = credentialManager.retrieveCredentials()
            
            if (credentials == null) {
                promise.reject("CREDENTIALS_NOT_FOUND", "No credentials found in secure storage. Please store credentials first.", null)
                return
            }
            
            val result = nativeInitBucket(
                credentials.bucketEndpoint,
                credentials.bucketName,
                credentials.accessKeyId,
                credentials.secretAccessKey,
                credentials.bucketRegion
            )
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error initializing bucket from secure storage", e)
        }
    }
    
    /**
     * Store credentials securely in Android Keystore
     */
    @ReactMethod
    fun storeCredentialsSecurely(
        bucket_endpoint: String,
        bucket_name: String,
        access_key_id: String,
        secret_access_key: String,
        bucket_region: String,
        promise: Promise
    ) {
        try {
            android.util.Log.d("TimonModule", "Attempting to store credentials securely...")
            val credentialManager = SecureCredentialManager.getInstance(reactApplicationContext)
            val success = credentialManager.storeCredentials(
                bucket_endpoint,
                bucket_name,
                access_key_id,
                secret_access_key,
                bucket_region
            )
            
            if (success) {
                android.util.Log.d("TimonModule", "Credentials stored successfully")
                promise.resolve("Credentials stored securely")
            } else {
                android.util.Log.e("TimonModule", "Failed to store credentials - check logcat for details")
                promise.reject("STORAGE_ERROR", "Failed to store credentials securely. Check logcat for details.", null)
            }
        } catch (e: Exception) {
            android.util.Log.e("TimonModule", "Exception storing credentials: ${e.message}", e)
            promise.reject("STORAGE_ERROR", "Error storing credentials: ${e.message}", e)
        }
    }
    
    /**
     * Check if debugging or tampering is detected
     */
    private fun isDebuggingDetected(): Boolean {
        // Check if debugger is attached
        if (Debug.isDebuggerConnected()) {
            return true
        }
        
        // Check for Frida (common hooking framework)
        try {
            // Frida detection: Check for frida-server process
            val process = Runtime.getRuntime().exec("ps")
            val reader = process.inputStream.bufferedReader()
            val output = reader.readText()
            if (output.contains("frida-server") || output.contains("frida")) {
                return true
            }
        } catch (e: Exception) {
            // Ignore errors in detection
        }
        
        return false
    }

    @ReactMethod
    fun nativeCloudSinkParquet(dbName: String, tableName: String, promise: Promise) {
        try {
            val result = nativeCloudSinkParquet(dbName, tableName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error sinking monthly parquet", e)
        }
    }

    @ReactMethod
    fun nativeCloudFetchParquet(userName: String, dbName: String, tableName: String, dateRange: ReadableMap?, promise: Promise) {
        try {
            val rustDateRange = if (dateRange != null && dateRange.hasKey("start") && dateRange.hasKey("end")) {
                mapOf(
                    "start" to dateRange.getString("start")!!,
                    "end" to dateRange.getString("end")!!
                )
            } else {
                mapOf("start" to "1970-01-01", "end" to "1970-01-02")
            }
            val result = nativeCloudFetchParquet(userName, dbName, tableName, rustDateRange)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error sinking monthly parquet", e)
        }
    }

    @ReactMethod
    fun nativeCloudFetchParquetBatch(usernames: ReadableArray, dbNames: ReadableArray, tableNames: ReadableArray, dateRange: ReadableMap?, promise: Promise) {
        try {
            // Convert ReadableArray to Array<String>
            val usernamesArray = Array(usernames.size()) { i ->
                usernames.getString(i) ?: ""
            }
            val dbNamesArray = Array(dbNames.size()) { i ->
                dbNames.getString(i) ?: ""
            }
            val tableNamesArray = Array(tableNames.size()) { i ->
                tableNames.getString(i) ?: ""
            }
            
            val rustDateRange = if (dateRange != null && dateRange.hasKey("start") && dateRange.hasKey("end")) {
                mapOf(
                    "start" to dateRange.getString("start")!!,
                    "end" to dateRange.getString("end")!!
                )
            } else {
                mapOf("start" to "1970-01-01", "end" to "1970-01-02")
            }
            
            val result = nativeCloudFetchParquetBatch(usernamesArray, dbNamesArray, tableNamesArray, rustDateRange)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error batch fetching parquet", e)
        }
    }
}
