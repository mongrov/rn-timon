package com.rustexample

import com.facebook.react.bridge.*
import com.facebook.react.module.annotations.ReactModule

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
    external fun initTimon(storagePath: String, bucketInterval: Int): String
    external fun createDatabase(dbName: String): String
    external fun createTable(dbName: String, tableName: String, schema: String): String
    external fun listDatabases(): String
    external fun listTables(dbName: String): String
    external fun deleteDatabase(dbName: String): String
    external fun deleteTable(dbName: String, tableName: String): String
    external fun insert(dbName: String, tableName: String, jsonData: String): String
    external fun query(dbName: String, sqlQuery: String, userName: String?): String


    // ******************************** S3 Compatible Storage ********************************
    external fun initBucket(bucket_endpoint: String, bucket_name: String, access_key_id: String, secret_access_key: String, bucket_region: String): String
    external fun queryBucket(userName: String, dbName: String, sqlQuery: String, dateRange: Map<String, String>): String
    external fun cloudSinkParquet(userName: String, dbName: String, tableName: String): String
    external fun cloudFetchParquet(userName: String, dbName: String, tableName: String, dateRange: Map<String, String>): String

    override fun getName(): String {
        return NAME
    }

    // ******************************** File Storage Methods ********************************

    @ReactMethod
    fun initTimon(storagePath: String, bucketInterval: Int, promise: Promise) {
        try {
            val result = initTimon(storagePath, bucketInterval)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error initializing Timon", e)
        }
    }

    @ReactMethod
    fun createDatabase(dbName: String, promise: Promise) {
        try {
            val result = createDatabase(dbName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error creating database", e)
        }
    }

    @ReactMethod
    fun createTable(dbName: String, tableName: String, schema: String, promise: Promise) {
        try {
            val result = createTable(dbName, tableName, schema)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error creating table", e)
        }
    }

    @ReactMethod
    fun listDatabases(promise: Promise) {
        try {
            val result = listDatabases()
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error listing databases", e)
        }
    }

    @ReactMethod
    fun listTables(dbName: String, promise: Promise) {
        try {
            val result = listTables(dbName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error listing tables", e)
        }
    }

    @ReactMethod
    fun deleteDatabase(dbName: String, promise: Promise) {
        try {
            val result = deleteDatabase(dbName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error deleting database", e)
        }
    }

    @ReactMethod
    fun deleteTable(dbName: String, tableName: String, promise: Promise) {
        try {
            val result = deleteTable(dbName, tableName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error deleting table", e)
        }
    }

    @ReactMethod
    fun insert(dbName: String, tableName: String, jsonData: String, promise: Promise) {
        try {
            val result = insert(dbName, tableName, jsonData)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error inserting data", e)
        }
    }

    @ReactMethod
    fun query(dbName: String, sqlQuery: String, userName: String?, promise: Promise) {
        try {
            val result = query(dbName, sqlQuery, userName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error querying", e)
        }
    }

    // ******************************** S3 Compatible Storage Methods ********************************

    @ReactMethod
    fun initBucket(bucket_endpoint: String, bucket_name: String, access_key_id: String, secret_access_key: String, bucket_region: String, promise: Promise) {
        try {
            val result = initBucket(bucket_endpoint, bucket_name, access_key_id, secret_access_key, bucket_region)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error initializing bucket", e)
        }
    }

    @ReactMethod
    fun queryBucket(userName: String, dbName: String, sqlQuery: String, dateRange: ReadableMap?, promise: Promise) {
        try {
            val rustDateRange = if (dateRange != null && dateRange.hasKey("start") && dateRange.hasKey("end")) {
                mapOf(
                    "start" to dateRange.getString("start")!!,
                    "end" to dateRange.getString("end")!!
                )
            } else {
                mapOf("start" to "1970-01-01", "end" to "1970-01-02")
            }
            val result = queryBucket(userName, dbName, sqlQuery, rustDateRange)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error querying bucket", e)
        }
    }

    @ReactMethod
    fun cloudSinkParquet(userName: String, dbName: String, tableName: String, promise: Promise) {
        try {
            val result = cloudSinkParquet(userName, dbName, tableName)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error sinking monthly parquet", e)
        }
    }

    @ReactMethod
    fun cloudFetchParquet(userName: String, dbName: String, tableName: String, dateRange: ReadableMap?, promise: Promise) {
        try {
            val rustDateRange = if (dateRange != null && dateRange.hasKey("start") && dateRange.hasKey("end")) {
                mapOf(
                    "start" to dateRange.getString("start")!!,
                    "end" to dateRange.getString("end")!!
                )
            } else {
                mapOf("start" to "1970-01-01", "end" to "1970-01-02")
            }
            val result = cloudFetchParquet(userName, dbName, tableName, rustDateRange)
            promise.resolve(result)
        } catch (e: Exception) {
            promise.reject("Error sinking monthly parquet", e)
        }
    }
}
