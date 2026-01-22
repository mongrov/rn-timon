package com.rustexample

import android.content.Context
import android.security.keystore.KeyGenParameterSpec
import android.security.keystore.KeyProperties
import android.util.Base64
import java.security.KeyStore
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.GCMParameterSpec

/**
 * Secure credential manager using Android Keystore for storing AWS credentials.
 * Credentials are encrypted using AES-256-GCM and stored in Android Keystore.
 */
class SecureCredentialManager private constructor(context: Context) {
    private val keyStore: KeyStore = KeyStore.getInstance("AndroidKeyStore").apply {
        load(null)
    }
    
    private val alias = "aws_credentials_key"
    private val sharedPrefs = context.getSharedPreferences("secure_credentials", Context.MODE_PRIVATE)
    
    companion object {
        @Volatile
        private var INSTANCE: SecureCredentialManager? = null
        
        fun getInstance(context: Context): SecureCredentialManager {
            return INSTANCE ?: synchronized(this) {
                INSTANCE ?: SecureCredentialManager(context.applicationContext).also { INSTANCE = it }
            }
        }
    }
    
    /**
     * Initialize the encryption key if it doesn't exist
     */
    private fun initKey() {
        if (!keyStore.containsAlias(alias)) {
            val keyGenerator = KeyGenerator.getInstance(KeyProperties.KEY_ALGORITHM_AES, "AndroidKeyStore")
            val keyGenParameterSpec = KeyGenParameterSpec.Builder(
                alias,
                KeyProperties.PURPOSE_ENCRYPT or KeyProperties.PURPOSE_DECRYPT
            )
                .setBlockModes(KeyProperties.BLOCK_MODE_GCM)
                .setEncryptionPaddings(KeyProperties.ENCRYPTION_PADDING_NONE)
                .setKeySize(256)
                .build()
            
            keyGenerator.init(keyGenParameterSpec)
            keyGenerator.generateKey()
        }
    }
    
    /**
     * Store AWS credentials securely
     */
    fun storeCredentials(
        bucketEndpoint: String,
        bucketName: String,
        accessKeyId: String,
        secretAccessKey: String,
        bucketRegion: String
    ): Boolean {
        return try {
            initKey()
            
            val secretKey = keyStore.getKey(alias, null) as SecretKey
            
            // Encrypt each credential separately (create new cipher for each)
            val encryptedEndpoint = encryptWithNewCipher(secretKey, bucketEndpoint)
            val encryptedBucketName = encryptWithNewCipher(secretKey, bucketName)
            val encryptedAccessKey = encryptWithNewCipher(secretKey, accessKeyId)
            val encryptedSecretKey = encryptWithNewCipher(secretKey, secretAccessKey)
            val encryptedRegion = encryptWithNewCipher(secretKey, bucketRegion)
            
            // Store encrypted values and IV
            val success = sharedPrefs.edit().apply {
                putString("encrypted_endpoint", encryptedEndpoint.first)
                putString("iv_endpoint", encryptedEndpoint.second)
                putString("encrypted_bucket_name", encryptedBucketName.first)
                putString("iv_bucket_name", encryptedBucketName.second)
                putString("encrypted_access_key_id", encryptedAccessKey.first)
                putString("iv_access_key_id", encryptedAccessKey.second)
                putString("encrypted_secret_access_key", encryptedSecretKey.first)
                putString("iv_secret_access_key", encryptedSecretKey.second)
                putString("encrypted_region", encryptedRegion.first)
                putString("iv_region", encryptedRegion.second)
            }.commit()
            
            if (!success) {
                android.util.Log.e("SecureCredentialManager", "Failed to commit SharedPreferences")
            }
            
            success
        } catch (e: Exception) {
            android.util.Log.e("SecureCredentialManager", "Error storing credentials: ${e.message}", e)
            e.printStackTrace()
            false
        }
    }
    
    /**
     * Encrypt data with a new cipher instance
     */
    private fun encryptWithNewCipher(secretKey: SecretKey, plaintext: String): Pair<String, String> {
        val cipher = Cipher.getInstance("AES/GCM/NoPadding")
        cipher.init(Cipher.ENCRYPT_MODE, secretKey)
        return encrypt(cipher, plaintext)
    }
    
    /**
     * Retrieve AWS credentials securely
     */
    fun retrieveCredentials(): Credentials? {
        return try {
            if (!keyStore.containsAlias(alias)) {
                return null
            }
            
            val secretKey = keyStore.getKey(alias, null) as SecretKey
            
            val encryptedEndpoint = sharedPrefs.getString("encrypted_endpoint", null)
            val ivEndpoint = sharedPrefs.getString("iv_endpoint", null)
            val encryptedBucketName = sharedPrefs.getString("encrypted_bucket_name", null)
            val ivBucketName = sharedPrefs.getString("iv_bucket_name", null)
            val encryptedAccessKey = sharedPrefs.getString("encrypted_access_key_id", null)
            val ivAccessKey = sharedPrefs.getString("iv_access_key_id", null)
            val encryptedSecretKey = sharedPrefs.getString("encrypted_secret_access_key", null)
            val ivSecretKey = sharedPrefs.getString("iv_secret_access_key", null)
            val encryptedRegion = sharedPrefs.getString("encrypted_region", null)
            val ivRegion = sharedPrefs.getString("iv_region", null)
            
            if (encryptedEndpoint == null || ivEndpoint == null ||
                encryptedBucketName == null || ivBucketName == null ||
                encryptedAccessKey == null || ivAccessKey == null ||
                encryptedSecretKey == null || ivSecretKey == null ||
                encryptedRegion == null || ivRegion == null) {
                return null
            }
            
            Credentials(
                bucketEndpoint = decrypt(secretKey, encryptedEndpoint, ivEndpoint) ?: return null,
                bucketName = decrypt(secretKey, encryptedBucketName, ivBucketName) ?: return null,
                accessKeyId = decrypt(secretKey, encryptedAccessKey, ivAccessKey) ?: return null,
                secretAccessKey = decrypt(secretKey, encryptedSecretKey, ivSecretKey) ?: return null,
                bucketRegion = decrypt(secretKey, encryptedRegion, ivRegion) ?: return null
            )
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }
    
    /**
     * Clear stored credentials
     */
    fun clearCredentials(): Boolean {
        return try {
            with(sharedPrefs.edit()) {
                clear()
                commit()
            }
        } catch (e: Exception) {
            e.printStackTrace()
            false
        }
    }
    
    /**
     * Check if credentials are stored
     */
    fun hasCredentials(): Boolean {
        return sharedPrefs.contains("encrypted_endpoint")
    }
    
    private fun encrypt(cipher: Cipher, plaintext: String): Pair<String, String> {
        val encrypted = cipher.doFinal(plaintext.toByteArray(Charsets.UTF_8))
        val iv = cipher.iv
        return Pair(
            Base64.encodeToString(encrypted, Base64.DEFAULT),
            Base64.encodeToString(iv, Base64.DEFAULT)
        )
    }
    
    private fun decrypt(secretKey: SecretKey, encrypted: String, iv: String): String? {
        return try {
            val cipher = Cipher.getInstance("AES/GCM/NoPadding")
            val gcmSpec = GCMParameterSpec(128, Base64.decode(iv, Base64.DEFAULT))
            cipher.init(Cipher.DECRYPT_MODE, secretKey, gcmSpec)
            val decrypted = cipher.doFinal(Base64.decode(encrypted, Base64.DEFAULT))
            String(decrypted, Charsets.UTF_8)
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }
    
    data class Credentials(
        val bucketEndpoint: String,
        val bucketName: String,
        val accessKeyId: String,
        val secretAccessKey: String,
        val bucketRegion: String
    )
}
