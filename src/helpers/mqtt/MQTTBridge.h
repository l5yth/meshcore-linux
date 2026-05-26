#pragma once

#include <cstdint>
#include <queue>
#include <string>
#include <vector>
#include <cstring>

namespace mqtt {

/**
 * @brief Hash size support: 1, 2, or 3 bytes
 * Used to identify destination nodes in the mesh network
 */
enum class HashSize : uint8_t {
    BYTES_1 = 1,  ///< Single byte hash (256 destinations)
    BYTES_2 = 2,  ///< 2-byte hash (65536 destinations)
    BYTES_3 = 3   ///< 3-byte hash (16M destinations)
};

/**
 * @brief Represents a message in the MQTT queue
 * 
 * Structure for passing messages between application layer and meshcore.
 * Messages can flow bidirectionally:
 * - Outbound: Application queues -> Meshcore sends via radio
 * - Inbound: Meshcore receives via radio -> Application receives
 */
struct MQTTMessage {
    HashSize hash_size;           ///< Size of destination hash (1, 2, or 3 bytes)
    
    /**
     * @brief Destination hash union
     * 
     * Contains the hash address depending on hash_size:
     * - BYTES_1: use hash_1b (0-255)
     * - BYTES_2: use hash_2b (0-65535, big-endian)
     * - BYTES_3: use hash_3b[3] (0-16777215, big-endian)
     */
    union {
        uint8_t  hash_1b;         ///< 1-byte hash
        uint16_t hash_2b;         ///< 2-byte hash (big-endian)
        uint8_t  hash_3b[3];      ///< 3-byte hash (big-endian: [0]=MSB, [2]=LSB)
    } destination_hash;
    
    uint8_t* payload;             ///< Message content (raw bytes, will be encrypted by meshcore)
    size_t payload_len;           ///< Length of payload in bytes
    uint32_t timestamp;           ///< Timestamp when message was queued (Unix time)
    
    bool is_outbound;             ///< Direction: true=app->meshcore, false=meshcore->app
    
    std::string source_topic;     ///< MQTT topic this message came from (inbound only)
    
    /**
     * @brief Constructor
     */
    MQTTMessage() : hash_size(HashSize::BYTES_1), payload(nullptr), payload_len(0), 
                    timestamp(0), is_outbound(false) {
        memset(&destination_hash, 0, sizeof(destination_hash));
    }
    
    /**
     * @brief Destructor - frees payload if allocated
     */
    ~MQTTMessage() {
        if (payload) {
            delete[] payload;
            payload = nullptr;
        }
    }
    
    /**
     * @brief Set hash from raw bytes with automatic size detection
     * @param hash_data Pointer to hash bytes
     * @param hash_len Length of hash data (1, 2, or 3)
     * @return true if successful, false if invalid length
     */
    bool setHash(const uint8_t* hash_data, size_t hash_len) {
        if (!hash_data || hash_len > 3 || hash_len == 0) {
            return false;
        }
        
        hash_size = static_cast<HashSize>(hash_len);
        memset(&destination_hash, 0, sizeof(destination_hash));
        
        if (hash_len == 1) {
            destination_hash.hash_1b = hash_data[0];
        } else if (hash_len == 2) {
            destination_hash.hash_2b = (hash_data[0] << 8) | hash_data[1];
        } else { // hash_len == 3
            destination_hash.hash_3b[0] = hash_data[0];
            destination_hash.hash_3b[1] = hash_data[1];
            destination_hash.hash_3b[2] = hash_data[2];
        }
        
        return true;
    }
    
    /**
     * @brief Get hash as raw bytes
     * @param buffer Output buffer (must be at least 3 bytes)
     * @param buffer_size Size of output buffer
     * @return Number of bytes written to buffer
     */
    size_t getHash(uint8_t* buffer, size_t buffer_size) const {
        size_t hash_len = static_cast<size_t>(hash_size);
        if (buffer_size < hash_len) {
            return 0;
        }
        
        if (hash_len == 1) {
            buffer[0] = destination_hash.hash_1b;
        } else if (hash_len == 2) {
            buffer[0] = (destination_hash.hash_2b >> 8) & 0xFF;
            buffer[1] = destination_hash.hash_2b & 0xFF;
        } else { // hash_len == 3
            buffer[0] = destination_hash.hash_3b[0];
            buffer[1] = destination_hash.hash_3b[1];
            buffer[2] = destination_hash.hash_3b[2];
        }
        
        return hash_len;
    }
};

/**
 * @brief Configuration for MQTT Bridge, loaded from meshcored.ini
 */
struct MQTTConfig {
    bool enabled = false;              ///< Enable/disable MQTT support
    std::string broker_host;           ///< MQTT broker hostname/IP
    int broker_port = 1883;            ///< MQTT broker port
    std::string client_id;             ///< MQTT client ID
    std::string username;              ///< Optional: MQTT username
    std::string password;              ///< Optional: MQTT password
    
    size_t outbound_queue_size = 50;   ///< Max messages: app -> meshcore
    size_t inbound_queue_size = 50;    ///< Max messages: meshcore -> app
    uint32_t message_ttl_seconds = 3600; ///< Message time-to-live
    
    bool use_tls = false;              ///< Use TLS/SSL for MQTT connection
    std::string tls_ca_file;           ///< Path to CA certificate file
    
    int keepalive_seconds = 60;        ///< MQTT keepalive interval
    bool clean_session = true;         ///< MQTT clean session flag
};

/**
 * @brief Hash-to-Topic route mapping
 * 
 * Maps a hash address to an MQTT topic with direction settings
 */
struct HashRoute {
    HashSize hash_size;                ///< Size of this hash
    union {
        uint8_t  hash_1b;
        uint16_t hash_2b;
        uint8_t  hash_3b[3];
    } hash;                            ///< The hash address
    
    std::string topic;                 ///< MQTT topic for this route
    
    enum Direction {
        PUBLISH = 0,                   ///< Send to MQTT (app->meshcore->MQTT)
        SUBSCRIBE = 1,                 ///< Receive from MQTT (MQTT->meshcore->app)
        BIDIRECTIONAL = 2              ///< Both directions
    } direction = Direction::PUBLISH;
    
    /**
     * @brief Match a hash against this route
     * @param cmp_size Size of hash being compared
     * @param cmp_hash Hash data to compare
     * @return true if hash matches
     */
    bool matches(HashSize cmp_size, const uint8_t* cmp_hash) const {
        if (cmp_size != hash_size) return false;
        
        size_t hash_len = static_cast<size_t>(hash_size);
        if (hash_size == HashSize::BYTES_1) {
            return cmp_hash[0] == hash.hash_1b;
        } else if (hash_size == HashSize::BYTES_2) {
            return (cmp_hash[0] << 8 | cmp_hash[1]) == hash.hash_2b;
        } else {
            return cmp_hash[0] == hash.hash_3b[0] &&
                   cmp_hash[1] == hash.hash_3b[1] &&
                   cmp_hash[2] == hash.hash_3b[2];
        }
    }
};

/**
 * @brief MQTT Bridge for bidirectional message routing
 * 
 * This bridge connects the application layer to meshcore via MQTT.
 * 
 * Outbound Flow (app -> meshcore):
 *   1. Application enqueues a message with hash and payload
 *   2. Bridge stores in outbound queue
 *   3. Meshcore periodically polls outbound queue
 *   4. Meshcore encrypts and sends via radio
 * 
 * Inbound Flow (meshcore -> app):
 *   1. Meshcore receives encrypted radio packet
 *   2. Meshcore decrypts and calls enqueueInbound()
 *   3. Bridge stores in inbound queue
 *   4. Application polls inbound queue to receive messages
 *   5. Application forwards to MQTT broker as needed
 * 
 * All encryption and radio transmission is handled by meshcore.
 * This bridge only manages the queuing and routing logic.
 */
class MQTTBridge {
private:
    MQTTConfig _config;
    
    // Queues for message passing
    std::queue<MQTTMessage*> _outbound_queue;  ///< app -> meshcore
    std::queue<MQTTMessage*> _inbound_queue;   ///< meshcore -> app
    
    // Route mapping
    std::vector<HashRoute> _routes;
    
    // State tracking
    bool _initialized = false;
    uint32_t _last_cleanup = 0;
    
    /**
     * @brief Clean up expired messages from queues
     */
    void cleanupExpiredMessages();
    
    /**
     * @brief Find route matching the given hash
     * @param hash_size Size of hash
     * @param hash Hash data
     * @return Pointer to route if found, nullptr otherwise
     */
    const HashRoute* findRoute(HashSize hash_size, const uint8_t* hash) const;
    
public:
    /**
     * @brief Constructor
     * @param config MQTT configuration
     */
    explicit MQTTBridge(const MQTTConfig& config);
    
    /**
     * @brief Destructor
     */
    ~MQTTBridge();
    
    /**
     * @brief Initialize the bridge
     * @return true if successful
     */
    bool begin();
    
    /**
     * @brief Shut down the bridge
     */
    void end();
    
    /**
     * @brief Main loop - handle periodic tasks
     * Should be called regularly from the main event loop
     */
    void loop();
    
    /**
     * @brief Add a hash-to-topic route
     * 
     * Maps a mesh hash address to an MQTT topic with direction.
     * Can be called before or after begin().
     * 
     * @param hash_size Size of hash (1, 2, or 3 bytes)
     * @param hash Hash data (must be at least hash_size bytes)
     * @param topic MQTT topic string
     * @param direction Publish, Subscribe, or Bidirectional
     * @return true if route added successfully
     */
    bool addRoute(HashSize hash_size, const uint8_t* hash, 
                  const std::string& topic, HashRoute::Direction direction);
    
    // ===== Outbound: App -> Meshcore =====
    
    /**
     * @brief Queue a message from application for sending via meshcore
     * 
     * Application calls this to send a message through the mesh.
     * The message is queued until meshcore retrieves it.
     * 
     * @param hash_size Size of destination hash (1, 2, or 3 bytes)
     * @param hash Destination hash data
     * @param payload Message payload (bytes)
     * @param payload_len Length of payload
     * @return true if enqueued successfully, false if queue full
     */
    bool enqueueOutbound(HashSize hash_size, const uint8_t* hash,
                         const uint8_t* payload, size_t payload_len);
    
    /**
     * @brief Retrieve next outbound message for meshcore to send
     * 
     * Meshcore calls this to get the next message to transmit.
     * Caller takes ownership of the returned message and must delete it.
     * 
     * @param out_msg Output parameter - set to next message if available
     * @return true if message is available, false if queue empty
     */
    bool dequeueOutbound(MQTTMessage*& out_msg);
    
    /**
     * @brief Get count of pending outbound messages
     * @return Number of messages waiting to be sent
     */
    size_t getOutboundCount() const {
        return _outbound_queue.size();
    }
    
    // ===== Inbound: Meshcore -> App =====
    
    /**
     * @brief Queue a message received by meshcore for application
     * 
     * Meshcore calls this after receiving and decrypting a radio message.
     * The message is queued for the application to retrieve.
     * 
     * @param hash_size Size of source hash (1, 2, or 3 bytes)
     * @param hash Source hash data
     * @param payload Decrypted message payload (bytes)
     * @param payload_len Length of payload
     * @param source_topic Optional MQTT topic this came from
     * @return true if enqueued successfully, false if queue full
     */
    bool enqueueInbound(HashSize hash_size, const uint8_t* hash,
                        const uint8_t* payload, size_t payload_len,
                        const std::string& source_topic = "");
    
    /**
     * @brief Retrieve next inbound message for application
     * 
     * Application calls this to get messages received from the mesh.
     * Caller takes ownership of the returned message and must delete it.
     * 
     * @param out_msg Output parameter - set to next message if available
     * @return true if message is available, false if queue empty
     */
    bool dequeueInbound(MQTTMessage*& out_msg);
    
    /**
     * @brief Get count of pending inbound messages
     * @return Number of messages waiting for application
     */
    size_t getInboundCount() const {
        return _inbound_queue.size();
    }
    
    // ===== Configuration =====
    
    /**
     * @brief Check if bridge is initialized and ready
     * @return true if initialized
     */
    bool isInitialized() const {
        return _initialized;
    }
    
    /**
     * @brief Get configuration
     * @return Reference to configuration
     */
    const MQTTConfig& getConfig() const {
        return _config;
    }
    
    /**
     * @brief Get number of configured routes
     * @return Number of hash->topic mappings
     */
    size_t getRouteCount() const {
        return _routes.size();
    }
};

} // namespace mqtt
