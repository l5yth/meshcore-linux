#pragma once

#include "MQTTBridge.h"
#include <string>
#include <map>

namespace mqtt {

/**
 * @brief Parser for MQTT configuration from meshcored.ini
 * 
 * Loads and parses MQTT settings from INI file.
 * 
 * Configuration format:
 * 
 * [mqtt]
 * enabled = true
 * broker = mqtt.example.com
 * port = 1883
 * client_id = meshcore-node-1
 * username = user
 * password = pass
 * 
 * # Hash routes: hash_<size>_<hex> = topic[:direction]
 * # size: 1, 2, 3 (bytes)
 * # direction: publish, subscribe, bidirectional (default: publish)
 * 
 * hash_1b_ff = sensors/light
 * hash_1b_fe = sensors/temperature:subscribe
 * hash_2b_abcd = mesh/broadcast:bidirectional
 * hash_3b_123456 = mesh/data
 */
class MQTTConfigParser {
public:
    /**
     * @brief Parse MQTT configuration from INI file
     * @param config_file Path to meshcored.ini file
     * @param out_config Output configuration structure
     * @return true if successfully parsed
     */
    static bool parseFromFile(const std::string& config_file, MQTTConfig& out_config);
    
    /**
     * @brief Parse MQTT configuration from text buffer
     * @param config_text INI file contents
     * @param out_config Output configuration structure
     * @return true if successfully parsed
     */
    static bool parseFromString(const std::string& config_text, MQTTConfig& out_config);
    
    /**
     * @brief Parse a single hash route definition
     * @param hash_def Hash definition string (e.g., "1b_ff")
     * @param topic_def Topic definition string (e.g., "sensors/light:publish")
     * @param out_hash_size Output hash size
     * @param out_hash Output hash bytes (buffer must be 3 bytes)
     * @param out_topic Output topic string
     * @param out_direction Output direction
     * @return true if successfully parsed
     */
    static bool parseHashRoute(const std::string& hash_def,
                               const std::string& topic_def,
                               HashSize& out_hash_size,
                               uint8_t* out_hash,
                               std::string& out_topic,
                               HashRoute::Direction& out_direction);
    
private:
    /**
     * @brief Parse hex string to bytes
     * @param hex_str Hex string (e.g., "ff", "abcd", "123456")
     * @param out_bytes Output buffer for bytes
     * @param expected_len Expected number of output bytes
     * @return true if successfully parsed
     */
    static bool parseHexString(const std::string& hex_str, uint8_t* out_bytes, size_t expected_len);
    
    /**
     * @brief Parse direction string
     * @param dir_str Direction string ("publish", "subscribe", "bidirectional")
     * @return Parsed direction
     */
    static HashRoute::Direction parseDirection(const std::string& dir_str);
};

} // namespace mqtt
