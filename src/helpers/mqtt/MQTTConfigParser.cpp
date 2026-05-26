#include "MQTTConfigParser.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cctype>
#include <cstdlib>

namespace mqtt {

/**
 * @brief Trim whitespace from string
 */
static std::string trim(const std::string& str) {
    auto start = str.begin();
    while (start != str.end() && std::isspace(*start)) {
        start++;
    }
    
    auto end = str.end();
    do {
        end--;
    } while (std::distance(start, end) > 0 && std::isspace(*end));
    
    return std::string(start, end + 1);
}

/**
 * @brief Convert string to lowercase
 */
static std::string toLower(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::tolower);
    return result;
}

bool MQTTConfigParser::parseFromFile(const std::string& config_file, MQTTConfig& out_config) {
    std::ifstream file(config_file);
    if (!file.is_open()) {
        return false;
    }
    
    std::stringstream buffer;
    buffer << file.rdbuf();
    file.close();
    
    return parseFromString(buffer.str(), out_config);
}

bool MQTTConfigParser::parseFromString(const std::string& config_text, MQTTConfig& out_config) {
    std::istringstream stream(config_text);
    std::string line;
    bool in_mqtt_section = false;
    
    while (std::getline(stream, line)) {
        // Remove comments
        auto comment_pos = line.find('#');
        if (comment_pos != std::string::npos) {
            line = line.substr(0, comment_pos);
        }
        
        line = trim(line);
        
        // Check for section header
        if (line.substr(0, 1) == "[") {
            in_mqtt_section = (toLower(line) == "[mqtt]");
            continue;
        }
        
        if (!in_mqtt_section || line.empty()) {
            continue;
        }
        
        // Parse key=value
        auto eq_pos = line.find('=');
        if (eq_pos == std::string::npos) {
            continue;
        }
        
        std::string key = toLower(trim(line.substr(0, eq_pos)));
        std::string value = trim(line.substr(eq_pos + 1));
        
        if (key == "enabled") {
            out_config.enabled = (toLower(value) == "true" || value == "1");
        } else if (key == "broker") {
            out_config.broker_host = value;
        } else if (key == "port") {
            out_config.broker_port = std::atoi(value.c_str());
        } else if (key == "client_id") {
            out_config.client_id = value;
        } else if (key == "username") {
            out_config.username = value;
        } else if (key == "password") {
            out_config.password = value;
        } else if (key == "outbound_queue_size") {
            out_config.outbound_queue_size = std::atoi(value.c_str());
        } else if (key == "inbound_queue_size") {
            out_config.inbound_queue_size = std::atoi(value.c_str());
        } else if (key == "message_ttl_seconds") {
            out_config.message_ttl_seconds = std::atoi(value.c_str());
        } else if (key == "use_tls") {
            out_config.use_tls = (toLower(value) == "true" || value == "1");
        } else if (key == "tls_ca_file") {
            out_config.tls_ca_file = value;
        } else if (key == "keepalive_seconds") {
            out_config.keepalive_seconds = std::atoi(value.c_str());
        } else if (key == "clean_session") {
            out_config.clean_session = (toLower(value) == "true" || value == "1");
        } else if (key.substr(0, 5) == "hash_") {
            // Parse hash route
            HashSize hash_size;
            uint8_t hash[3];
            std::string topic;
            HashRoute::Direction direction;
            
            std::string hash_def = key.substr(5); // Remove "hash_" prefix
            if (parseHashRoute(hash_def, value, hash_size, hash, topic, direction)) {
                // Route will be added to bridge separately
                // This is just validating the parse
            }
        }
    }
    
    return out_config.enabled && !out_config.broker_host.empty() && !out_config.client_id.empty();
}

bool MQTTConfigParser::parseHashRoute(const std::string& hash_def,
                                      const std::string& topic_def,
                                      HashSize& out_hash_size,
                                      uint8_t* out_hash,
                                      std::string& out_topic,
                                      HashRoute::Direction& out_direction) {
    // Parse hash definition: "1b_ff", "2b_abcd", "3b_123456"
    std::istringstream hash_stream(hash_def);
    std::string size_str, hex_str;
    
    if (!std::getline(hash_stream, size_str, '_') || size_str.empty()) {
        return false;
    }
    
    if (!std::getline(hash_stream, hex_str) || hex_str.empty()) {
        return false;
    }
    
    // Parse size (1, 2, or 3)
    int size_val = std::atoi(size_str.c_str());
    if (size_val < 1 || size_val > 3) {
        return false;
    }
    out_hash_size = static_cast<HashSize>(size_val);
    
    // Parse hex string
    if (!parseHexString(hex_str, out_hash, size_val)) {
        return false;
    }
    
    // Parse topic definition: "topic" or "topic:direction"
    auto colon_pos = topic_def.find(':');
    if (colon_pos != std::string::npos) {
        out_topic = trim(topic_def.substr(0, colon_pos));
        std::string dir_str = toLower(trim(topic_def.substr(colon_pos + 1)));
        out_direction = parseDirection(dir_str);
    } else {
        out_topic = trim(topic_def);
        out_direction = HashRoute::Direction::PUBLISH;
    }
    
    return !out_topic.empty();
}

bool MQTTConfigParser::parseHexString(const std::string& hex_str, uint8_t* out_bytes, size_t expected_len) {
    if (hex_str.length() != expected_len * 2) {
        return false; // Each byte needs 2 hex digits
    }
    
    for (size_t i = 0; i < expected_len; i++) {
        std::string byte_str = hex_str.substr(i * 2, 2);
        char* endptr = nullptr;
        long val = std::strtol(byte_str.c_str(), &endptr, 16);
        
        if (endptr != byte_str.c_str() + 2 || val < 0 || val > 255) {
            return false; // Invalid hex
        }
        
        out_bytes[i] = static_cast<uint8_t>(val);
    }
    
    return true;
}

HashRoute::Direction MQTTConfigParser::parseDirection(const std::string& dir_str) {
    if (dir_str == "publish") {
        return HashRoute::Direction::PUBLISH;
    } else if (dir_str == "subscribe") {
        return HashRoute::Direction::SUBSCRIBE;
    } else if (dir_str == "bidirectional") {
        return HashRoute::Direction::BIDIRECTIONAL;
    }
    return HashRoute::Direction::PUBLISH; // Default
}

} // namespace mqtt
