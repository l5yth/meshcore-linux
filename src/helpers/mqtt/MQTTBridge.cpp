#include "MQTTBridge.h"
#include <ctime>
#include <cstring>

namespace mqtt {

MQTTBridge::MQTTBridge(const MQTTConfig& config)
    : _config(config), _initialized(false), _last_cleanup(0) {
}

MQTTBridge::~MQTTBridge() {
    end();
}

bool MQTTBridge::begin() {
    if (_initialized) {
        return true; // already initialized
    }
    
    if (!_config.enabled) {
        return false; // MQTT not enabled in config
    }
    
    if (_config.broker_host.empty() || _config.client_id.empty()) {
        return false; // missing required configuration
    }
    
    // TODO: Initialize MQTT client connection
    // This would typically involve:
    // 1. Creating MQTT client with broker_host:broker_port
    // 2. Setting credentials if provided
    // 3. Connecting and subscribing to configured topics
    // 4. Setting up callbacks for message reception
    
    _initialized = true;
    return true;
}

void MQTTBridge::end() {
    if (!_initialized) {
        return;
    }
    
    // TODO: Disconnect MQTT client and cleanup
    
    // Clear any remaining messages
    while (!_outbound_queue.empty()) {
        auto msg = _outbound_queue.front();
        _outbound_queue.pop();
        delete msg;
    }
    
    while (!_inbound_queue.empty()) {
        auto msg = _inbound_queue.front();
        _inbound_queue.pop();
        delete msg;
    }
    
    _initialized = false;
}

void MQTTBridge::loop() {
    if (!_initialized) {
        return;
    }
    
    // TODO: Handle MQTT client loop
    // This typically involves:
    // 1. Calling client.loop() for connection management
    // 2. Processing any received messages from MQTT
    // 3. Handling reconnection if needed
    
    // Periodically cleanup expired messages (every 60 seconds)
    uint32_t now = static_cast<uint32_t>(time(nullptr));
    if (now - _last_cleanup > 60) {
        cleanupExpiredMessages();
        _last_cleanup = now;
    }
}

bool MQTTBridge::addRoute(HashSize hash_size, const uint8_t* hash,
                         const std::string& topic,
                         HashRoute::Direction direction) {
    if (!hash || hash_size == HashSize::BYTES_1 && hash_size > HashSize::BYTES_3) {
        return false;
    }
    
    if (topic.empty()) {
        return false;
    }
    
    // Check if route already exists for this hash
    for (auto& route : _routes) {
        if (route.matches(hash_size, hash)) {
            // Update existing route
            route.topic = topic;
            route.direction = direction;
            return true;
        }
    }
    
    // Add new route
    HashRoute route;
    route.hash_size = hash_size;
    route.direction = direction;
    route.topic = topic;
    
    size_t hash_len = static_cast<size_t>(hash_size);
    if (hash_len == 1) {
        route.hash.hash_1b = hash[0];
    } else if (hash_len == 2) {
        route.hash.hash_2b = (hash[0] << 8) | hash[1];
    } else { // hash_len == 3
        route.hash.hash_3b[0] = hash[0];
        route.hash.hash_3b[1] = hash[1];
        route.hash.hash_3b[2] = hash[2];
    }
    
    _routes.push_back(route);
    return true;
}

bool MQTTBridge::enqueueOutbound(HashSize hash_size, const uint8_t* hash,
                                 const uint8_t* payload, size_t payload_len) {
    if (!hash || !payload || payload_len == 0) {
        return false;
    }
    
    if (_outbound_queue.size() >= _config.outbound_queue_size) {
        return false; // queue full
    }
    
    // Create new message
    auto msg = new MQTTMessage();
    msg->hash_size = hash_size;
    msg->is_outbound = true;
    msg->timestamp = static_cast<uint32_t>(time(nullptr));
    
    // Set hash
    if (!msg->setHash(hash, static_cast<size_t>(hash_size))) {
        delete msg;
        return false;
    }
    
    // Copy payload
    msg->payload = new uint8_t[payload_len];
    if (!msg->payload) {
        delete msg;
        return false;
    }
    memcpy(msg->payload, payload, payload_len);
    msg->payload_len = payload_len;
    
    _outbound_queue.push(msg);
    return true;
}

bool MQTTBridge::dequeueOutbound(MQTTMessage*& out_msg) {
    if (_outbound_queue.empty()) {
        return false;
    }
    
    out_msg = _outbound_queue.front();
    _outbound_queue.pop();
    return true;
}

bool MQTTBridge::enqueueInbound(HashSize hash_size, const uint8_t* hash,
                                const uint8_t* payload, size_t payload_len,
                                const std::string& source_topic) {
    if (!hash || !payload || payload_len == 0) {
        return false;
    }
    
    if (_inbound_queue.size() >= _config.inbound_queue_size) {
        return false; // queue full
    }
    
    // Create new message
    auto msg = new MQTTMessage();
    msg->hash_size = hash_size;
    msg->is_outbound = false;
    msg->timestamp = static_cast<uint32_t>(time(nullptr));
    msg->source_topic = source_topic;
    
    // Set hash
    if (!msg->setHash(hash, static_cast<size_t>(hash_size))) {
        delete msg;
        return false;
    }
    
    // Copy payload
    msg->payload = new uint8_t[payload_len];
    if (!msg->payload) {
        delete msg;
        return false;
    }
    memcpy(msg->payload, payload, payload_len);
    msg->payload_len = payload_len;
    
    _inbound_queue.push(msg);
    return true;
}

bool MQTTBridge::dequeueInbound(MQTTMessage*& out_msg) {
    if (_inbound_queue.empty()) {
        return false;
    }
    
    out_msg = _inbound_queue.front();
    _inbound_queue.pop();
    return true;
}

const HashRoute* MQTTBridge::findRoute(HashSize hash_size, const uint8_t* hash) const {
    if (!hash) {
        return nullptr;
    }
    
    for (const auto& route : _routes) {
        if (route.matches(hash_size, hash)) {
            return &route;
        }
    }
    
    return nullptr;
}

void MQTTBridge::cleanupExpiredMessages() {
    uint32_t now = static_cast<uint32_t>(time(nullptr));
    uint32_t ttl = _config.message_ttl_seconds;
    
    // Clean outbound queue
    std::queue<MQTTMessage*> cleaned_outbound;
    while (!_outbound_queue.empty()) {
        auto msg = _outbound_queue.front();
        _outbound_queue.pop();
        
        if (now - msg->timestamp < ttl) {
            cleaned_outbound.push(msg);
        } else {
            delete msg; // expired
        }
    }
    _outbound_queue = cleaned_outbound;
    
    // Clean inbound queue
    std::queue<MQTTMessage*> cleaned_inbound;
    while (!_inbound_queue.empty()) {
        auto msg = _inbound_queue.front();
        _inbound_queue.pop();
        
        if (now - msg->timestamp < ttl) {
            cleaned_inbound.push(msg);
        } else {
            delete msg; // expired
        }
    }
    _inbound_queue = cleaned_inbound;
}

} // namespace mqtt
