#pragma once

#include <cstdint>
#include <cstring>
#include <string_view>
#include <vector>

/// @brief Buffer adapter satisfying the tntcxx mpp::encode / RequestEncoder /
///        ResponseDecoder concept.
///
/// tntcxx's codec layers are templated on a BUFFER type and require:
///   - write(uint8_t)        — append one byte
///   - write(uint32_t)       — append 4 bytes in big-endian order
///   - write(string_view)    — append a byte range
///   - write(const T&)       — raw byte-copy fallback
///   - set(iterator, uint32_t) — back-patch 4 bytes at a given position
///   - begin() / end()       — random-access iterators
///   - clear()               — reset content
///   - size()                — current byte count
struct TntBuffer {
    std::vector<uint8_t> data;

    void write(uint8_t v) { data.push_back(v); }

    void write(uint32_t v) {
        data.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        data.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        data.push_back(static_cast<uint8_t>((v >>  8) & 0xFF));
        data.push_back(static_cast<uint8_t>( v        & 0xFF));
    }

    void write(std::string_view s) {
        data.insert(data.end(), s.begin(), s.end());
    }

    template <typename T>
    void write(const T& v) {
        const auto* p = reinterpret_cast<const uint8_t*>(&v);
        data.insert(data.end(), p, p + sizeof(T));
    }

    using iterator = uint8_t*;
    using const_iterator = const uint8_t*;

    iterator begin() { return data.data(); }
    iterator end()   { return data.data() + data.size(); }
    const_iterator begin() const { return data.data(); }
    const_iterator end()   const { return data.data() + data.size(); }

    /// Back-patch 4 bytes in big-endian order at position `pos`
    void set(iterator pos, uint32_t v) {
        pos[0] = static_cast<uint8_t>((v >> 24) & 0xFF);
        pos[1] = static_cast<uint8_t>((v >> 16) & 0xFF);
        pos[2] = static_cast<uint8_t>((v >>  8) & 0xFF);
        pos[3] = static_cast<uint8_t>( v        & 0xFF);
    }

    void clear() { data.clear(); }
    std::size_t size() const { return data.size(); }
    bool empty() const { return data.empty(); }
};
