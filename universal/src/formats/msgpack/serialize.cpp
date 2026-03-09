#include <userver/formats/msgpack/serialize.hpp>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

std::vector<uint8_t> ToBytes(const ValueBuilder& value) {
    return value.ToBytes();
}

Value FromBytes(const uint8_t* data, std::size_t len) noexcept {
    return Value::FromBytes(data, len);
}

Value FromBytes(const std::vector<uint8_t>& buf) noexcept {
    return Value::FromBytes(buf.data(), buf.size());
}

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
