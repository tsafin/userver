#include <vshard/impl/vshard_error.hpp>

#include <userver/logging/log.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

namespace {
VshardErrorType ErrorTypeFromCode(uint32_t code) noexcept {
    switch (code) {
        case 32: return VshardErrorType::kWrongBucket;
        case 33: return VshardErrorType::kTransfer;
        case 40: return VshardErrorType::kNonMaster;
        case 24: return VshardErrorType::kNoRouteset;
        default: return VshardErrorType::kUnknown;
    }
}
}  // namespace

VshardError ParseVshardError(const formats::msgpack::Value& val) {
    if (!val.IsObject()) return {};

    VshardError err;
    const auto& code_val = val["code"];
    if (!code_val.IsNull()) {
        err.code = code_val.As<uint32_t>(0);
        err.type = ErrorTypeFromCode(err.code);
    }
    const auto& msg_val = val["message"];
    if (!msg_val.IsNull()) {
        err.message = msg_val.As<std::string>("");
    }
    const auto& dst_val = val["destination"];
    if (!dst_val.IsNull()) {
        err.destination_uuid = dst_val.As<std::string>("");
    }
    return err;
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
