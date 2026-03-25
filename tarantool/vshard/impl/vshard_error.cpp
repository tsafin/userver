#include <vshard/impl/vshard_error.hpp>

#include <userver/logging/log.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

namespace {
VshardErrorType ErrorTypeFromCode(uint32_t code) noexcept {
    // vshard error codes from vshard/error.lua (1-based sequential codes)
    switch (code) {
        case 1:  return VshardErrorType::kWrongBucket;
        case 2:  return VshardErrorType::kNonMaster;
        case 7:  return VshardErrorType::kTransfer;
        case 9:  return VshardErrorType::kNoRouteset;  // NO_ROUTE_TO_BUCKET
        case 22: return VshardErrorType::kBucketIsLocked;
        default: return VshardErrorType::kUnknown;
    }
}
}  // namespace

namespace {
VshardErrorType ErrorTypeFromName(const std::string& name) noexcept {
    if (name == "WRONG_BUCKET") return VshardErrorType::kWrongBucket;
    if (name == "NON_MASTER") return VshardErrorType::kNonMaster;
    if (name == "TRANSFER_IS_IN_PROGRESS") return VshardErrorType::kTransfer;
    if (name == "NO_ROUTE_TO_BUCKET") return VshardErrorType::kNoRouteset;
    if (name == "BUCKET_IS_LOCKED") return VshardErrorType::kBucketIsLocked;
    return VshardErrorType::kUnknown;
}
}  // namespace

VshardError ParseVshardError(const formats::msgpack::Value& val) {
    if (!val.IsObject()) return {};

    VshardError err;
    const auto& code_val = val["code"];
    if (!code_val.IsMissing() && !code_val.IsNull()) {
        err.code = code_val.As<uint32_t>(0);
        err.type = ErrorTypeFromCode(err.code);
    }
    // Also parse name for extra robustness / cross-version compatibility
    if (err.type == VshardErrorType::kUnknown) {
        const auto& name_val = val["name"];
        if (!name_val.IsMissing() && name_val.IsString()) {
            err.name = name_val.As<std::string>("");
            err.type = ErrorTypeFromName(err.name);
        }
    }
    if (err.name.empty()) {
        const auto& name_val = val["name"];
        if (!name_val.IsMissing() && name_val.IsString()) {
            err.name = name_val.As<std::string>("");
        }
    }
    const auto& msg_val = val["message"];
    if (!msg_val.IsMissing() && !msg_val.IsNull()) {
        err.message = msg_val.As<std::string>("");
    }
    const auto& bucket_id_val = val["bucket_id"];
    if (!bucket_id_val.IsMissing() && !bucket_id_val.IsNull()) {
        err.bucket_id = bucket_id_val.As<uint32_t>();
    }
    const auto& dst_val = val["destination"];
    if (!dst_val.IsMissing() && !dst_val.IsNull()) {
        err.destination_uuid = dst_val.As<std::string>("");
    }
    const auto& rs_val = val["replicaset"];
    if (!rs_val.IsMissing() && !rs_val.IsNull()) {
        err.replicaset_uuid = rs_val.As<std::string>("");
    }
    const auto& replica_val = val["replica"];
    if (!replica_val.IsMissing() && !replica_val.IsNull()) {
        err.replica_uuid = replica_val.As<std::string>("");
    }
    const auto& master_val = val["master"];
    if (!master_val.IsMissing() && !master_val.IsNull()) {
        err.master_uuid = master_val.As<std::string>("");
    }
    return err;
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
