#include <vshard/impl/vshard_envelope.hpp>

#include <userver/logging/log.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

VshardEnvelope DecodeEnvelope(
    const storages::tarantool::ExecutionResult& result) {
    result.AssertOk();  // throws CommandException on IPROTO-level error

    const auto& data = result.GetData();

    // vshard.storage.call via IPROTO_CALL (0x0A) returns a flat array:
    //   [status, result_or_error]
    //   status = true  → success
    //   status = false → user function error
    //   status = nil   → vshard routing error (WRONG_BUCKET etc.)
    if (!data.IsArray() || data.GetSize() == 0) {
        LOG_WARNING() << "vshard envelope: unexpected IPROTO_DATA shape "
                      << "(size=" << (data.IsArray() ? data.GetSize() : -1) << ")";
        return {};
    }

    const auto& status = data[0];
    VshardEnvelope env;

    if (status.IsNull()) {
        // nil status → vshard routing error; data[1] is the vshard error object
        if (data.GetSize() >= 2) {
            env.vshard_error = ParseVshardError(data[1]);
        }
        return env;
    }

    if (!status.As<bool>(false)) {
        // false status → user function raised an error; data[1] is the error.
        // We re-surface it as a CommandException so callers see a real error.
        std::string err_msg = "vshard: user function error";
        if (data.GetSize() >= 2 && data[1].IsString()) {
            err_msg = data[1].As<std::string>("");
        }
        throw storages::tarantool::CommandException{1, std::move(err_msg)};
    }

    // true status → success; data[1] is the user function result (may be nil)
    if (data.GetSize() >= 2) {
        env.app_result = data[1];
    }
    return env;
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
