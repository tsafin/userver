#include <vshard/impl/vshard_envelope.hpp>

#include <userver/logging/log.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

VshardEnvelope DecodeEnvelope(
    const storages::tarantool::ExecutionResult& result) {
    result.AssertOk();  // throws CommandException on IPROTO-level error

    const auto& data = result.GetData();

    if (!data.IsArray() || data.GetSize() == 0) {
        LOG_WARNING() << "vshard envelope: unexpected IPROTO_DATA shape";
        return {};
    }

    const auto& outer = data[0];
    if (!outer.IsArray() || outer.GetSize() < 2) {
        // Plain (non-vshard) response — treat as success with raw data
        VshardEnvelope env;
        env.app_result = outer;
        return env;
    }

    VshardEnvelope env;
    env.app_result = outer[0];
    env.vshard_error = ParseVshardError(outer[1]);
    return env;
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
