#include <userver/storages/tarantool/result.hpp>

#include <userver/storages/tarantool/exceptions.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

ExecutionResult::ExecutionResult(bool ok, uint32_t error_code,
                                 std::string error_message,
                                 std::vector<uint8_t> data_buf,
                                 std::optional<TntErrorInfo> error_info)
    : ok_{ok},
      error_code_{error_code},
      error_message_{std::move(error_message)},
      error_info_{std::move(error_info)},
      data_buf_{std::move(data_buf)} {
    if (!data_buf_.empty()) {
        data_ = formats::msgpack::Value::FromBytes(data_buf_.data(),
                                                   data_buf_.size());
    }
}

void ExecutionResult::AssertOk() const {
    if (!ok_) {
        throw CommandException{error_code_, error_message_, error_info_};
    }
}

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
