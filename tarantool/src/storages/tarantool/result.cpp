#include <userver/storages/tarantool/result.hpp>

#include <userver/storages/tarantool/exceptions.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool {

ExecutionResult::ExecutionResult(bool ok, uint32_t error_code,
                                 std::string error_message,
                                 formats::json::Value data)
    : ok_{ok},
      error_code_{error_code},
      error_message_{std::move(error_message)},
      data_{std::move(data)} {}

void ExecutionResult::AssertOk() const {
    if (!ok_) {
        throw CommandException{error_code_, error_message_};
    }
}

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
