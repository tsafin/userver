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

ExecutionResult::ExecutionResult(ExecutionResult&& other) noexcept
    : ok_{other.ok_},
      error_code_{other.error_code_},
      error_message_{std::move(other.error_message_)},
      error_info_{std::move(other.error_info_)},
      data_buf_{std::move(other.data_buf_)} {
    // Rebind cursor to the new owner's buffer (other.data_ would point at the
    // now-empty source buffer after the vector move above).
    if (!data_buf_.empty()) {
        data_ = formats::msgpack::Value::FromBytes(data_buf_.data(),
                                                   data_buf_.size());
    }
}

ExecutionResult& ExecutionResult::operator=(ExecutionResult&& other) noexcept {
    if (this != &other) {
        ok_            = other.ok_;
        error_code_    = other.error_code_;
        error_message_ = std::move(other.error_message_);
        error_info_    = std::move(other.error_info_);
        data_buf_      = std::move(other.data_buf_);
        data_          = data_buf_.empty()
                             ? formats::msgpack::Value{}
                             : formats::msgpack::Value::FromBytes(
                                   data_buf_.data(), data_buf_.size());
    }
    return *this;
}

void ExecutionResult::AssertOk() const {
    if (!ok_) {
        throw CommandException{error_code_, error_message_, error_info_};
    }
}

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
