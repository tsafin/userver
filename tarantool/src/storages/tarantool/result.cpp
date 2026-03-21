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
    // data_ is lazily initialised on first GetData() call.
    // Callers that only use GetRawBytes() (e.g. vshard proxy) pay no
    // allocation cost for the Value tree.
}

ExecutionResult::ExecutionResult(ExecutionResult&& other) noexcept
    : ok_{other.ok_},
      error_code_{other.error_code_},
      error_message_{std::move(other.error_message_)},
      error_info_{std::move(other.error_info_)},
      data_buf_{std::move(other.data_buf_)},
      data_parsed_{other.data_parsed_} {
    // Rebind cursor to the new owner's buffer only if already parsed.
    // If nobody called GetData() before the move, we stay lazy.
    if (data_parsed_ && !data_buf_.empty()) {
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
        data_parsed_   = other.data_parsed_;
        data_          = (data_parsed_ && !data_buf_.empty())
                             ? formats::msgpack::Value::FromBytes(
                                   data_buf_.data(), data_buf_.size())
                             : formats::msgpack::Value{};
    }
    return *this;
}

const formats::msgpack::Value& ExecutionResult::GetData() const noexcept {
    if (!data_parsed_) {
        if (!data_buf_.empty()) {
            data_ = formats::msgpack::Value::FromBytes(data_buf_.data(),
                                                       data_buf_.size());
        }
        data_parsed_ = true;
    }
    return data_;
}

void ExecutionResult::AssertOk() const {
    if (!ok_) {
        throw CommandException{error_code_, error_message_, error_info_};
    }
}

}  // namespace storages::tarantool

USERVER_NAMESPACE_END
