#include <vshard/impl/vshard_envelope.hpp>

#include <userver/logging/log.hpp>

#include <storages/tarantool/impl/iproto_frames.hpp>
#include <storages/tarantool/impl/msgpack_constants.hpp>

USERVER_NAMESPACE_BEGIN

namespace storages::tarantool::vshard::impl {

namespace {

void PushFixArray(std::vector<uint8_t>& buf, uint8_t n) {
    buf.push_back(static_cast<uint8_t>(mp::kFixArrayMin | (n & 0x0fu)));
}

void PushNil(std::vector<uint8_t>& buf) {
    buf.push_back(mp::kNil);
}

}  // namespace

VshardEnvelope DecodeEnvelope(
    const storages::tarantool::ExecutionResult& result) {
    result.AssertOk();  // throws CommandException on IPROTO-level error

    const auto& data = result.GetData();

    if (!data.IsArray() || data.GetSize() == 0) {
        LOG_WARNING() << "vshard envelope: unexpected IPROTO_DATA shape "
                      << "(size=" << (data.IsArray() ? data.GetSize() : -1) << ")";
        return {};
    }

    const auto& status = data[0];
    VshardEnvelope env;

    if (status.IsNull()) {
        if (data.GetSize() >= 2) {
            env.vshard_error = ParseVshardError(data[1]);
        }
        return env;
    }

    if (!status.As<bool>(false)) {
        std::string err_msg = "vshard: user function error";
        if (data.GetSize() >= 2 && data[1].IsString()) {
            err_msg = data[1].As<std::string>("");
        }
        throw storages::tarantool::CommandException{1, std::move(err_msg)};
    }

    if (data.GetSize() >= 2) {
        env.app_result = data[1];
    }
    return env;
}

// ---------------------------------------------------------------------------
// Zero-copy raw path
// ---------------------------------------------------------------------------

namespace {

/// Fall-back for error paths: parse via Value tree (cold path).
RawEnvelopeResult DecodeEnvelopeRawFallback(
    const storages::tarantool::ExecutionResult& result) {
    auto env = DecodeEnvelope(result);
    RawEnvelopeResult r;
    if (!env.vshard_error.IsNull()) {
        r.status = RawEnvelopeStatus::kVshardError;
        r.vshard_error = std::move(env.vshard_error);
    }
    return r;
}

}  // namespace

RawEnvelopeResult DecodeEnvelopeRaw(
    const storages::tarantool::ExecutionResult& result) {
    result.AssertOk();

    const auto raw = result.GetRawBytes();
    const uint8_t* p = raw.data();
    const std::size_t len = raw.size();

    if (len == 0) return {};

    std::size_t pos = 0;

    // Read the outer array length.  Tarantool's IPROTO_CALL packs multi-return
    // values as array32 (0xdd) regardless of element count, so we must handle
    // all three array formats: fixarray (0x9N), array16 (0xdc), array32 (0xdd).
    std::size_t count = 0;
    const uint8_t fb = p[pos++];
    if ((fb & 0xf0u) == mp::kFixArrayMin) {
        count = fb & 0x0fu;
    } else if (fb == mp::kArray16) {
        if (pos + 2 > len) return {};
        count = (std::size_t)p[pos] << 8 | p[pos + 1];
        pos += 2;
    } else if (fb == mp::kArray32) {
        if (pos + 4 > len) return {};
        count = (std::size_t)p[pos] << 24 | (std::size_t)p[pos + 1] << 16 |
                (std::size_t)p[pos + 2] << 8 | p[pos + 3];
        pos += 4;
    } else {
        // Not an array at all — fall back to Value path.
        LOG_WARNING() << "vshard envelope: IPROTO_DATA is not an array fb=" << static_cast<int>(fb)
                      << " len=" << len;
        return DecodeEnvelopeRawFallback(result);
    }

    if (count == 0 || pos >= len) return {};

    // Element [0]: status — true (0xc3) / false (0xc2) / nil (0xc0)
    const uint8_t status_byte = p[pos];

    if (status_byte == mp::kTrue) {
        // Success path — extract element [1] and wrap it as router multi-return
        // values [result] without building a Value tree.
        ++pos;  // skip status byte
        if (count < 2 || pos >= len) {
            RawEnvelopeResult r;
            PushFixArray(r.return_values_bytes, 1);
            PushNil(r.return_values_bytes);
            return r;
        }
        const std::size_t result_start = pos;
        const std::size_t result_end =
            storages::tarantool::impl::msgpack_scan::SkipValue(p, len, pos);
        RawEnvelopeResult r;
        r.return_values_bytes.reserve(1 + (result_end - result_start));
        PushFixArray(r.return_values_bytes, 1);
        r.return_values_bytes.insert(
            r.return_values_bytes.end(), p + result_start, p + result_end);
        return r;
    }

    if (status_byte == mp::kFalse) {
        // Lua router surfaces storage/user-function failures as [nil, err].
        ++pos;  // skip status byte
        RawEnvelopeResult r;
        r.status = RawEnvelopeStatus::kStorageCallError;
        PushFixArray(r.return_values_bytes, 2);
        PushNil(r.return_values_bytes);
        if (count < 2 || pos >= len) {
            PushNil(r.return_values_bytes);
            return r;
        }
        const std::size_t err_start = pos;
        const std::size_t err_end =
            storages::tarantool::impl::msgpack_scan::SkipValue(p, len, pos);
        r.return_values_bytes.insert(
            r.return_values_bytes.end(), p + err_start, p + err_end);
        return r;
    }

    // Error paths — fall back to Value parse (cold path)
    return DecodeEnvelopeRawFallback(result);
}

}  // namespace storages::tarantool::vshard::impl

USERVER_NAMESPACE_END
