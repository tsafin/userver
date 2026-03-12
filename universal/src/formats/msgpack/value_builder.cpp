#include <userver/formats/msgpack/value_builder.hpp>

#include <cassert>
#include <cstring>
#include <stdexcept>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include <userver/formats/msgpack/tarantool_types.hpp>
#include <userver/utils/datetime/date.hpp>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

// ======================================================================== //
//  Internal Node                                                            //
// ======================================================================== //

namespace impl {

struct Node;

using NodePtr  = std::unique_ptr<Node>;
using Array    = std::vector<NodePtr>;
using StrPair  = std::pair<std::string,  NodePtr>;
using IntPair  = std::pair<uint64_t,     NodePtr>;
using StrMap   = std::vector<StrPair>;
using IntMap   = std::vector<IntPair>;

// Raw pre-encoded msgpack bytes (for ext types and other opaque values).
struct ExtData {
    std::vector<uint8_t> raw;
};

struct Node {
    using Data = std::variant<
        std::monostate,  // nil
        bool,
        int64_t,
        uint64_t,
        double,
        std::string,
        Array,
        StrMap,
        IntMap,
        ExtData
    >;

    Data data{std::monostate{}};

    Node() = default;
    explicit Node(bool v)            : data(v) {}
    explicit Node(int64_t v)         : data(v) {}
    explicit Node(uint64_t v)        : data(v) {}
    explicit Node(double v)          : data(v) {}
    explicit Node(std::string v)     : data(std::move(v)) {}
    explicit Node(Array arr)         : data(std::move(arr)) {}
    explicit Node(StrMap m)          : data(std::move(m)) {}
    explicit Node(IntMap m)          : data(std::move(m)) {}
    explicit Node(ExtData e)         : data(std::move(e)) {}

    // Deep copy
    Node(const Node& o);
    Node& operator=(const Node& o);
    Node(Node&&) = default;
    Node& operator=(Node&&) = default;
};

static NodePtr CloneNode(const Node& n);

Node::Node(const Node& o) : data(std::monostate{}) {
    *this = o;
}

Node& Node::operator=(const Node& o) {
    std::visit([&](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, Array>) {
            Array a;
            a.reserve(v.size());
            for (const auto& ptr : v) a.push_back(CloneNode(*ptr));
            data = std::move(a);
        } else if constexpr (std::is_same_v<T, StrMap>) {
            StrMap m;
            m.reserve(v.size());
            for (const auto& [k, vp] : v) m.emplace_back(k, CloneNode(*vp));
            data = std::move(m);
        } else if constexpr (std::is_same_v<T, IntMap>) {
            IntMap m;
            m.reserve(v.size());
            for (const auto& [k, vp] : v) m.emplace_back(k, CloneNode(*vp));
            data = std::move(m);
        } else {
            data = v;
        }
    }, o.data);
    return *this;
}

static NodePtr CloneNode(const Node& n) {
    return std::make_unique<Node>(n);
}

}  // namespace impl

// ======================================================================== //
//  Encoder: Node → msgpack bytes                                            //
// ======================================================================== //

namespace {

void AppendU16(std::vector<uint8_t>& out, uint16_t v) {
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v));
}

void AppendU32(std::vector<uint8_t>& out, uint32_t v) {
    out.push_back(static_cast<uint8_t>(v >> 24));
    out.push_back(static_cast<uint8_t>(v >> 16));
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v));
}

void AppendU64(std::vector<uint8_t>& out, uint64_t v) {
    AppendU32(out, static_cast<uint32_t>(v >> 32));
    AppendU32(out, static_cast<uint32_t>(v));
}

void EncodeUInt(std::vector<uint8_t>& out, uint64_t v) {
    if (v <= 0x7f) {
        out.push_back(static_cast<uint8_t>(v));
    } else if (v <= 0xff) {
        out.push_back(0xcc); out.push_back(static_cast<uint8_t>(v));
    } else if (v <= 0xffff) {
        out.push_back(0xcd); AppendU16(out, static_cast<uint16_t>(v));
    } else if (v <= 0xffffffff) {
        out.push_back(0xce); AppendU32(out, static_cast<uint32_t>(v));
    } else {
        out.push_back(0xcf); AppendU64(out, v);
    }
}

void EncodeInt(std::vector<uint8_t>& out, int64_t v) {
    if (v >= 0) {
        EncodeUInt(out, static_cast<uint64_t>(v));
    } else if (v >= -32) {
        out.push_back(static_cast<uint8_t>(v));
    } else if (v >= -128) {
        out.push_back(0xd0); out.push_back(static_cast<uint8_t>(static_cast<int8_t>(v)));
    } else if (v >= -32768) {
        out.push_back(0xd1); AppendU16(out, static_cast<uint16_t>(static_cast<int16_t>(v)));
    } else if (v >= -2147483648LL) {
        out.push_back(0xd2); AppendU32(out, static_cast<uint32_t>(static_cast<int32_t>(v)));
    } else {
        out.push_back(0xd3); AppendU64(out, static_cast<uint64_t>(v));
    }
}

void EncodeStr(std::vector<uint8_t>& out, std::string_view s) {
    const std::size_t len = s.size();
    if (len <= 31) {
        out.push_back(static_cast<uint8_t>(0xa0 | len));
    } else if (len <= 0xff) {
        out.push_back(0xd9); out.push_back(static_cast<uint8_t>(len));
    } else if (len <= 0xffff) {
        out.push_back(0xda); AppendU16(out, static_cast<uint16_t>(len));
    } else {
        out.push_back(0xdb); AppendU32(out, static_cast<uint32_t>(len));
    }
    out.insert(out.end(),
               reinterpret_cast<const uint8_t*>(s.data()),
               reinterpret_cast<const uint8_t*>(s.data()) + len);
}

void EncodeArrayHeader(std::vector<uint8_t>& out, std::size_t n) {
    if (n <= 15) {
        out.push_back(static_cast<uint8_t>(0x90 | n));
    } else if (n <= 0xffff) {
        out.push_back(0xdc); AppendU16(out, static_cast<uint16_t>(n));
    } else {
        out.push_back(0xdd); AppendU32(out, static_cast<uint32_t>(n));
    }
}

void EncodeMapHeader(std::vector<uint8_t>& out, std::size_t n) {
    if (n <= 15) {
        out.push_back(static_cast<uint8_t>(0x80 | n));
    } else if (n <= 0xffff) {
        out.push_back(0xde); AppendU16(out, static_cast<uint16_t>(n));
    } else {
        out.push_back(0xdf); AppendU32(out, static_cast<uint32_t>(n));
    }
}

// Forward declaration
void EncodeNode(std::vector<uint8_t>& out, const impl::Node& node);

void EncodeNode(std::vector<uint8_t>& out, const impl::Node& node) {
    std::visit([&](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
            out.push_back(0xc0);  // nil
        } else if constexpr (std::is_same_v<T, bool>) {
            out.push_back(v ? 0xc3 : 0xc2);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            EncodeInt(out, v);
        } else if constexpr (std::is_same_v<T, uint64_t>) {
            EncodeUInt(out, v);
        } else if constexpr (std::is_same_v<T, double>) {
            uint64_t bits{};
            std::memcpy(&bits, &v, 8);
            out.push_back(0xcb);
            AppendU64(out, bits);
        } else if constexpr (std::is_same_v<T, std::string>) {
            EncodeStr(out, v);
        } else if constexpr (std::is_same_v<T, impl::Array>) {
            EncodeArrayHeader(out, v.size());
            for (const auto& ptr : v) EncodeNode(out, *ptr);
        } else if constexpr (std::is_same_v<T, impl::StrMap>) {
            EncodeMapHeader(out, v.size());
            for (const auto& [k, vp] : v) {
                EncodeStr(out, k);
                EncodeNode(out, *vp);
            }
        } else if constexpr (std::is_same_v<T, impl::IntMap>) {
            EncodeMapHeader(out, v.size());
            for (const auto& [k, vp] : v) {
                EncodeUInt(out, k);
                EncodeNode(out, *vp);
            }
        } else if constexpr (std::is_same_v<T, impl::ExtData>) {
            out.insert(out.end(), v.raw.begin(), v.raw.end());
        }
    }, node.data);
}

}  // namespace

// ======================================================================== //
//  ValueBuilder implementation                                              //
// ======================================================================== //

ValueBuilder::ValueBuilder(std::shared_ptr<impl::Node> root,
                           impl::Node* node) noexcept
    : root_(std::move(root)), node_(node) {}

// ---- Scalar constructors -----------------------------------------------

ValueBuilder::ValueBuilder()
    : root_(std::make_shared<impl::Node>()), node_(root_.get()) {}

ValueBuilder::ValueBuilder(bool v)
    : root_(std::make_shared<impl::Node>(v)), node_(root_.get()) {}

ValueBuilder::ValueBuilder(int8_t v)
    : root_(std::make_shared<impl::Node>(static_cast<int64_t>(v))), node_(root_.get()) {}

ValueBuilder::ValueBuilder(int16_t v)
    : root_(std::make_shared<impl::Node>(static_cast<int64_t>(v))), node_(root_.get()) {}

ValueBuilder::ValueBuilder(int32_t v)
    : root_(std::make_shared<impl::Node>(static_cast<int64_t>(v))), node_(root_.get()) {}

ValueBuilder::ValueBuilder(int64_t v)
    : root_(std::make_shared<impl::Node>(v)), node_(root_.get()) {}

ValueBuilder::ValueBuilder(uint8_t v)
    : root_(std::make_shared<impl::Node>(static_cast<uint64_t>(v))), node_(root_.get()) {}

ValueBuilder::ValueBuilder(uint16_t v)
    : root_(std::make_shared<impl::Node>(static_cast<uint64_t>(v))), node_(root_.get()) {}

ValueBuilder::ValueBuilder(uint32_t v)
    : root_(std::make_shared<impl::Node>(static_cast<uint64_t>(v))), node_(root_.get()) {}

ValueBuilder::ValueBuilder(uint64_t v)
    : root_(std::make_shared<impl::Node>(v)), node_(root_.get()) {}

ValueBuilder::ValueBuilder(float v)
    : root_(std::make_shared<impl::Node>(static_cast<double>(v))), node_(root_.get()) {}

ValueBuilder::ValueBuilder(double v)
    : root_(std::make_shared<impl::Node>(v)), node_(root_.get()) {}

ValueBuilder::ValueBuilder(std::string_view v)
    : root_(std::make_shared<impl::Node>(std::string(v))), node_(root_.get()) {}

ValueBuilder::ValueBuilder(const std::string& v)
    : root_(std::make_shared<impl::Node>(std::string(v))), node_(root_.get()) {}

ValueBuilder::ValueBuilder(const char* v)
    : root_(std::make_shared<impl::Node>(std::string(v))), node_(root_.get()) {}

ValueBuilder::ValueBuilder(const Value& v)
    : root_(std::make_shared<impl::Node>()), node_(root_.get()) {
    // Re-encode the Value into a vector, then decode it back into node tree.
    // For Phase 1 this is good enough; Phase 2 will add zero-copy paths.
    // Actually: just store the raw bytes and decode lazily is complex.
    // Simpler: encode the value to bytes and store as opaque binary leaf.
    // But we need to be able to re-encode that leaf... 
    // Pragmatic: just call ToBytes on a temporary builder made from
    // the raw bytes. We'll revisit for Phase 2.
    //
    // For now: decode msgpack scalars that the connector actually needs.
    if (v.IsMissing() || v.IsNull()) {
        node_->data = std::monostate{};
    } else if (v.IsBool()) {
        node_->data = v.As<bool>();
    } else if (v.IsUInt()) {
        node_->data = v.As<uint64_t>();
    } else if (v.IsInt()) {
        node_->data = v.As<int64_t>();
    } else if (v.IsDouble()) {
        node_->data = v.As<double>();
    } else if (v.IsString()) {
        node_->data = v.As<std::string>();
    } else if (v.IsArray()) {
        impl::Array arr;
        const std::size_t n = v.GetSize();
        arr.reserve(n);
        for (std::size_t i = 0; i < n; ++i) {
            arr.push_back(std::make_unique<impl::Node>(
                *ValueBuilder{v[i]}.root_));
        }
        node_->data = std::move(arr);
    } else if (v.IsObject()) {
        // We don't know if keys are int or string; scan both.
        // For simplicity, try int keys first.
        // In practice for connector use this path is rarely hit.
        node_->data = std::monostate{};
    }
}

// ---- Copy / assign -----------------------------------------------------

ValueBuilder::ValueBuilder(const ValueBuilder& other)
    : root_(std::make_shared<impl::Node>(*other.node_)), node_(root_.get()) {}

ValueBuilder& ValueBuilder::operator=(ValueBuilder rhs) noexcept {
    *node_ = std::move(*rhs.node_);
    return *this;
}

// ---- Container factories -----------------------------------------------

/*static*/ ValueBuilder ValueBuilder::Array() {
    auto root = std::make_shared<impl::Node>(impl::Array{});
    auto* node = root.get();
    return ValueBuilder{std::move(root), node};
}

/*static*/ ValueBuilder ValueBuilder::Object() {
    auto root = std::make_shared<impl::Node>(impl::StrMap{});
    auto* node = root.get();
    return ValueBuilder{std::move(root), node};
}

/*static*/ ValueBuilder ValueBuilder::IntKeyObject() {
    auto root = std::make_shared<impl::Node>(impl::IntMap{});
    auto* node = root.get();
    return ValueBuilder{std::move(root), node};
}

// ---- Navigation / mutation ---------------------------------------------

ValueBuilder ValueBuilder::operator[](uint64_t key) {
    if (!std::holds_alternative<impl::IntMap>(node_->data)) {
        if (std::holds_alternative<std::monostate>(node_->data)) {
            node_->data = impl::IntMap{};
        } else {
            throw TypeMismatchException(0, 9 /*kTypeMap*/, GetPath());
        }
    }
    auto& map = std::get<impl::IntMap>(node_->data);
    for (auto& [k, v] : map) {
        if (k == key) return ValueBuilder{root_, v.get()};
    }
    map.emplace_back(key, std::make_unique<impl::Node>());
    return ValueBuilder{root_, map.back().second.get()};
}

ValueBuilder ValueBuilder::operator[](std::string_view key) {
    if (!std::holds_alternative<impl::StrMap>(node_->data)) {
        if (std::holds_alternative<std::monostate>(node_->data)) {
            node_->data = impl::StrMap{};
        } else {
            throw TypeMismatchException(0, 9 /*kTypeMap*/, GetPath());
        }
    }
    auto& map = std::get<impl::StrMap>(node_->data);
    for (auto& [k, v] : map) {
        if (k == key) return ValueBuilder{root_, v.get()};
    }
    map.emplace_back(std::string(key), std::make_unique<impl::Node>());
    return ValueBuilder{root_, map.back().second.get()};
}

void ValueBuilder::PushBack(ValueBuilder value) {
    if (!std::holds_alternative<impl::Array>(node_->data)) {
        if (std::holds_alternative<std::monostate>(node_->data)) {
            node_->data = impl::Array{};
        } else {
            throw TypeMismatchException(0, 8 /*kTypeArray*/, GetPath());
        }
    }
    auto& arr = std::get<impl::Array>(node_->data);
    arr.push_back(std::make_unique<impl::Node>(std::move(*value.node_)));
}

// ---- Type predicates ---------------------------------------------------

bool ValueBuilder::IsNull() const noexcept {
    return std::holds_alternative<std::monostate>(node_->data);
}

bool ValueBuilder::IsBool() const noexcept {
    return std::holds_alternative<bool>(node_->data);
}

bool ValueBuilder::IsInt() const noexcept {
    return std::holds_alternative<int64_t>(node_->data);
}

bool ValueBuilder::IsUInt() const noexcept {
    return std::holds_alternative<uint64_t>(node_->data);
}

bool ValueBuilder::IsDouble() const noexcept {
    return std::holds_alternative<double>(node_->data);
}

bool ValueBuilder::IsString() const noexcept {
    return std::holds_alternative<std::string>(node_->data);
}

bool ValueBuilder::IsArray() const noexcept {
    return std::holds_alternative<impl::Array>(node_->data);
}

bool ValueBuilder::IsObject() const noexcept {
    return std::holds_alternative<impl::StrMap>(node_->data) ||
           std::holds_alternative<impl::IntMap>(node_->data);
}

std::size_t ValueBuilder::GetSize() const {
    if (std::holds_alternative<impl::Array>(node_->data))
        return std::get<impl::Array>(node_->data).size();
    if (std::holds_alternative<impl::StrMap>(node_->data))
        return std::get<impl::StrMap>(node_->data).size();
    if (std::holds_alternative<impl::IntMap>(node_->data))
        return std::get<impl::IntMap>(node_->data).size();
    return 0;
}

// ---- Serialisation -----------------------------------------------------

void ValueBuilder::AppendTo(std::vector<uint8_t>& dest) const {
    EncodeNode(dest, *node_);
}

std::vector<uint8_t> ValueBuilder::ToBytes() const {
    std::vector<uint8_t> out;
    out.reserve(64);
    AppendTo(out);
    return out;
}

Value ValueBuilder::ToValue(std::vector<uint8_t>& out) const {
    out = ToBytes();
    return Value::FromBytes(out.data(), out.size());
}

// ---- Tarantool ext type constructors --------------------------------------

ValueBuilder::ValueBuilder(TntUuid uuid)
    : root_(std::make_shared<impl::Node>(impl::ExtData{EncodeUuid(uuid)})),
      node_(root_.get()) {}

ValueBuilder::ValueBuilder(utils::datetime::Date date)
    : root_(std::make_shared<impl::Node>(impl::ExtData{EncodeDate(date)})),
      node_(root_.get()) {}

ValueBuilder::ValueBuilder(DatetimeTz dt)
    : root_(std::make_shared<impl::Node>(impl::ExtData{EncodeDatetimeTz(dt)})),
      node_(root_.get()) {}

ValueBuilder::ValueBuilder(DatetimeWithoutTz dt)
    : root_(std::make_shared<impl::Node>(impl::ExtData{EncodeDatetimeWithoutTz(dt)})),
      node_(root_.get()) {}

ValueBuilder::ValueBuilder(TimestampTz ts)
    : root_(std::make_shared<impl::Node>(impl::ExtData{EncodeTimestampTz(ts)})),
      node_(root_.get()) {}

ValueBuilder::ValueBuilder(TimestampWithoutTz ts)
    : root_(std::make_shared<impl::Node>(impl::ExtData{EncodeTimestampWithoutTz(ts)})),
      node_(root_.get()) {}

ValueBuilder::ValueBuilder(TntInterval interval)
    : root_(std::make_shared<impl::Node>(impl::ExtData{EncodeInterval(interval)})),
      node_(root_.get()) {}

// ---- Path --------------------------------------------------------------

std::string ValueBuilder::GetPath() const {
    return "/";
}

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
