#include <userver/formats/msgpack/exception.hpp>

#include <string>

#include <fmt/format.h>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

namespace {

constexpr std::string_view kErrorAtPath1 = "Error at path '";
constexpr std::string_view kErrorAtPath2 = "': ";

std::string_view TypeName(int t) noexcept {
    switch (t) {
        case 0:  return "null";
        case 1:  return "bool";
        case 2:  return "int";
        case 3:  return "uint";
        case 4:  return "float";
        case 5:  return "double";
        case 6:  return "str";
        case 7:  return "bin";
        case 8:  return "array";
        case 9:  return "map";
        case 10: return "ext";
        default: return "unknown";
    }
}

}  // namespace

ExceptionWithPath::ExceptionWithPath(std::string_view msg, std::string_view path)
    : Exception(std::string(kErrorAtPath1) + std::string(path) +
                std::string(kErrorAtPath2) + std::string(msg)),
      path_size_(path.size())
{}

std::string_view ExceptionWithPath::GetPath() const noexcept {
    return GetMessage().substr(kErrorAtPath1.size(), path_size_);
}

std::string_view ExceptionWithPath::GetMessageWithoutPath() const noexcept {
    return GetMessage().substr(path_size_ + kErrorAtPath1.size() +
                               kErrorAtPath2.size());
}

TypeMismatchException::TypeMismatchException(int actual, int expected,
                                             std::string_view path)
    : ExceptionWithPath(
          fmt::format("Wrong type. Expected: {}, actual: {}",
                      TypeName(expected), TypeName(actual)),
          path),
      actual_(actual),
      expected_(expected)
{}

OutOfBoundsException::OutOfBoundsException(std::size_t index, std::size_t size,
                                           std::string_view path)
    : ExceptionWithPath(
          fmt::format("Index {} of array of size {} is out of bounds",
                      index, size),
          path)
{}

MemberMissingException::MemberMissingException(std::string_view path)
    : ExceptionWithPath("Field is missing", path)
{}

ConversionException::ConversionException(std::string_view msg,
                                         std::string_view path)
    : ExceptionWithPath(msg, path)
{}

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
