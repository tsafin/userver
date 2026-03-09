#pragma once

/// @file userver/formats/msgpack/exception.hpp
/// @brief Exceptions for formats::msgpack

#include <exception>
#include <string>
#include <string_view>

USERVER_NAMESPACE_BEGIN

namespace formats::msgpack {

/// @brief Base exception for all msgpack format errors.
class Exception : public std::exception {
public:
    explicit Exception(std::string_view msg) : msg_(msg) {}

    const char* what() const noexcept override { return msg_.c_str(); }
    std::string_view GetMessage() const noexcept { return msg_; }

private:
    std::string msg_;
};

/// @brief Exception that includes the path to the failing field.
class ExceptionWithPath : public Exception {
public:
    ExceptionWithPath(std::string_view msg, std::string_view path);

    std::string_view GetPath() const noexcept;
    std::string_view GetMessageWithoutPath() const noexcept;

private:
    std::size_t path_size_;
};

/// @brief Thrown when the actual type doesn't match the expected type.
class TypeMismatchException : public ExceptionWithPath {
public:
    TypeMismatchException(int actual, int expected, std::string_view path);

    int GetActual() const noexcept { return actual_; }
    int GetExpected() const noexcept { return expected_; }

private:
    int actual_;
    int expected_;
};

/// @brief Thrown when an array index is out of bounds.
class OutOfBoundsException : public ExceptionWithPath {
public:
    OutOfBoundsException(std::size_t index, std::size_t size,
                         std::string_view path);
};

/// @brief Thrown when a required map key is absent.
class MemberMissingException : public ExceptionWithPath {
public:
    explicit MemberMissingException(std::string_view path);
};

/// @brief Thrown when a value cannot be converted to the requested C++ type.
class ConversionException : public ExceptionWithPath {
public:
    ConversionException(std::string_view msg, std::string_view path);
};

/// @brief Thrown when the msgpack buffer is truncated or malformed.
class ParseException : public Exception {
public:
    explicit ParseException(std::string_view msg) : Exception(msg) {}
};

}  // namespace formats::msgpack

USERVER_NAMESPACE_END
