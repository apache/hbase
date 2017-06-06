/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#pragma once

#include <folly/ExceptionWrapper.h>
#include <folly/io/IOBuf.h>
#include <exception>
#include <string>
#include <vector>

namespace hbase {

class ThrowableWithExtraContext {
 public:
  ThrowableWithExtraContext(folly::exception_wrapper cause, const long& when)
      : cause_(cause), when_(when), extras_("") {}

  ThrowableWithExtraContext(folly::exception_wrapper cause, const long& when,
                            const std::string& extras)
      : cause_(cause), when_(when), extras_(extras) {}

  virtual std::string ToString() {
    // TODO:
    // return new Date(this.when).toString() + ", " + extras + ", " + t.toString();
    return extras_ + ", " + cause_.what().toStdString();
  }

  virtual folly::exception_wrapper cause() { return cause_; }

 private:
  folly::exception_wrapper cause_;
  long when_;
  std::string extras_;
};

class IOException : public std::logic_error {
 public:
  IOException() : logic_error("") {}

  IOException(const std::string& what) : logic_error(what) {}
  IOException(const std::string& what, folly::exception_wrapper cause)
      : logic_error(what), cause_(cause) {}
  virtual ~IOException() = default;

  virtual folly::exception_wrapper cause() { return cause_; }

 private:
  folly::exception_wrapper cause_;
};

class RetriesExhaustedException : public IOException {
 public:
  RetriesExhaustedException(const int& num_retries,
                            std::shared_ptr<std::vector<ThrowableWithExtraContext>> exceptions)
      : IOException(GetMessage(num_retries, exceptions),
                    exceptions->empty() ? folly::exception_wrapper{}
                                        : (*exceptions)[exceptions->size() - 1].cause()) {}
  virtual ~RetriesExhaustedException() = default;

 private:
  std::string GetMessage(const int& num_retries,
                         std::shared_ptr<std::vector<ThrowableWithExtraContext>> exceptions) {
    std::string buffer("Failed after attempts=");
    buffer.append(std::to_string(num_retries + 1));
    buffer.append(", exceptions:\n");
    for (auto it = exceptions->begin(); it != exceptions->end(); it++) {
      buffer.append(it->ToString());
      buffer.append("\n");
    }
    return buffer;
  }
};

class HBaseIOException : public IOException {};

class RemoteException : public IOException {
 public:
  RemoteException() : port_(0), do_not_retry_(false) {}

  RemoteException(const std::string& what) : IOException(what), port_(0), do_not_retry_(false) {}

  RemoteException(const std::string& what, folly::exception_wrapper cause)
      : IOException(what, cause), port_(0), do_not_retry_(false) {}

  virtual ~RemoteException() = default;

  std::string exception_class_name() const { return exception_class_name_; }

  RemoteException* set_exception_class_name(const std::string& value) {
    exception_class_name_ = value;
    return this;
  }

  std::string stack_trace() const { return stack_trace_; }

  RemoteException* set_stack_trace(const std::string& value) {
    stack_trace_ = value;
    return this;
  }

  std::string hostname() const { return hostname_; }

  RemoteException* set_hostname(const std::string& value) {
    hostname_ = value;
    return this;
  }

  int port() const { return port_; }

  RemoteException* set_port(int value) {
    port_ = value;
    return this;
  }

  bool do_not_retry() const { return do_not_retry_; }

  RemoteException* set_do_not_retry(bool value) {
    do_not_retry_ = value;
    return this;
  }

 private:
  std::string exception_class_name_;
  std::string stack_trace_;
  std::string hostname_;
  int port_;
  bool do_not_retry_;
};

/**
 * List of known exceptions from Java side, and Java-specific exception logic
 */
class ExceptionUtil {
 private:
  // unknown scanner and sub-classes
  static constexpr const char* kUnknownScannerException =
      "org.apache.hadoop.hbase.UnknownScannerException";

  // not serving region and sub-classes
  static constexpr const char* kNotServingRegionException =
      "org.apache.hadoop.hbase.NotServingRegionException";
  static constexpr const char* kRegionInRecoveryException =
      "org.apache.hadoop.hbase.exceptions.RegionInRecoveryException";
  static constexpr const char* kRegionOpeningException =
      "org.apache.hadoop.hbase.exceptions.RegionOpeningException";
  static constexpr const char* kRegionMovedException =
      "org.apache.hadoop.hbase.exceptions.RegionMovedException";

  // Region server stopped and sub-classes
  static constexpr const char* kRegionServerStoppedException =
      "org.apache.hadoop.hbase.regionserver.RegionServerStoppedException";
  static constexpr const char* kRegionServerAbortedException =
      "org.apache.hadoop.hbase.regionserver.RegionServerAbortedException";

  // other scanner related exceptions
  static constexpr const char* kOutOfOrderScannerNextException =
      "org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException";
  static constexpr const char* kScannerResetException =
      "org.apache.hadoop.hbase.exceptions.ScannerResetException";

 public:
  /**
   * Returns whether or not the exception should be retried by looking at the
   * remote exception.
   */
  static bool ShouldRetry(const folly::exception_wrapper& error) {
    bool do_not_retry = false;
    error.with_exception(
        [&](const RemoteException& remote_ex) { do_not_retry = remote_ex.do_not_retry(); });
    return !do_not_retry;
  }

  /**
   * Returns whether the scanner is closed when the client received the
   * remote exception.
   * Ok, here is a nice detail about the java exceptions. In the java side, we
   * just have a hierarchy of Exception classes that we use both client side and
   * server side. On the client side, we rethrow the server side exception by
   * un-wrapping the exception from a RemoteException or a ServiceException
   * (see ConnectionUtils.translateException() in Java).
   * Since this object-hierarchy info is not available in C++ side, we are doing a
   * very fragile catch-all list of all exception types in Java that extend these
   * three base classes: UnknownScannerException, NotServingRegionException,
   * RegionServerStoppedException
   */
  static bool IsScannerClosed(const folly::exception_wrapper& exception) {
    bool scanner_closed = false;
    exception.with_exception([&](const RemoteException& remote_ex) {
      auto java_class = remote_ex.exception_class_name();
      if (java_class == kUnknownScannerException || java_class == kNotServingRegionException ||
          java_class == kRegionInRecoveryException || java_class == kRegionOpeningException ||
          java_class == kRegionMovedException || java_class == kRegionServerStoppedException ||
          java_class == kRegionServerAbortedException) {
        scanner_closed = true;
      }
    });
    return scanner_closed;
  }

  static bool IsScannerOutOfOrder(const folly::exception_wrapper& exception) {
    bool scanner_out_of_order = false;
    exception.with_exception([&](const RemoteException& remote_ex) {
      auto java_class = remote_ex.exception_class_name();
      if (java_class == kOutOfOrderScannerNextException || java_class == kScannerResetException) {
        scanner_out_of_order = true;
      }
    });
    return scanner_out_of_order;
  }
};
}  // namespace hbase
