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
#include <memory>
#include <string>
#include <vector>

namespace hbase {

class ThrowableWithExtraContext {
 public:
  ThrowableWithExtraContext(folly::exception_wrapper cause, const int64_t& when)
      : cause_(cause), when_(when), extras_("") {}

  ThrowableWithExtraContext(folly::exception_wrapper cause, const int64_t& when,
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
  int64_t when_;
  std::string extras_;
};

class IOException : public std::logic_error {
 public:
  IOException() : logic_error(""), do_not_retry_(false) {}

  explicit IOException(const std::string& what) : logic_error(what), do_not_retry_(false) {}

  IOException(const std::string& what, bool do_not_retry)
      : logic_error(what), do_not_retry_(do_not_retry) {}

  IOException(const std::string& what, folly::exception_wrapper cause)
      : logic_error(what), cause_(cause), do_not_retry_(false) {}

  IOException(const std::string& what, folly::exception_wrapper cause, bool do_not_retry)
      : logic_error(what), cause_(cause), do_not_retry_(do_not_retry) {}

  virtual ~IOException() = default;

  virtual folly::exception_wrapper cause() { return cause_; }

  bool do_not_retry() const { return do_not_retry_; }

  IOException* set_do_not_retry(bool value) {
    do_not_retry_ = value;
    return this;
  }

 private:
  folly::exception_wrapper cause_;
  // In case the exception is a RemoteException, do_not_retry information can come from
  // the PB field in the RPC response, or it can be deduced from the Java-exception
  // hierarchy in ExceptionUtil::ShouldRetry(). In case this is a client-side exception
  // raised from the C++ internals, set this field so that the retrying callers can
  // re-throw the exception without retrying.
  bool do_not_retry_;
};

class RetriesExhaustedException : public IOException {
 public:
  RetriesExhaustedException(const int& num_retries,
                            std::shared_ptr<std::vector<ThrowableWithExtraContext>> exceptions)
      : IOException(GetMessage(num_retries, exceptions),
                    exceptions->empty() ? folly::exception_wrapper{}
                                        : (*exceptions)[exceptions->size() - 1].cause()),
        num_retries_(num_retries) {}
  virtual ~RetriesExhaustedException() = default;

  int32_t num_retries() const { return num_retries_; }

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

 private:
  int32_t num_retries_;
};

class RemoteException : public IOException {
 public:
  RemoteException() : IOException(), port_(0) {}

  explicit RemoteException(const std::string& what) : IOException(what), port_(0) {}

  RemoteException(const std::string& what, folly::exception_wrapper cause)
      : IOException(what, cause), port_(0) {}

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

 private:
  std::string exception_class_name_;
  std::string stack_trace_;
  std::string hostname_;
  int port_;
};

/**
 * Raised from the client side if we cannot find the table (does not have anything to
 * do with the Java exception of the same name).
 */
class TableNotFoundException : public IOException {
 public:
  explicit TableNotFoundException(const std::string& table_name)
      : IOException("Table cannot be found:" + table_name, true) {}

  virtual ~TableNotFoundException() = default;
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

  // All other DoNotRetryIOExceptions
  static constexpr const char* kDoNotRetryIOException =
      "org.apache.hadoop.hbase.DoNotRetryIOException";
  static constexpr const char* kTableNotFoundException =
      "org.apache.hadoop.hbase.TableNotFoundException";
  static constexpr const char* kTableNotEnabledException =
      "org.apache.hadoop.hbase.TableNotEnabledException";
  static constexpr const char* kCoprocessorException =
      "org.apache.hadoop.hbase.coprocessor.CoprocessorException";
  static constexpr const char* kBypassCoprocessorException =
      "org.apache.hadoop.hbase.coprocessor.BypassCoprocessorException";
  static constexpr const char* kInvalidFamilyOperationException =
      "org.apache.hadoop.hbase.InvalidFamilyOperationException";
  static constexpr const char* kServerTooBusyException =
      "org.apache.hadoop.hbase.ipc.ServerTooBusyException";  // This should NOT be DNRIOE?
  static constexpr const char* kFailedSanityCheckException =
      "org.apache.hadoop.hbase.exceptions.FailedSanityCheckException";
  static constexpr const char* kCorruptHFileException =
      "org.apache.hadoop.hbase.io.hfile.CorruptHFileException";
  static constexpr const char* kLabelAlreadyExistsException =
      "org.apache.hadoop.hbase.security.visibility.LabelAlreadyExistsException";
  static constexpr const char* kFatalConnectionException =
      "org.apache.hadoop.hbase.ipc.FatalConnectionException";
  static constexpr const char* kUnsupportedCryptoException =
      "org.apache.hadoop.hbase.ipc.UnsupportedCryptoException";
  static constexpr const char* kUnsupportedCellCodecException =
      "org.apache.hadoop.hbase.ipc.UnsupportedCellCodecException";
  static constexpr const char* kEmptyServiceNameException =
      "org.apache.hadoop.hbase.ipc.EmptyServiceNameException";
  static constexpr const char* kUnknownServiceException =
      "org.apache.hadoop.hbase.ipc.UnknownServiceException";
  static constexpr const char* kWrongVersionException =
      "org.apache.hadoop.hbase.ipc.WrongVersionException";
  static constexpr const char* kBadAuthException = "org.apache.hadoop.hbase.ipc.BadAuthException";
  static constexpr const char* kUnsupportedCompressionCodecException =
      "org.apache.hadoop.hbase.ipc.UnsupportedCompressionCodecException";
  static constexpr const char* kDoNotRetryRegionException =
      "org.apache.hadoop.hbase.client.DoNotRetryRegionException";
  static constexpr const char* kRowTooBigException =
      "org.apache.hadoop.hbase.client.RowTooBigException";
  static constexpr const char* kRowTooBigExceptionDeprecated =
      "org.apache.hadoop.hbase.regionserver.RowTooBigException";
  static constexpr const char* kUnknownRegionException =
      "org.apache.hadoop.hbase.UnknownRegionException";
  static constexpr const char* kMergeRegionException =
      "org.apache.hadoop.hbase.exceptions.MergeRegionException";
  static constexpr const char* kNoServerForRegionException =
      "org.apache.hadoop.hbase.client.NoServerForRegionException";
  static constexpr const char* kQuotaExceededException =
      "org.apache.hadoop.hbase.quotas.QuotaExceededException";
  static constexpr const char* kSpaceLimitingException =
      "org.apache.hadoop.hbase.quotas.SpaceLimitingException";
  static constexpr const char* kThrottlingException =
      "org.apache.hadoop.hbase.quotas.ThrottlingException";
  static constexpr const char* kAccessDeniedException =
      "org.apache.hadoop.hbase.security.AccessDeniedException";
  static constexpr const char* kUnknownProtocolException =
      "org.apache.hadoop.hbase.exceptions.UnknownProtocolException";
  static constexpr const char* kRequestTooBigException =
      "org.apache.hadoop.hbase.exceptions.RequestTooBigException";
  static constexpr const char* kNotAllMetaRegionsOnlineException =
      "org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException";
  static constexpr const char* kConstraintException =
      "org.apache.hadoop.hbase.constraint.ConstraintException";
  static constexpr const char* kNoSuchColumnFamilyException =
      "org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException";
  static constexpr const char* kLeaseException =
      "org.apache.hadoop.hbase.regionserver.LeaseException";
  static constexpr const char* kInvalidLabelException =
      "org.apache.hadoop.hbase.security.visibility.InvalidLabelException";

  // TODO:
  // These exceptions are not thrown in the regular read / write paths, although they are
  // DoNotRetryIOExceptions. Add these to the list below in case we start doing Admin/DDL ops
  // ReplicationPeerNotFoundException, XXXSnapshotException, NamespaceExistException,
  // NamespaceNotFoundException, TableExistsException, TableNotDisabledException,
  static const std::vector<const char*> kAllDoNotRetryIOExceptions;

 public:
  /**
   * Returns whether or not the exception should be retried by looking at the
   * client-side IOException, or RemoteException coming from server side.
   */
  static bool ShouldRetry(const folly::exception_wrapper& error);

  /**
   * Returns whether the java exception class extends DoNotRetryException.
   * In the java side, we just have a hierarchy of Exception classes that we use
   * both client side and server side. On the client side, we rethrow the server
   * side exception by un-wrapping the exception from a RemoteException or a ServiceException
   * (see ConnectionUtils.translateException() in Java).
   * Since this object-hierarchy info is not available in C++ side, we are doing a
   * very fragile catch-all list of all exception types in Java that extend the
   * DoNotRetryException class type.
   */
  static bool IsJavaDoNotRetryException(const std::string& java_class_name);

  /**
   * Returns whether the scanner is closed when the client received the
   * remote exception.
   * Since the object-hierarchy info is not available in C++ side, we are doing a
   * very fragile catch-all list of all exception types in Java that extend these
   * three base classes: UnknownScannerException, NotServingRegionException,
   * RegionServerStoppedException
   */
  static bool IsScannerClosed(const folly::exception_wrapper& exception);

  static bool IsScannerOutOfOrder(const folly::exception_wrapper& exception);
};
}  // namespace hbase
