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
#include "exceptions/exception.h"

namespace hbase {
const std::vector<const char*> ExceptionUtil::kAllDoNotRetryIOExceptions = {
    kDoNotRetryIOException,
    kTableNotFoundException,
    kTableNotEnabledException,
    kCoprocessorException,
    kBypassCoprocessorException,
    kInvalidFamilyOperationException,
    kServerTooBusyException,
    kFailedSanityCheckException,
    kCorruptHFileException,
    kLabelAlreadyExistsException,
    kFatalConnectionException,
    kUnsupportedCryptoException,
    kUnsupportedCellCodecException,
    kEmptyServiceNameException,
    kUnknownServiceException,
    kWrongVersionException,
    kBadAuthException,
    kUnsupportedCompressionCodecException,
    kDoNotRetryRegionException,
    kRowTooBigException,
    kRowTooBigExceptionDeprecated,
    kUnknownRegionException,
    kMergeRegionException,
    kNoServerForRegionException,
    kQuotaExceededException,
    kSpaceLimitingException,
    kThrottlingException,
    kAccessDeniedException,
    kUnknownProtocolException,
    kRequestTooBigException,
    kNotAllMetaRegionsOnlineException,
    kConstraintException,
    kNoSuchColumnFamilyException,
    kLeaseException,
    kInvalidLabelException,
    kUnknownScannerException,
    kScannerResetException,
    kOutOfOrderScannerNextException};

bool ExceptionUtil::ShouldRetry(const folly::exception_wrapper& error) {
  bool do_not_retry = false;
  error.with_exception(
      [&](const IOException& ioe) { do_not_retry = do_not_retry || ioe.do_not_retry(); });
  error.with_exception([&](const RemoteException& remote_ex) {
    do_not_retry = do_not_retry || IsJavaDoNotRetryException(remote_ex.exception_class_name());
  });
  return !do_not_retry;
}

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
bool ExceptionUtil::IsJavaDoNotRetryException(const std::string& java_class_name) {
  for (auto exception : kAllDoNotRetryIOExceptions) {
    if (java_class_name == exception) {
      return true;
    }
  }
  return false;
}

/**
 * Returns whether the scanner is closed when the client received the
 * remote exception.
 * Since the object-hierarchy info is not available in C++ side, we are doing a
 * very fragile catch-all list of all exception types in Java that extend these
 * three base classes: UnknownScannerException, NotServingRegionException,
 * RegionServerStoppedException
 */
bool ExceptionUtil::IsScannerClosed(const folly::exception_wrapper& exception) {
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

/**
 * Returns whether the wrapped exception is a java exception of type OutOfOrderScannerNextException
 * or ScannerResetException. These two exception types are thrown from the server side when the
 * scanner on the server side is closed.
 */
bool ExceptionUtil::IsScannerOutOfOrder(const folly::exception_wrapper& exception) {
  bool scanner_out_of_order = false;
  exception.with_exception([&](const RemoteException& remote_ex) {
    auto java_class = remote_ex.exception_class_name();
    if (java_class == kOutOfOrderScannerNextException || java_class == kScannerResetException) {
      scanner_out_of_order = true;
    }
  });
  return scanner_out_of_order;
}
}  // namespace hbase
