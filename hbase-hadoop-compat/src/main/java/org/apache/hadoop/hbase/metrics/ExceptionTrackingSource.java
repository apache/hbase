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
 */

package org.apache.hadoop.hbase.metrics;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Common interface for metrics source implementations which need to track individual exception
 * types thrown or received.
 */
@InterfaceAudience.Private
public interface ExceptionTrackingSource extends BaseSource {
  String EXCEPTIONS_NAME="exceptions";
  String EXCEPTIONS_DESC="Exceptions caused by requests";
  String EXCEPTIONS_TYPE_DESC="Number of requests that resulted in the specified type of Exception";
  String EXCEPTIONS_OOO_NAME="exceptions.OutOfOrderScannerNextException";
  String EXCEPTIONS_BUSY_NAME="exceptions.RegionTooBusyException";
  String EXCEPTIONS_UNKNOWN_NAME="exceptions.UnknownScannerException";
  String EXCEPTIONS_SCANNER_RESET_NAME="exceptions.ScannerResetException";
  String EXCEPTIONS_SANITY_NAME="exceptions.FailedSanityCheckException";
  String EXCEPTIONS_MOVED_NAME="exceptions.RegionMovedException";
  String EXCEPTIONS_NSRE_NAME="exceptions.NotServingRegionException";
  String EXCEPTIONS_MULTI_TOO_LARGE_NAME = "exceptions.multiResponseTooLarge";
  String EXCEPTIONS_MULTI_TOO_LARGE_DESC = "A response to a multi request was too large and the " +
      "rest of the requests will have to be retried.";
  String EXCEPTIONS_CALL_QUEUE_TOO_BIG = "exceptions.callQueueTooBig";
  String EXCEPTIONS_CALL_QUEUE_TOO_BIG_DESC = "Call queue is full";
  String EXCEPTIONS_QUOTA_EXCEEDED = "exceptions.quotaExceeded";
  String EXCEPTIONS_RPC_THROTTLING = "exceptions.rpcThrottling";
  String EXCEPTIONS_REQUEST_TOO_BIG = "exceptions.requestTooBig";
  String OTHER_EXCEPTIONS = "exceptions.otherExceptions";

  void exception();

  /**
   * Different types of exceptions
   */
  void outOfOrderException();
  void failedSanityException();
  void movedRegionException();
  void notServingRegionException();
  void unknownScannerException();
  void scannerResetException();
  void tooBusyException();
  void multiActionTooLargeException();
  void callQueueTooBigException();
  void quotaExceededException();
  void rpcThrottlingException();
  void requestTooBigException();
  void otherExceptions();
}
