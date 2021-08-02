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

package org.apache.hadoop.hbase.thrift;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.metrics.ExceptionTrackingSource;
import org.apache.hadoop.hbase.quotas.QuotaExceededException;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Simple test coprocessor for injecting exceptions on Get requests.
 */
@CoreCoprocessor
public class ErrorThrowingGetObserver implements RegionCoprocessor, RegionObserver {
  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  public static final String SHOULD_ERROR_ATTRIBUTE = "error";

  @Override
  public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
                       Get get, List<Cell> results) throws IOException {
    byte[] errorType = get.getAttribute(SHOULD_ERROR_ATTRIBUTE);
    if (errorType != null) {
      ErrorType type = ErrorType.valueOf(Bytes.toString(errorType));
      switch (type) {
        case CALL_QUEUE_TOO_BIG:
          throw new CallQueueTooBigException("Failing for test");
        case MULTI_ACTION_RESULT_TOO_LARGE:
          throw new MultiActionResultTooLarge("Failing for test");
        case FAILED_SANITY_CHECK:
          throw new FailedSanityCheckException("Failing for test");
        case NOT_SERVING_REGION:
          throw new NotServingRegionException("Failing for test");
        case REGION_MOVED:
          throw new RegionMovedException(e.getEnvironment().getServerName(), 1);
        case SCANNER_RESET:
          throw new ScannerResetException("Failing for test");
        case UNKNOWN_SCANNER:
          throw new UnknownScannerException("Failing for test");
        case REGION_TOO_BUSY:
          throw new RegionTooBusyException("Failing for test");
        case OUT_OF_ORDER_SCANNER_NEXT:
          throw new OutOfOrderScannerNextException("Failing for test");
        case QUOTA_EXCEEDED:
          throw new QuotaExceededException("Failing for test");
        case RPC_THROTTLING:
          throw new RpcThrottlingException("Failing for test");
        default:
          throw new DoNotRetryIOException("Failing for test");
      }
    }
  }

  public enum ErrorType {
    CALL_QUEUE_TOO_BIG(ExceptionTrackingSource.EXCEPTIONS_CALL_QUEUE_TOO_BIG),
    MULTI_ACTION_RESULT_TOO_LARGE(ExceptionTrackingSource.EXCEPTIONS_MULTI_TOO_LARGE_NAME),
    FAILED_SANITY_CHECK(ExceptionTrackingSource.EXCEPTIONS_SANITY_NAME),
    NOT_SERVING_REGION(ExceptionTrackingSource.EXCEPTIONS_NSRE_NAME),
    REGION_MOVED(ExceptionTrackingSource.EXCEPTIONS_MOVED_NAME),
    SCANNER_RESET(ExceptionTrackingSource.EXCEPTIONS_SCANNER_RESET_NAME),
    UNKNOWN_SCANNER(ExceptionTrackingSource.EXCEPTIONS_UNKNOWN_NAME),
    REGION_TOO_BUSY(ExceptionTrackingSource.EXCEPTIONS_BUSY_NAME),
    OUT_OF_ORDER_SCANNER_NEXT(ExceptionTrackingSource.EXCEPTIONS_OOO_NAME),
    QUOTA_EXCEEDED(ExceptionTrackingSource.EXCEPTIONS_QUOTA_EXCEEDED),
    RPC_THROTTLING(ExceptionTrackingSource.EXCEPTIONS_RPC_THROTTLING);

    private final String metricName;

    ErrorType(String metricName) {
      this.metricName = metricName;
    }

    public String getMetricName() {
      return metricName;
    }
  }
}
