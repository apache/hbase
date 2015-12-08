/**
 *
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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;

@InterfaceAudience.Private
public class MetricsHBaseServer {
  private MetricsHBaseServerSource source;

  public MetricsHBaseServer(String serverName, MetricsHBaseServerWrapper wrapper) {
    source = CompatibilitySingletonFactory.getInstance(MetricsHBaseServerSourceFactory.class)
                                          .create(serverName, wrapper);
  }

  void authorizationSuccess() {
    source.authorizationSuccess();
  }

  void authorizationFailure() {
    source.authorizationFailure();
  }

  void authenticationFailure() {
    source.authenticationFailure();
  }

  void authenticationSuccess() {
    source.authenticationSuccess();
  }

  void authenticationFallback() {
    source.authenticationFallback();
  }

  void sentBytes(long count) {
    source.sentBytes(count);
  }

  void receivedBytes(int count) {
    source.receivedBytes(count);
  }

  void sentResponse(long count) { source.sentResponse(count); }

  void receivedRequest(long count) { source.receivedRequest(count); }

  void dequeuedCall(int qTime) {
    source.dequeuedCall(qTime);
  }

  void processedCall(int processingTime) {
    source.processedCall(processingTime);
  }

  void totalCall(int totalTime) {
    source.queuedAndProcessedCall(totalTime);
  }

  public void exception(Throwable throwable) {
    source.exception();

    /**
     * Keep some metrics for commonly seen exceptions
     *
     * Try and  put the most common types first.
     * Place child types before the parent type that they extend.
     *
     * If this gets much larger we might have to go to a hashmap
     */
    if (throwable != null) {
      if (throwable instanceof OutOfOrderScannerNextException) {
        source.outOfOrderException();
      } else if (throwable instanceof RegionTooBusyException) {
        source.tooBusyException();
      } else if (throwable instanceof UnknownScannerException) {
        source.unknownScannerException();
      } else if (throwable instanceof RegionMovedException) {
        source.movedRegionException();
      } else if (throwable instanceof NotServingRegionException) {
        source.notServingRegionException();
      } else if (throwable instanceof FailedSanityCheckException) {
        source.failedSanityException();
      } else if (throwable instanceof MultiActionResultTooLarge) {
        source.multiActionTooLargeException();
      }
    }
  }

  public MetricsHBaseServerSource getMetricsSource() {
    return source;
  }
}
