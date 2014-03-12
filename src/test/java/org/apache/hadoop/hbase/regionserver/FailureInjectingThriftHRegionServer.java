/*
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;

import java.io.IOException;
import java.util.ArrayList;

/**
 * This class simulates different failure case on server side for unit tests.
 */
public class FailureInjectingThriftHRegionServer extends ThriftHRegionServer {
  public static final Log LOG = LogFactory.getLog(FailureInjectingThriftHRegionServer.class);

  public FailureInjectingThriftHRegionServer(HRegionServer server) {
    super(server);
  }

  public enum FailureType {
    NONE,
    REGIONOVERLOADEDEXCEPTION,
    STOP,
    MIXEDRETRIABLEEXCEPTIONS,
    DONOTRETRYEXCEPTION,
  }

  private static final ArrayList<Exception> retriableExceptions = new ArrayList<>();

  private volatile static FailureType failureType = FailureType.NONE;
  private volatile static int repeats = 0;
  private volatile static int repeatCount = 0;

  public static void setFailureMode(FailureType t, int r) {
    failureType = t;
    repeats = r;
    repeatCount = 0;
  }

  private static void clearExceptionMode() {
    setFailureMode(FailureType.NONE, 0);
  }

  @Override
  public ListenableFuture<Result> getAsync(byte[] regionName, Get get) {
    switch (failureType) {
      // Region server will throw RegionOverloadedException, wrapped in ThriftHBaseException
      case REGIONOVERLOADEDEXCEPTION:
        if (++repeatCount > repeats) {
          clearExceptionMode();
          return super.getAsync(regionName, get);
        }
        LOG.debug("Exception repeat count: " + repeatCount);
        return Futures.immediateFailedFuture(new ThriftHBaseException(
            new RegionOverloadedException("GetAsync Test ROE", HConstants.DEFAULT_HBASE_CLIENT_PAUSE)));

      // Region server will disconnect the client, cause TTransportException on client side
      case STOP:
        if (++repeatCount > repeats) {
          clearExceptionMode();
          return super.getAsync(regionName, get);
        }
        LOG.debug("Exception repeat count: " + repeatCount);
        stop("GetAsync Test Stop");
        try {
          Thread.sleep(9999999);
        } catch (Exception e) {
          LOG.debug("Interrupted");
        }
        return null;

      case MIXEDRETRIABLEEXCEPTIONS:
        if (++repeatCount > repeats) {
          clearExceptionMode();
          return super.getAsync(regionName, get);
        }
        LOG.debug("Exception repeat count: " + repeatCount);
        return Futures.immediateFailedFuture(getRetriableExceptions(repeatCount));

      case DONOTRETRYEXCEPTION:
        if (++repeatCount > repeats) {
          clearExceptionMode();
          return super.getAsync(regionName, get);
        }
        LOG.debug("Exception repeat count: " + repeatCount);
        return Futures.immediateFailedFuture(new ThriftHBaseException(
            new DoNotRetryIOException("GetAsync Test DoNotRetryIOE")));

      default:
        return super.getAsync(regionName, get);
    }
  }

  /**
   * Provide exceptions that let client retry under default configuration
   * @param index
   * @return
   */
  private Exception getRetriableExceptions(int index) {
    if (retriableExceptions.isEmpty()) {
      retriableExceptions.add(new ThriftHBaseException(new IOException("Test IOE")));
      retriableExceptions.add(new NullPointerException("Test NPE"));
      retriableExceptions.add(new RuntimeException("Test RTE"));
    }

    return retriableExceptions.get(index % retriableExceptions.size());
  }
}
