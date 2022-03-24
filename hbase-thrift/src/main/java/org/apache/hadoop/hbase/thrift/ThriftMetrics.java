/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.exceptions.ScannerResetException;
import org.apache.hadoop.hbase.quotas.QuotaExceededException;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift2.generated.TIOError;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is for maintaining the various statistics of thrift server
 * and publishing them through the metrics interfaces.
 */
@InterfaceAudience.Private
public class ThriftMetrics  {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftMetrics.class);

  public enum ThriftServerType {
    ONE,
    TWO
  }

  public MetricsThriftServerSource getSource() {
    return source;
  }

  public void setSource(MetricsThriftServerSource source) {
    this.source = source;
  }

  protected MetricsThriftServerSource source;
  protected final long slowResponseTime;
  public static final String SLOW_RESPONSE_NANO_SEC =
    "hbase.thrift.slow.response.nano.second";
  public static final long DEFAULT_SLOW_RESPONSE_NANO_SEC = 10 * 1000 * 1000;
  private final ThriftServerType thriftServerType;

  public ThriftMetrics(Configuration conf, ThriftServerType t) {
    slowResponseTime = conf.getLong(SLOW_RESPONSE_NANO_SEC, DEFAULT_SLOW_RESPONSE_NANO_SEC);
    thriftServerType = t;
    if (t == ThriftServerType.ONE) {
      source = CompatibilitySingletonFactory.getInstance(MetricsThriftServerSourceFactory.class)
              .createThriftOneSource();
    } else if (t == ThriftServerType.TWO) {
      source = CompatibilitySingletonFactory.getInstance(MetricsThriftServerSourceFactory.class)
              .createThriftTwoSource();
    }

  }

  public void incTimeInQueue(long time) {
    source.incTimeInQueue(time);
  }

  public void setCallQueueLen(int len) {
    source.setCallQueueLen(len);
  }

  public void incNumRowKeysInBatchGet(int diff) {
    source.incNumRowKeysInBatchGet(diff);
  }

  public void incNumRowKeysInBatchMutate(int diff) {
    source.incNumRowKeysInBatchMutate(diff);
  }

  public void incMethodTime(String name, long time) {
    source.incMethodTime(name, time);
    // inc general processTime
    source.incCall(time);
    if (time > slowResponseTime) {
      source.incSlowCall(time);
    }
  }

  public void incActiveWorkerCount() {
    source.incActiveWorkerCount();
  }

  public void decActiveWorkerCount() {
    source.decActiveWorkerCount();
  }

  /**
   * Increment the count for a specific exception type.  This is called for each exception type
   * that is returned to the thrift handler.
   * @param rawThrowable type of exception
   */
  public void exception(Throwable rawThrowable) {
    source.exception();

    Throwable throwable = unwrap(rawThrowable);
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
      } else if (throwable instanceof ScannerResetException) {
        source.scannerResetException();
      } else if (throwable instanceof RegionMovedException) {
        source.movedRegionException();
      } else if (throwable instanceof NotServingRegionException) {
        source.notServingRegionException();
      } else if (throwable instanceof FailedSanityCheckException) {
        source.failedSanityException();
      } else if (throwable instanceof MultiActionResultTooLarge) {
        source.multiActionTooLargeException();
      } else if (throwable instanceof CallQueueTooBigException) {
        source.callQueueTooBigException();
      } else if (throwable instanceof QuotaExceededException) {
        source.quotaExceededException();
      } else if (throwable instanceof RpcThrottlingException) {
        source.rpcThrottlingException();
      } else if (throwable instanceof CallDroppedException) {
        source.callDroppedException();
      } else if (throwable instanceof RequestTooBigException) {
        source.requestTooBigException();
      } else {
        source.otherExceptions();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unknown exception type", throwable);
        }
      }
    }
  }

  protected static Throwable unwrap(Throwable t) {
    if (t == null) {
      return t;
    }
    if (t instanceof TIOError || t instanceof IOError) {
      t = t.getCause();
    }
    return ClientExceptionsUtil.findException(t);
  }

  public ThriftServerType getThriftServerType() {
    return thriftServerType;
  }
}
