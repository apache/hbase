/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.quotas;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.quotas.OperationQuota.AvgOperationSize;
import org.apache.hadoop.hbase.quotas.OperationQuota.OperationType;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Simple time based limiter that checks the quota Throttle
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class TimeBasedLimiter implements QuotaLimiter {
  private long writeLastTs = 0;
  private long readLastTs = 0;

  private RateLimiter reqsLimiter = new RateLimiter();
  private RateLimiter reqSizeLimiter = new RateLimiter();
  private RateLimiter writeReqsLimiter = new RateLimiter();
  private RateLimiter writeSizeLimiter = new RateLimiter();
  private RateLimiter readReqsLimiter = new RateLimiter();
  private RateLimiter readSizeLimiter = new RateLimiter();
  private AvgOperationSize avgOpSize = new AvgOperationSize();

  private TimeBasedLimiter() {
  }

  static QuotaLimiter fromThrottle(final Throttle throttle) {
    TimeBasedLimiter limiter = new TimeBasedLimiter();
    boolean isBypass = true;
    if (throttle.hasReqNum()) {
      setFromTimedQuota(limiter.reqsLimiter, throttle.getReqNum());
      isBypass = false;
    }

    if (throttle.hasReqSize()) {
      setFromTimedQuota(limiter.reqSizeLimiter, throttle.getReqSize());
      isBypass = false;
    }

    if (throttle.hasWriteNum()) {
      setFromTimedQuota(limiter.writeReqsLimiter, throttle.getWriteNum());
      isBypass = false;
    }

    if (throttle.hasWriteSize()) {
      setFromTimedQuota(limiter.writeSizeLimiter, throttle.getWriteSize());
      isBypass = false;
    }

    if (throttle.hasReadNum()) {
      setFromTimedQuota(limiter.readReqsLimiter, throttle.getReadNum());
      isBypass = false;
    }

    if (throttle.hasReadSize()) {
      setFromTimedQuota(limiter.readSizeLimiter, throttle.getReadSize());
      isBypass = false;
    }
    return isBypass ? NoopQuotaLimiter.get() : limiter;
  }

  public void update(final TimeBasedLimiter other) {
    reqsLimiter.update(other.reqsLimiter);
    reqSizeLimiter.update(other.reqSizeLimiter);
    writeReqsLimiter.update(other.writeReqsLimiter);
    writeSizeLimiter.update(other.writeSizeLimiter);
    readReqsLimiter.update(other.readReqsLimiter);
    readSizeLimiter.update(other.readSizeLimiter);
  }

  private static void setFromTimedQuota(final RateLimiter limiter, final TimedQuota timedQuota) {
    limiter.set(timedQuota.getSoftLimit(), ProtobufUtil.toTimeUnit(timedQuota.getTimeUnit()));
  }

  @Override
  public void checkQuota(long writeSize, long readSize) throws ThrottlingException {
    long now = EnvironmentEdgeManager.currentTime();
    long lastTs = Math.max(readLastTs, writeLastTs);

    if (!reqsLimiter.canExecute(now, lastTs)) {
      ThrottlingException.throwNumRequestsExceeded(reqsLimiter.waitInterval());
    }
    if (!reqSizeLimiter.canExecute(now, lastTs, writeSize + readSize)) {
      ThrottlingException.throwNumRequestsExceeded(reqSizeLimiter
          .waitInterval(writeSize + readSize));
    }

    if (writeSize > 0) {
      if (!writeReqsLimiter.canExecute(now, writeLastTs)) {
        ThrottlingException.throwNumWriteRequestsExceeded(writeReqsLimiter.waitInterval());
      }
      if (!writeSizeLimiter.canExecute(now, writeLastTs, writeSize)) {
        ThrottlingException.throwWriteSizeExceeded(writeSizeLimiter.waitInterval(writeSize));
      }
    }

    if (readSize > 0) {
      if (!readReqsLimiter.canExecute(now, readLastTs)) {
        ThrottlingException.throwNumReadRequestsExceeded(readReqsLimiter.waitInterval());
      }
      if (!readSizeLimiter.canExecute(now, readLastTs, readSize)) {
        ThrottlingException.throwReadSizeExceeded(readSizeLimiter.waitInterval(readSize));
      }
    }
  }

  @Override
  public void grabQuota(long writeSize, long readSize) {
    assert writeSize != 0 || readSize != 0;

    long now = EnvironmentEdgeManager.currentTime();

    reqsLimiter.consume(1);
    reqSizeLimiter.consume(writeSize + readSize);

    if (writeSize > 0) {
      writeReqsLimiter.consume(1);
      writeSizeLimiter.consume(writeSize);
      writeLastTs = now;
    }
    if (readSize > 0) {
      readReqsLimiter.consume(1);
      readSizeLimiter.consume(readSize);
      readLastTs = now;
    }
  }

  @Override
  public void consumeWrite(final long size) {
    reqSizeLimiter.consume(size);
    writeSizeLimiter.consume(size);
  }

  @Override
  public void consumeRead(final long size) {
    reqSizeLimiter.consume(size);
    readSizeLimiter.consume(size);
  }

  @Override
  public boolean isBypass() {
    return false;
  }

  @Override
  public long getWriteAvailable() {
    return writeSizeLimiter.getAvailable();
  }

  @Override
  public long getReadAvailable() {
    return readSizeLimiter.getAvailable();
  }

  @Override
  public void addOperationSize(OperationType type, long size) {
    avgOpSize.addOperationSize(type, size);
  }

  @Override
  public long getAvgOperationSize(OperationType type) {
    return avgOpSize.getAvgOperationSize(type);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("TimeBasedLimiter(");
    if (!reqsLimiter.isBypass()) builder.append("reqs=" + reqsLimiter);
    if (!reqSizeLimiter.isBypass()) builder.append(" resSize=" + reqSizeLimiter);
    if (!writeReqsLimiter.isBypass()) builder.append(" writeReqs=" + writeReqsLimiter);
    if (!writeSizeLimiter.isBypass()) builder.append(" writeSize=" + writeSizeLimiter);
    if (!readReqsLimiter.isBypass()) builder.append(" readReqs=" + readReqsLimiter);
    if (!readSizeLimiter.isBypass()) builder.append(" readSize=" + readSizeLimiter);
    builder.append(')');
    return builder.toString();
  }
}
