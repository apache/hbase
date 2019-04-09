/**
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

package org.apache.hadoop.hbase.quotas;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.TimedQuota;

/**
 * Simple time based limiter that checks the quota Throttle
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TimeBasedLimiter implements QuotaLimiter {
  private static final Configuration conf = HBaseConfiguration.create();
  private RateLimiter reqsLimiter = null;
  private RateLimiter reqSizeLimiter = null;
  private RateLimiter writeReqsLimiter = null;
  private RateLimiter writeSizeLimiter = null;
  private RateLimiter readReqsLimiter = null;
  private RateLimiter readSizeLimiter = null;
  private RateLimiter reqCapacityUnitLimiter = null;
  private RateLimiter writeCapacityUnitLimiter = null;
  private RateLimiter readCapacityUnitLimiter = null;

  private TimeBasedLimiter() {
    if (FixedIntervalRateLimiter.class.getName().equals(
      conf.getClass(RateLimiter.QUOTA_RATE_LIMITER_CONF_KEY, AverageIntervalRateLimiter.class)
          .getName())) {
      reqsLimiter = new FixedIntervalRateLimiter();
      reqSizeLimiter = new FixedIntervalRateLimiter();
      writeReqsLimiter = new FixedIntervalRateLimiter();
      writeSizeLimiter = new FixedIntervalRateLimiter();
      readReqsLimiter = new FixedIntervalRateLimiter();
      readSizeLimiter = new FixedIntervalRateLimiter();
      reqCapacityUnitLimiter = new FixedIntervalRateLimiter();
      writeCapacityUnitLimiter = new FixedIntervalRateLimiter();
      readCapacityUnitLimiter = new FixedIntervalRateLimiter();
    } else {
      reqsLimiter = new AverageIntervalRateLimiter();
      reqSizeLimiter = new AverageIntervalRateLimiter();
      writeReqsLimiter = new AverageIntervalRateLimiter();
      writeSizeLimiter = new AverageIntervalRateLimiter();
      readReqsLimiter = new AverageIntervalRateLimiter();
      readSizeLimiter = new AverageIntervalRateLimiter();
      reqCapacityUnitLimiter = new AverageIntervalRateLimiter();
      writeCapacityUnitLimiter = new AverageIntervalRateLimiter();
      readCapacityUnitLimiter = new AverageIntervalRateLimiter();
    }
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

    if (throttle.hasReqCapacityUnit()) {
      setFromTimedQuota(limiter.reqCapacityUnitLimiter, throttle.getReqCapacityUnit());
      isBypass = false;
    }

    if (throttle.hasWriteCapacityUnit()) {
      setFromTimedQuota(limiter.writeCapacityUnitLimiter, throttle.getWriteCapacityUnit());
      isBypass = false;
    }

    if (throttle.hasReadCapacityUnit()) {
      setFromTimedQuota(limiter.readCapacityUnitLimiter, throttle.getReadCapacityUnit());
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
    reqCapacityUnitLimiter.update(other.reqCapacityUnitLimiter);
    writeCapacityUnitLimiter.update(other.writeCapacityUnitLimiter);
    readCapacityUnitLimiter.update(other.readCapacityUnitLimiter);
  }

  private static void setFromTimedQuota(final RateLimiter limiter, final TimedQuota timedQuota) {
    limiter.set(timedQuota.getSoftLimit(), ProtobufUtil.toTimeUnit(timedQuota.getTimeUnit()));
  }

  @Override
  public void checkQuota(long writeReqs, long estimateWriteSize, long readReqs,
      long estimateReadSize, long estimateWriteCapacityUnit, long estimateReadCapacityUnit)
      throws RpcThrottlingException {
    if (!reqsLimiter.canExecute(writeReqs + readReqs)) {
      RpcThrottlingException.throwNumRequestsExceeded(reqsLimiter.waitInterval());
    }
    if (!reqSizeLimiter.canExecute(estimateWriteSize + estimateReadSize)) {
      RpcThrottlingException.throwRequestSizeExceeded(
          reqSizeLimiter.waitInterval(estimateWriteSize + estimateReadSize));
    }
    if (!reqCapacityUnitLimiter.canExecute(estimateWriteCapacityUnit + estimateReadCapacityUnit)) {
      RpcThrottlingException.throwRequestCapacityUnitExceeded(
        reqCapacityUnitLimiter.waitInterval(estimateWriteCapacityUnit + estimateReadCapacityUnit));
    }

    if (estimateWriteSize > 0) {
      if (!writeReqsLimiter.canExecute(writeReqs)) {
        RpcThrottlingException.throwNumWriteRequestsExceeded(writeReqsLimiter.waitInterval());
      }
      if (!writeSizeLimiter.canExecute(estimateWriteSize)) {
        RpcThrottlingException.throwWriteSizeExceeded(
            writeSizeLimiter.waitInterval(estimateWriteSize));
      }
      if (!writeCapacityUnitLimiter.canExecute(estimateWriteCapacityUnit)) {
        RpcThrottlingException.throwWriteCapacityUnitExceeded(
          writeCapacityUnitLimiter.waitInterval(estimateWriteCapacityUnit));
      }
    }

    if (estimateReadSize > 0) {
      if (!readReqsLimiter.canExecute(readReqs)) {
        RpcThrottlingException.throwNumReadRequestsExceeded(readReqsLimiter.waitInterval());
      }
      if (!readSizeLimiter.canExecute(estimateReadSize)) {
        RpcThrottlingException.throwReadSizeExceeded(
            readSizeLimiter.waitInterval(estimateReadSize));
      }
      if (!readCapacityUnitLimiter.canExecute(estimateReadCapacityUnit)) {
        RpcThrottlingException.throwReadCapacityUnitExceeded(
          readCapacityUnitLimiter.waitInterval(estimateReadCapacityUnit));
      }
    }
  }

  @Override
  public void grabQuota(long writeReqs, long writeSize, long readReqs, long readSize,
      long writeCapacityUnit, long readCapacityUnit) {
    assert writeSize != 0 || readSize != 0;

    reqsLimiter.consume(writeReqs + readReqs);
    reqSizeLimiter.consume(writeSize + readSize);

    if (writeSize > 0) {
      writeReqsLimiter.consume(writeReqs);
      writeSizeLimiter.consume(writeSize);
    }
    if (readSize > 0) {
      readReqsLimiter.consume(readReqs);
      readSizeLimiter.consume(readSize);
    }
    if (writeCapacityUnit > 0) {
      reqCapacityUnitLimiter.consume(writeCapacityUnit);
      writeCapacityUnitLimiter.consume(writeCapacityUnit);
    }
    if (readCapacityUnit > 0) {
      reqCapacityUnitLimiter.consume(readCapacityUnit);
      readCapacityUnitLimiter.consume(readCapacityUnit);
    }
  }

  @Override
  public void consumeWrite(final long size, long capacityUnit) {
    reqSizeLimiter.consume(size);
    writeSizeLimiter.consume(size);
    reqCapacityUnitLimiter.consume(capacityUnit);
    writeCapacityUnitLimiter.consume(capacityUnit);
  }

  @Override
  public void consumeRead(final long size, long capacityUnit) {
    reqSizeLimiter.consume(size);
    readSizeLimiter.consume(size);
    reqCapacityUnitLimiter.consume(capacityUnit);
    readCapacityUnitLimiter.consume(capacityUnit);
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
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("TimeBasedLimiter(");
    if (!reqsLimiter.isBypass()) {
      builder.append("reqs=" + reqsLimiter);
    }
    if (!reqSizeLimiter.isBypass()) {
      builder.append(" resSize=" + reqSizeLimiter);
    }
    if (!writeReqsLimiter.isBypass()) {
      builder.append(" writeReqs=" + writeReqsLimiter);
    }
    if (!writeSizeLimiter.isBypass()) {
      builder.append(" writeSize=" + writeSizeLimiter);
    }
    if (!readReqsLimiter.isBypass()) {
      builder.append(" readReqs=" + readReqsLimiter);
    }
    if (!readSizeLimiter.isBypass()) {
      builder.append(" readSize=" + readSizeLimiter);
    }
    if (!reqCapacityUnitLimiter.isBypass()) {
      builder.append(" reqCapacityUnit=" + reqCapacityUnitLimiter);
    }
    if (!writeCapacityUnitLimiter.isBypass()) {
      builder.append(" writeCapacityUnit=" + writeCapacityUnitLimiter);
    }
    if (!readCapacityUnitLimiter.isBypass()) {
      builder.append(" readCapacityUnit=" + readCapacityUnitLimiter);
    }
    builder.append(')');
    return builder.toString();
  }
}
