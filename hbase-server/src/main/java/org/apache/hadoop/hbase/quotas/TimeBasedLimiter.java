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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;

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
    } else {
      reqsLimiter = new AverageIntervalRateLimiter();
      reqSizeLimiter = new AverageIntervalRateLimiter();
      writeReqsLimiter = new AverageIntervalRateLimiter();
      writeSizeLimiter = new AverageIntervalRateLimiter();
      readReqsLimiter = new AverageIntervalRateLimiter();
      readSizeLimiter = new AverageIntervalRateLimiter();
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
    if (!reqsLimiter.canExecute()) {
      ThrottlingException.throwNumRequestsExceeded(reqsLimiter.waitInterval());
    }
    if (!reqSizeLimiter.canExecute(writeSize + readSize)) {
      ThrottlingException.throwRequestSizeExceeded(reqSizeLimiter
          .waitInterval(writeSize + readSize));
    }

    if (writeSize > 0) {
      if (!writeReqsLimiter.canExecute()) {
        ThrottlingException.throwNumWriteRequestsExceeded(writeReqsLimiter.waitInterval());
      }
      if (!writeSizeLimiter.canExecute(writeSize)) {
        ThrottlingException.throwWriteSizeExceeded(writeSizeLimiter.waitInterval(writeSize));
      }
    }

    if (readSize > 0) {
      if (!readReqsLimiter.canExecute()) {
        ThrottlingException.throwNumReadRequestsExceeded(readReqsLimiter.waitInterval());
      }
      if (!readSizeLimiter.canExecute(readSize)) {
        ThrottlingException.throwReadSizeExceeded(readSizeLimiter.waitInterval(readSize));
      }
    }
  }

  @Override
  public void grabQuota(long writeSize, long readSize) {
    assert writeSize != 0 || readSize != 0;

    reqsLimiter.consume(1);
    reqSizeLimiter.consume(writeSize + readSize);

    if (writeSize > 0) {
      writeReqsLimiter.consume(1);
      writeSizeLimiter.consume(writeSize);
    }
    if (readSize > 0) {
      readReqsLimiter.consume(1);
      readSizeLimiter.consume(readSize);
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
