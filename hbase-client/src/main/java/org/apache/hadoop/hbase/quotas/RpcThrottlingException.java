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
package org.apache.hadoop.hbase.quotas;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Describe the throttling result. TODO: At some point this will be handled on the client side to
 * prevent operation to go on the server if the waitInterval is greater than the one got as result
 * of this exception.
 */
@InterfaceAudience.Public
public class RpcThrottlingException extends HBaseIOException {

  @InterfaceAudience.Public
  public enum Type {
    NumRequestsExceeded,
    RequestSizeExceeded,
    NumReadRequestsExceeded,
    NumWriteRequestsExceeded,
    WriteSizeExceeded,
    ReadSizeExceeded,
    RequestCapacityUnitExceeded,
    ReadCapacityUnitExceeded,
    WriteCapacityUnitExceeded,
    AtomicRequestNumberExceeded,
    AtomicReadSizeExceeded,
    AtomicWriteSizeExceeded,
    RequestHandlerUsageTimeExceeded,
  }

  private static final String[] MSG_TYPE = new String[] { "number of requests exceeded",
    "request size limit exceeded", "number of read requests exceeded",
    "number of write requests exceeded", "write size limit exceeded", "read size limit exceeded",
    "request capacity unit exceeded", "read capacity unit exceeded", "write capacity unit exceeded",
    "atomic request number exceeded", "atomic read size exceeded", "atomic write size exceeded",
    "request handler usage time exceeded" };

  private static final String MSG_WAIT = " - wait ";

  private long waitInterval;
  private Type type;

  public RpcThrottlingException(String msg) {
    super(msg);

    // Dirty workaround to get the information after
    // ((RemoteException)e.getCause()).unwrapRemoteException()
    for (int i = 0; i < MSG_TYPE.length; ++i) {
      int index = msg.indexOf(MSG_TYPE[i]);
      if (index >= 0) {
        String waitTimeStr = msg.substring(index + MSG_TYPE[i].length() + MSG_WAIT.length());
        type = Type.values()[i];
        waitInterval = timeFromString(waitTimeStr);
        break;
      }
    }
  }

  public RpcThrottlingException(final Type type, final long waitInterval, final String msg) {
    super(msg);
    this.waitInterval = waitInterval;
    this.type = type;
  }

  public Type getType() {
    return this.type;
  }

  public long getWaitInterval() {
    return this.waitInterval;
  }

  public static void throwNumRequestsExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.NumRequestsExceeded, waitInterval);
  }

  public static void throwRequestSizeExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.RequestSizeExceeded, waitInterval);
  }

  public static void throwNumReadRequestsExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.NumReadRequestsExceeded, waitInterval);
  }

  public static void throwNumWriteRequestsExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.NumWriteRequestsExceeded, waitInterval);
  }

  public static void throwWriteSizeExceeded(final long waitInterval) throws RpcThrottlingException {
    throwThrottlingException(Type.WriteSizeExceeded, waitInterval);
  }

  public static void throwReadSizeExceeded(final long waitInterval) throws RpcThrottlingException {
    throwThrottlingException(Type.ReadSizeExceeded, waitInterval);
  }

  public static void throwRequestCapacityUnitExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.RequestCapacityUnitExceeded, waitInterval);
  }

  public static void throwReadCapacityUnitExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.ReadCapacityUnitExceeded, waitInterval);
  }

  public static void throwWriteCapacityUnitExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.WriteCapacityUnitExceeded, waitInterval);
  }

  public static void throwAtomicRequestNumberExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.AtomicRequestNumberExceeded, waitInterval);
  }

  public static void throwAtomicReadSizeExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.AtomicReadSizeExceeded, waitInterval);
  }

  public static void throwAtomicWriteSizeExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.AtomicWriteSizeExceeded, waitInterval);
  }

  public static void throwRequestHandlerUsageTimeExceeded(final long waitInterval)
    throws RpcThrottlingException {
    throwThrottlingException(Type.RequestHandlerUsageTimeExceeded, waitInterval);
  }

  private static void throwThrottlingException(final Type type, final long waitInterval)
    throws RpcThrottlingException {
    String msg = MSG_TYPE[type.ordinal()] + MSG_WAIT + stringFromMillis(waitInterval);
    throw new RpcThrottlingException(type, waitInterval, msg);
  }

  // Visible for TestRpcThrottlingException
  protected static String stringFromMillis(long millis) {
    StringBuilder buf = new StringBuilder();
    long hours = millis / (60 * 60 * 1000);
    long rem = (millis % (60 * 60 * 1000));
    long minutes = rem / (60 * 1000);
    rem = rem % (60 * 1000);
    long seconds = rem / 1000;
    long milliseconds = rem % 1000;

    if (hours != 0) {
      buf.append(hours);
      buf.append(hours > 1 ? "hrs, " : "hr, ");
    }
    if (minutes != 0) {
      buf.append(minutes);
      buf.append(minutes > 1 ? "mins, " : "min, ");
    }
    if (seconds != 0) {
      buf.append(seconds);
      buf.append("sec, ");
    }
    buf.append(milliseconds);
    buf.append("ms");
    return buf.toString();
  }

  // Visible for TestRpcThrottlingException
  protected static long timeFromString(String timeDiff) {
    Pattern pattern =
      Pattern.compile("^(?:(\\d+)hrs?, )?(?:(\\d+)mins?, )?(?:(\\d+)sec[, ]{0,2})?(?:(\\d+)ms)?");
    long[] factors = new long[] { 60 * 60 * 1000, 60 * 1000, 1000, 1 };
    Matcher m = pattern.matcher(timeDiff);
    if (m.find()) {
      int numGroups = m.groupCount();
      long time = 0;
      for (int j = 1; j <= numGroups; j++) {
        String group = m.group(j);
        if (group == null) {
          continue;
        }
        time += Math.round(Float.parseFloat(group) * factors[j - 1]);
      }
      return time;
    }
    return -1;
  }

  /**
   * There is little value in an RpcThrottlingException having a stack trace, since its cause is
   * well understood without one. When a RegionServer is under heavy load and needs to serve many
   * RpcThrottlingExceptions, skipping fillInStackTrace() will save CPU time and allocations, both
   * here and later when the exception must be serialized over the wire.
   */
  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }
}
