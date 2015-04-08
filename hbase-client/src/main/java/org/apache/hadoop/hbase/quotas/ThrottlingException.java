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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Describe the throttling result. TODO: At some point this will be handled on the client side to
 * prevent operation to go on the server if the waitInterval is grater than the one got as result of
 * this exception.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ThrottlingException extends QuotaExceededException {
  private static final long serialVersionUID = 1406576492085155743L;

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum Type {
    NumRequestsExceeded, NumReadRequestsExceeded, NumWriteRequestsExceeded, WriteSizeExceeded,
    ReadSizeExceeded,
  }

  private static final String[] MSG_TYPE = new String[] { "number of requests exceeded",
      "number of read requests exceeded", "number of write requests exceeded",
      "write size limit exceeded", "read size limit exceeded", };

  private static final String MSG_WAIT = " - wait ";

  private long waitInterval;
  private Type type;

  public ThrottlingException(String msg) {
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

  public ThrottlingException(final Type type, final long waitInterval, final String msg) {
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

  public static void throwNumRequestsExceeded(final long waitInterval) throws ThrottlingException {
    throwThrottlingException(Type.NumRequestsExceeded, waitInterval);
  }

  public static void throwNumReadRequestsExceeded(final long waitInterval)
      throws ThrottlingException {
    throwThrottlingException(Type.NumReadRequestsExceeded, waitInterval);
  }

  public static void throwNumWriteRequestsExceeded(final long waitInterval)
      throws ThrottlingException {
    throwThrottlingException(Type.NumWriteRequestsExceeded, waitInterval);
  }

  public static void throwWriteSizeExceeded(final long waitInterval) throws ThrottlingException {
    throwThrottlingException(Type.WriteSizeExceeded, waitInterval);
  }

  public static void throwReadSizeExceeded(final long waitInterval) throws ThrottlingException {
    throwThrottlingException(Type.ReadSizeExceeded, waitInterval);
  }

  private static void throwThrottlingException(final Type type, final long waitInterval)
      throws ThrottlingException {
    String msg = MSG_TYPE[type.ordinal()] + MSG_WAIT + formatTime(waitInterval);
    throw new ThrottlingException(type, waitInterval, msg);
  }

  public static String formatTime(long timeDiff) {
    StringBuilder buf = new StringBuilder();
    long hours = timeDiff / (60 * 60 * 1000);
    long rem = (timeDiff % (60 * 60 * 1000));
    long minutes = rem / (60 * 1000);
    rem = rem % (60 * 1000);
    float seconds = rem / 1000.0f;

    if (hours != 0) {
      buf.append(hours);
      buf.append("hrs, ");
    }
    if (minutes != 0) {
      buf.append(minutes);
      buf.append("mins, ");
    }
    buf.append(String.format("%.2fsec", seconds));
    return buf.toString();
  }

  private static long timeFromString(String timeDiff) {
    Pattern[] patterns =
        new Pattern[] { Pattern.compile("^(\\d+\\.\\d\\d)sec"),
            Pattern.compile("^(\\d+)mins, (\\d+\\.\\d\\d)sec"),
            Pattern.compile("^(\\d+)hrs, (\\d+)mins, (\\d+\\.\\d\\d)sec") };

    for (int i = 0; i < patterns.length; ++i) {
      Matcher m = patterns[i].matcher(timeDiff);
      if (m.find()) {
        long time = Math.round(Float.parseFloat(m.group(1 + i)) * 1000);
        if (i > 0) {
          time += Long.parseLong(m.group(i)) * (60 * 1000);
        }
        if (i > 1) {
          time += Long.parseLong(m.group(i - 1)) * (60 * 60 * 1000);
        }
        return time;
      }
    }

    return -1;
  }
}
