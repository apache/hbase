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
package org.apache.hadoop.hbase.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.exceptions.HBaseException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public final class PrettyPrinter {

  private static final Logger LOG = LoggerFactory.getLogger(PrettyPrinter.class);

  private static final String INTERVAL_REGEX =
    "((\\d+)\\s*SECONDS?\\s*\\()?\\s*" + "((\\d+)\\s*DAYS?)?\\s*((\\d+)\\s*HOURS?)?\\s*"
      + "((\\d+)\\s*MINUTES?)?\\s*((\\d+)\\s*SECONDS?)?\\s*\\)?";
  private static final Pattern INTERVAL_PATTERN =
    Pattern.compile(INTERVAL_REGEX, Pattern.CASE_INSENSITIVE);
  private static final String SIZE_REGEX =
    "((\\d+)\\s*B?\\s*\\()?\\s*" + "((\\d+)\\s*TB?)?\\s*((\\d+)\\s*GB?)?\\s*"
      + "((\\d+)\\s*MB?)?\\s*((\\d+)\\s*KB?)?\\s*((\\d+)\\s*B?)?\\s*\\)?";
  private static final Pattern SIZE_PATTERN = Pattern.compile(SIZE_REGEX, Pattern.CASE_INSENSITIVE);

  public enum Unit {
    TIME_INTERVAL,
    LONG,
    BOOLEAN,
    BYTE,
    NONE
  }

  public static String format(final String value, final Unit unit) {
    StringBuilder human = new StringBuilder();
    switch (unit) {
      case TIME_INTERVAL:
        human.append(humanReadableTTL(Long.parseLong(value)));
        break;
      case LONG:
        byte[] longBytes = Bytes.toBytesBinary(value);
        human.append(String.valueOf(Bytes.toLong(longBytes)));
        break;
      case BOOLEAN:
        byte[] booleanBytes = Bytes.toBytesBinary(value);
        human.append(String.valueOf(Bytes.toBoolean(booleanBytes)));
        break;
      case BYTE:
        human.append(humanReadableByte(Long.parseLong(value)));
        break;
      default:
        human.append(value);
    }
    return human.toString();
  }

  /**
   * Convert a human readable string to its value.
   * @see org.apache.hadoop.hbase.util.PrettyPrinter#format(String, Unit)
   * @return the value corresponding to the human readable string
   */
  public static String valueOf(final String pretty, final Unit unit) throws HBaseException {
    StringBuilder value = new StringBuilder();
    switch (unit) {
      case TIME_INTERVAL:
        value.append(humanReadableIntervalToSec(pretty));
        break;
      case BYTE:
        value.append(humanReadableSizeToBytes(pretty));
        break;
      default:
        value.append(pretty);
    }
    return value.toString();
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "ICAST_INTEGER_MULTIPLY_CAST_TO_LONG",
      justification = "Will not overflow")
  private static String humanReadableTTL(final long interval) {
    StringBuilder sb = new StringBuilder();
    int days, hours, minutes, seconds;

    // edge cases first
    if (interval == Integer.MAX_VALUE) {
      sb.append("FOREVER");
      return sb.toString();
    }
    if (interval < HConstants.MINUTE_IN_SECONDS) {
      sb.append(interval);
      sb.append(" SECOND").append(interval == 1 ? "" : "S");
      return sb.toString();
    }

    days = (int) (interval / HConstants.DAY_IN_SECONDS);
    hours = (int) (interval - HConstants.DAY_IN_SECONDS * days) / HConstants.HOUR_IN_SECONDS;
    minutes =
      (int) (interval - HConstants.DAY_IN_SECONDS * days - HConstants.HOUR_IN_SECONDS * hours)
        / HConstants.MINUTE_IN_SECONDS;
    seconds = (int) (interval - HConstants.DAY_IN_SECONDS * days
      - HConstants.HOUR_IN_SECONDS * hours - HConstants.MINUTE_IN_SECONDS * minutes);

    sb.append(interval);
    sb.append(" SECONDS (");

    if (days > 0) {
      sb.append(days);
      sb.append(" DAY").append(days == 1 ? "" : "S");
    }

    if (hours > 0) {
      sb.append(days > 0 ? " " : "");
      sb.append(hours);
      sb.append(" HOUR").append(hours == 1 ? "" : "S");
    }

    if (minutes > 0) {
      sb.append(days + hours > 0 ? " " : "");
      sb.append(minutes);
      sb.append(" MINUTE").append(minutes == 1 ? "" : "S");
    }

    if (seconds > 0) {
      sb.append(days + hours + minutes > 0 ? " " : "");
      sb.append(seconds);
      sb.append(" SECOND").append(minutes == 1 ? "" : "S");
    }

    sb.append(")");

    return sb.toString();
  }

  /**
   * Convert a human readable time interval to seconds. Examples of the human readable time
   * intervals are: 50 DAYS 1 HOUR 30 MINUTES , 25000 SECONDS etc. The units of time specified can
   * be in uppercase as well as lowercase. Also, if a single number is specified without any time
   * unit, it is assumed to be in seconds.
   * @return value in seconds
   */
  private static long humanReadableIntervalToSec(final String humanReadableInterval)
    throws HBaseException {
    if (humanReadableInterval == null || humanReadableInterval.equalsIgnoreCase("FOREVER")) {
      return HConstants.FOREVER;
    }

    try {
      return Long.parseLong(humanReadableInterval);
    } catch (NumberFormatException ex) {
      LOG.debug("Given interval value is not a number, parsing for human readable format");
    }

    String days = null;
    String hours = null;
    String minutes = null;
    String seconds = null;
    String expectedTtl = null;
    long ttl;

    Matcher matcher = PrettyPrinter.INTERVAL_PATTERN.matcher(humanReadableInterval);
    if (matcher.matches()) {
      expectedTtl = matcher.group(2);
      days = matcher.group(4);
      hours = matcher.group(6);
      minutes = matcher.group(8);
      seconds = matcher.group(10);
    }
    ttl = 0;
    ttl += days != null ? Long.parseLong(days) * HConstants.DAY_IN_SECONDS : 0;
    ttl += hours != null ? Long.parseLong(hours) * HConstants.HOUR_IN_SECONDS : 0;
    ttl += minutes != null ? Long.parseLong(minutes) * HConstants.MINUTE_IN_SECONDS : 0;
    ttl += seconds != null ? Long.parseLong(seconds) : 0;

    if (expectedTtl != null && Long.parseLong(expectedTtl) != ttl) {
      throw new HBaseException(
        "Malformed TTL string: TTL values in seconds and human readable" + "format do not match");
    }
    return ttl;
  }

  /**
   * Convert a long size to a human readable string. Example: 10763632640 -> 10763632640 B (10GB
   * 25MB)
   * @param size the size in bytes
   * @return human readable string
   */
  private static String humanReadableByte(final long size) {
    StringBuilder sb = new StringBuilder();
    long tb, gb, mb, kb, b;

    if (size < HConstants.KB_IN_BYTES) {
      sb.append(size);
      sb.append(" B");
      return sb.toString();
    }

    tb = size / HConstants.TB_IN_BYTES;
    gb = (size - HConstants.TB_IN_BYTES * tb) / HConstants.GB_IN_BYTES;
    mb =
      (size - HConstants.TB_IN_BYTES * tb - HConstants.GB_IN_BYTES * gb) / HConstants.MB_IN_BYTES;
    kb = (size - HConstants.TB_IN_BYTES * tb - HConstants.GB_IN_BYTES * gb
      - HConstants.MB_IN_BYTES * mb) / HConstants.KB_IN_BYTES;
    b = (size - HConstants.TB_IN_BYTES * tb - HConstants.GB_IN_BYTES * gb
      - HConstants.MB_IN_BYTES * mb - HConstants.KB_IN_BYTES * kb);

    sb.append(size).append(" B (");
    if (tb > 0) {
      sb.append(tb);
      sb.append("TB");
    }

    if (gb > 0) {
      sb.append(tb > 0 ? " " : "");
      sb.append(gb);
      sb.append("GB");
    }

    if (mb > 0) {
      sb.append(tb + gb > 0 ? " " : "");
      sb.append(mb);
      sb.append("MB");
    }

    if (kb > 0) {
      sb.append(tb + gb + mb > 0 ? " " : "");
      sb.append(kb);
      sb.append("KB");
    }

    if (b > 0) {
      sb.append(tb + gb + mb + kb > 0 ? " " : "");
      sb.append(b);
      sb.append("B");
    }

    sb.append(")");
    return sb.toString();
  }

  /**
   * Convert a human readable size to bytes. Examples of the human readable size are: 50 GB 20 MB 1
   * KB , 25000 B etc. The units of size specified can be in uppercase as well as lowercase. Also,
   * if a single number is specified without any time unit, it is assumed to be in bytes.
   * @param humanReadableSize human readable size
   * @return value in bytes
   */
  private static long humanReadableSizeToBytes(final String humanReadableSize)
    throws HBaseException {
    if (humanReadableSize == null) {
      return -1;
    }

    try {
      return Long.parseLong(humanReadableSize);
    } catch (NumberFormatException ex) {
      LOG.debug("Given size value is not a number, parsing for human readable format");
    }

    String tb = null;
    String gb = null;
    String mb = null;
    String kb = null;
    String b = null;
    String expectedSize = null;
    long size = 0;

    Matcher matcher = PrettyPrinter.SIZE_PATTERN.matcher(humanReadableSize);
    if (matcher.matches()) {
      expectedSize = matcher.group(2);
      tb = matcher.group(4);
      gb = matcher.group(6);
      mb = matcher.group(8);
      kb = matcher.group(10);
      b = matcher.group(12);
    }
    size += tb != null ? Long.parseLong(tb) * HConstants.TB_IN_BYTES : 0;
    size += gb != null ? Long.parseLong(gb) * HConstants.GB_IN_BYTES : 0;
    size += mb != null ? Long.parseLong(mb) * HConstants.MB_IN_BYTES : 0;
    size += kb != null ? Long.parseLong(kb) * HConstants.KB_IN_BYTES : 0;
    size += b != null ? Long.parseLong(b) : 0;

    if (expectedSize != null && Long.parseLong(expectedSize) != size) {
      throw new HBaseException(
        "Malformed size string: values in byte and human readable" + "format do not match");
    }
    return size;
  }

  /**
   * Pretty prints a collection of any type to a string. Relies on toString() implementation of the
   * object type.
   * @param collection collection to pretty print.
   * @return Pretty printed string for the collection.
   */
  public static String toString(Collection<?> collection) {
    List<String> stringList = new ArrayList<>();
    for (Object o : collection) {
      stringList.add(Objects.toString(o));
    }
    return "[" + String.join(",", stringList) + "]";
  }

}
