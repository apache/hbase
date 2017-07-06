/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.commons.lang.time.FastDateFormat;

import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * {@link TimestampType} is an enum to represent different ways of encoding time in HBase using
 * 64 bits. Time is usually encoded as a 64-bit long in {@link org.apache.hadoop.hbase.Cell}
 * timestamps and is used for sorting {@link org.apache.hadoop.hbase.Cell}s, ordering writes etc.
 * It has methods which help in constructing or interpreting the 64 bit timestamp and getter
 * methods to read the hard coded constants of the particular {@link TimestampType}.
 *
 * <p>
 * Enum {@link TimestampType} is dumb in a way. It doesn't have any logic other than interpreting
 * the 64 bits. Any monotonically increasing or monotonically non-decreasing semantics of the
 * timestamps are the responsibility of the clock implementation generating the particular
 * timestamps. There can be several clock implementations, and each such implementation can map
 * its representation of the timestamp to one of the available Timestamp types i.e.
 * {@link #HYBRID} or {@link #PHYSICAL}. In essence, the {@link TimestampType} is only used
 * internally by the Clock implementations and thus never exposed to the user. The user has to
 * know only the different available clock types. So, for the user, timestamp types do not exist.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public enum TimestampType {
  /**
   * Hybrid is a Timestamp type used to encode both physical time and logical time components
   * into a single. 64 bits long integer. It has methods to decipher the 64 bits hybrid timestamp
   * and also to construct the hybrid timestamp.
   */
  HYBRID {
    /**
     * Hard coded 44-bits for physical time, with most significant bit carrying the sign i.e 0
     * as we are dealing with positive integers and the remaining 43 bits are to be interpreted as
     * system time in milli seconds. See
     * <a href="https://issues.apache.org/jira/browse/HBASE-14070">HBASE-14070 </a> for
     * understanding the choice of going with the millisecond resolution for physical time.
     * Thus allowing us to represent all the dates between unix epoch (1970) and year 2248 with
     * signed timestamp comparison with 44 bits for physical time assuming a millisecond
     * resolution with signed long integers. Picking 42 bits to represent the physical time has
     * the problem of representing time until 2039 only, with signed integers, might cause Y2k39
     * bug hoping HBase to be around till then. The trade-off here is with the year until we can
     * represent the physical time vs if we are able capture all the events in the worst case
     * (read: leap seconds etc) without the logical component of the timestamp overflowing. With
     * 20 bits for logical time, one can represent upto one million events at the same
     * millisecond. In case of leap seconds, the no of events happening in the same second is very
     * unlikely to exceed one million.
     */
    @SuppressWarnings("unused")
    private static final int BITS_FOR_PHYSICAL_TIME = 44;

    /**
     * Remaining 20-bits for logical time, allowing values up to 1,048,576. Logical Time is the
     * least significant part of the 64 bit timestamp, so unsigned comparison can be used for LT.
     */

    private static final int BITS_FOR_LOGICAL_TIME = 20;

    /**
     * Max value for physical time in the {@link #HYBRID} timestamp representation, inclusive.
     * This assumes signed comparison.
     */
    private static final long PHYSICAL_TIME_MAX_VALUE = 0x7ffffffffffL;

    /**
     * Max value for logical time in the {@link #HYBRID} timestamp representation
     */
    static final long LOGICAL_TIME_MAX_VALUE = 0xfffffL;

    public long toEpochTimeMillisFromTimestamp(long timestamp) {
      return getPhysicalTime(timestamp);
    }

    public long fromEpochTimeMillisToTimestamp(long timestamp) {
      return toTimestamp(TimeUnit.MILLISECONDS, timestamp, 0);
    }

    public long toTimestamp(TimeUnit timeUnit, long physicalTime, long logicalTime) {
      physicalTime = TimeUnit.MILLISECONDS.convert(physicalTime, timeUnit);
      return (physicalTime << BITS_FOR_LOGICAL_TIME) + logicalTime;
    }

    public long getPhysicalTime(long timestamp) {
      return timestamp >>> BITS_FOR_LOGICAL_TIME; // assume unsigned timestamp
    }

    long getLogicalTime(long timestamp) {
      return timestamp & LOGICAL_TIME_MAX_VALUE;
    }

    public long getMaxPhysicalTime() {
      return PHYSICAL_TIME_MAX_VALUE;
    }

    public long getMaxLogicalTime() {
      return LOGICAL_TIME_MAX_VALUE;
    }

    int getBitsForLogicalTime() {
      return BITS_FOR_LOGICAL_TIME;
    }

    /**
     * Returns whether the given timestamp is "likely" of {@link #HYBRID} {@link TimestampType}.
     * Timestamp implementations can use the full range of 64bits long to represent physical and
     * logical components of time. However, this method returns whether the given timestamp is a
     * likely representation depending on heuristics for the clock implementation.
     *
     * Hybrid timestamps are checked whether they belong to Hybrid range assuming
     * that Hybrid timestamps will only have > 0 logical time component for timestamps
     * corresponding to years after 2016. This method will return false if lt > 0 and year is
     * before 2016. Due to left shifting for Hybrid time, all millisecond-since-epoch timestamps
     * from years 1970-10K fall into
     * year 1970 when interpreted as Hybrid timestamps. Thus, {@link #isLikelyOfType(long)} will
     * return false for timestamps which are in the year 1970 and logical time = 0 when
     * interpreted as of type Hybrid Time.
     *
     * <p>
     *   <b>Note that </b> this method uses heuristics which may not hold
     * if system timestamps are intermixed from client side and server side or timestamp
     * sources other than system clock are used.
     * </p>
     * @param timestamp {@link #HYBRID} Timestamp
     * @return true if the timestamp is likely to be of the corresponding {@link TimestampType}
     * else false
     */
    public boolean isLikelyOfType(long timestamp) {
      final long physicalTime = getPhysicalTime(timestamp);
      final long logicalTime = getLogicalTime(timestamp);

      // heuristic 1: Up until year 2016 (1451635200000), lt component cannot be non-zero.
      if (physicalTime < 1451635200000L && logicalTime != 0) {
        return false;
      } else if (physicalTime < 31536000000L) {
        // heuristic 2: Even if logical time = 0, physical time after left shifting by 20 bits,
        // will be before year 1971(31536000000L), as after left shifting by 20, all epoch ms
        // timestamps from wall time end up in year less than 1971, even for epoch time for the
        // year 10000. This assumes Hybrid time is not used to represent timestamps for year 1970
        // UTC.
        return false;
      }
      return true;
    }

    /**
     * Returns a string representation for Physical Time and Logical Time components. The format is:
     * <code>yyyy-MM-dd HH:mm:ss:SSS(Physical Time),Logical Time</code>
     * Physical Time is converted to UTC time and not to local time for uniformity.
     * Example: 2015-07-17 16:56:35:891(1437177395891), 0
     * @param timestamp A {@link #HYBRID} Timestamp
     * @return A date time string formatted as mentioned in the method description
     */
    public String toString(long timestamp) {
      long physicalTime = getPhysicalTime(timestamp);
      long logicalTime = getLogicalTime(timestamp);
      return new StringBuilder().append(dateFormat.format(physicalTime)).append("(")
          .append(physicalTime).append(")").append(", ").append(logicalTime).toString();
    }
  },

  /**
   * Physical is a Timestamp type used to encode the physical time in 64 bits.
   * It has helper methods to decipher the 64 bit encoding of physical time.
   */
  PHYSICAL {
    public long toEpochTimeMillisFromTimestamp(long timestamp) {
      return timestamp;
    }

    public long fromEpochTimeMillisToTimestamp(long timestamp) {
      return timestamp;
    }

    public long toTimestamp(TimeUnit timeUnit, long physicalTime, long logicalTime) {
      return TimeUnit.MILLISECONDS.convert(physicalTime, timeUnit);
    }

    public long getPhysicalTime(long timestamp) {
      return timestamp;
    }

    long getLogicalTime(long timestamp) {
      return 0;
    }

    public long getMaxPhysicalTime() {
      return Long.MAX_VALUE;
    }

    public long getMaxLogicalTime() {
      return 0;
    }

    int getBitsForLogicalTime() {
      return 0;
    }

    public boolean isLikelyOfType(long timestamp) {
      // heuristic: the timestamp should be up to year 3K (32503680000000L).
      return timestamp < 32503680000000L;
    }

    /**
     * Returns a string representation for Physical Time and Logical Time components. The format is:
     * <code>yyyy-MM-dd HH:mm:ss:SSS(Physical Time)</code>
     * Physical Time is converted to UTC time and not to local time for uniformity.
     * Example: 2015-07-17 16:56:35:891(1437177395891), 0
     * @param timestamp epoch time in milliseconds
     * @return A date time string formatted as mentioned in the method description
     */
    public String toString(long timestamp) {
      return new StringBuilder().append(dateFormat.format(timestamp)).append("(")
          .append(timestamp).append(")").append(", ").append("0").toString();
    }
  };

  /**
   * This is used internally by the enum methods of Hybrid and Physical Timestamp types to
   * convert the
   * timestamp to the format set here. UTC timezone instead of local time zone for convenience
   * and uniformity
   */
  private static final FastDateFormat dateFormat =
      FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss:SSS", TimeZone.getTimeZone("UTC"));

  /**
   * Converts the given timestamp to the unix epoch timestamp with millisecond resolution.
   * Returned timestamp is compatible with System.currentTimeMillis().
   * @param timestamp {@link #HYBRID} or {@link #PHYSICAL} Timestamp
   * @return number of milliseconds from epoch
   */
  abstract public long toEpochTimeMillisFromTimestamp(long timestamp);

  /**
   * Converts the given time in milliseconds to the corresponding {@link TimestampType}
   * representation.
   * @param timeInMillis epoch time in {@link TimeUnit#MILLISECONDS}
   * @return a timestamp representation corresponding to {@link TimestampType}.
   */
  abstract public long fromEpochTimeMillisToTimestamp(long timeInMillis);

  /**
   * Converts the given physical clock in the given {@link TimeUnit} to a 64-bit timestamp
   * @param timeUnit a time unit as in the enum {@link TimeUnit}
   * @param physicalTime physical time
   * @param logicalTime logical time
   * @return a timestamp in 64 bits
   */
  abstract public long toTimestamp(TimeUnit timeUnit, long physicalTime, long logicalTime);

  /**
   * Extracts and returns the physical time from the timestamp
   * @param timestamp {@link #HYBRID} or {@link #PHYSICAL} Timestamp
   * @return physical time in {@link TimeUnit#MILLISECONDS}
   */
  abstract public long getPhysicalTime(long timestamp);

  /**
   * Extracts and returns the logical time from the timestamp
   * @param timestamp {@link #HYBRID} or {@link #PHYSICAL} Timestamp
   * @return logical time
   */
  abstract long getLogicalTime(long timestamp);

  /**
   * @return the maximum possible physical time in {@link TimeUnit#MILLISECONDS}
   */
  abstract public long getMaxPhysicalTime();

  /**
   * @return the maximum possible logical time
   */
  abstract public long getMaxLogicalTime();

  /**
   * @return number of least significant bits allocated for logical time
   */
  abstract int getBitsForLogicalTime();

  /**
   * @param timestamp epoch time in milliseconds
   * @return True if the timestamp generated by the clock is of type {@link #PHYSICAL} else False
   */
  abstract public boolean isLikelyOfType(long timestamp);

  public abstract String toString(long timestamp);

}
