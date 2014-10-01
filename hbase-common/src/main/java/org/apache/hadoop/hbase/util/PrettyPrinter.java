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

package org.apache.hadoop.hbase.util;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HConstants;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class PrettyPrinter {

  public enum Unit {
    TIME_INTERVAL,
    NONE
  }

  public static String format(final String value, final Unit unit) {
    StringBuilder human = new StringBuilder();
    switch (unit) {
      case TIME_INTERVAL:
        human.append(humanReadableTTL(Long.valueOf(value)));
        break;
      default:
        human.append(value);
    }
    return human.toString();
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="ICAST_INTEGER_MULTIPLY_CAST_TO_LONG",
      justification="Will not overflow")
  private static String humanReadableTTL(final long interval){
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

    days  =   (int) (interval / HConstants.DAY_IN_SECONDS);
    hours =   (int) (interval - HConstants.DAY_IN_SECONDS * days) / HConstants.HOUR_IN_SECONDS;
    minutes = (int) (interval - HConstants.DAY_IN_SECONDS * days
        - HConstants.HOUR_IN_SECONDS * hours) / HConstants.MINUTE_IN_SECONDS;
    seconds = (int) (interval - HConstants.DAY_IN_SECONDS * days
        - HConstants.HOUR_IN_SECONDS * hours - HConstants.MINUTE_IN_SECONDS * minutes);

    sb.append(interval);
    sb.append(" SECONDS (");

    if (days > 0) {
      sb.append(days);
      sb.append(" DAY").append(days == 1 ? "" : "S");
    }

    if (hours > 0 ) {
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

}
