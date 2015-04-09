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

package org.apache.hadoop.hbase.procedure2.util;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class StringUtils {
  private StringUtils() {}

  public static String humanTimeDiff(long timeDiff) {
    StringBuilder buf = new StringBuilder();
    long hours = timeDiff / (60*60*1000);
    long rem = (timeDiff % (60*60*1000));
    long minutes =  rem / (60*1000);
    rem = rem % (60*1000);
    float seconds = rem / 1000.0f;

    if (hours != 0){
      buf.append(hours);
      buf.append("hrs, ");
    }
    if (minutes != 0){
      buf.append(minutes);
      buf.append("mins, ");
    }
    if (hours > 0 || minutes > 0) {
      buf.append(seconds);
      buf.append("sec");
    } else {
      buf.append(String.format("%.4fsec", seconds));
    }
    return buf.toString();
  }

  public static String humanSize(double size) {
    if (size >= (1L << 40)) return String.format("%.1fT", size / (1L << 40));
    if (size >= (1L << 30)) return String.format("%.1fG", size / (1L << 30));
    if (size >= (1L << 20)) return String.format("%.1fM", size / (1L << 20));
    if (size >= (1L << 10)) return String.format("%.1fK", size / (1L << 10));
    return String.format("%.0f", size);
  }

  public static boolean isEmpty(final String input) {
    return input == null || input.length() == 0;
  }

  public static String buildString(final String... parts) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < parts.length; ++i) {
      sb.append(parts[i]);
    }
    return sb.toString();
  }

  public static StringBuilder appendStrings(final StringBuilder sb, final String... parts) {
    for (int i = 0; i < parts.length; ++i) {
      sb.append(parts[i]);
    }
    return sb;
  }
}