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
package org.apache.hadoop.hbase.security.oauthbearer;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public final class Utils {
  /**
   *  Converts a {@code Map} class into a string, concatenating keys and values
   *  Example:
   *      {@code mkString({ key: "hello", keyTwo: "hi" }, "|START|", "|END|", "=", ",")
   *          => "|START|key=hello,keyTwo=hi|END|"}
   */
  public static <K, V> String mkString(Map<K, V> map, String begin, String end,
    String keyValueSeparator, String elementSeparator) {
    StringBuilder bld = new StringBuilder();
    bld.append(begin);
    String prefix = "";
    for (Map.Entry<K, V> entry : map.entrySet()) {
      bld.append(prefix).append(entry.getKey()).
        append(keyValueSeparator).append(entry.getValue());
      prefix = elementSeparator;
    }
    bld.append(end);
    return bld.toString();
  }

  /**
   *  Converts an extensions string into a {@code Map<String, String>}.
   *
   *  Example:
   *      {@code parseMap("key=hey,keyTwo=hi,keyThree=hello", "=", ",") =>
   *      { key: "hey", keyTwo: "hi", keyThree: "hello" }}
   *
   */
  public static Map<String, String> parseMap(String mapStr,
    String keyValueSeparator, String elementSeparator) {
    Map<String, String> map = new HashMap<>();

    if (!mapStr.isEmpty()) {
      String[] attrvals = mapStr.split(elementSeparator);
      for (String attrval : attrvals) {
        String[] array = attrval.split(keyValueSeparator, 2);
        map.put(array[0], array[1]);
      }
    }
    return map;
  }

  /**
   * Given two maps (A, B), returns all the key-value pairs in A whose keys are not contained in B
   */
  public static <K, V> Map<K, V> subtractMap(Map<? extends K, ? extends V> minuend,
    Map<? extends K, ? extends V> subtrahend) {
    return minuend.entrySet().stream()
      .filter(entry -> !subtrahend.containsKey(entry.getKey()))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Checks if a string is null, empty or whitespace only.
   * @param str a string to be checked
   * @return true if the string is null, empty or whitespace only; otherwise, return false.
   */
  public static boolean isBlank(String str) {
    return str == null || str.trim().isEmpty();
  }

  private Utils() {
    // empty
  }
}
