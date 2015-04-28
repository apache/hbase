/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.util;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Utilities for storing more complex collection types in
 * {@link org.apache.hadoop.conf.Configuration} instances.
 */
public final class ConfigurationUtil {
  // TODO: hopefully this is a good delimiter; it's not in the base64 alphabet, 
  // nor is it valid for paths
  public static final char KVP_DELIMITER = '^';

  // Disallow instantiation
  private ConfigurationUtil() {

  }

  /**
   * Store a collection of Map.Entry's in conf, with each entry separated by ','
   * and key values delimited by {@link #KVP_DELIMITER}
   *
   * @param conf      configuration to store the collection in
   * @param key       overall key to store keyValues under
   * @param keyValues kvps to be stored under key in conf
   */
  public static void setKeyValues(Configuration conf, String key,
      Collection<Map.Entry<String, String>> keyValues) {
    setKeyValues(conf, key, keyValues, KVP_DELIMITER);
  }

  /**
   * Store a collection of Map.Entry's in conf, with each entry separated by ','
   * and key values delimited by delimiter.
   *
   * @param conf      configuration to store the collection in
   * @param key       overall key to store keyValues under
   * @param keyValues kvps to be stored under key in conf
   * @param delimiter character used to separate each kvp
   */
  public static void setKeyValues(Configuration conf, String key,
      Collection<Map.Entry<String, String>> keyValues, char delimiter) {
    List<String> serializedKvps = Lists.newArrayList();

    for (Map.Entry<String, String> kvp : keyValues) {
      serializedKvps.add(kvp.getKey() + delimiter + kvp.getValue());
    }

    conf.setStrings(key, serializedKvps.toArray(new String[serializedKvps.size()]));
  }

  /**
   * Retrieve a list of key value pairs from configuration, stored under the provided key
   *
   * @param conf configuration to retrieve kvps from
   * @param key  key under which the key values are stored
   * @return the list of kvps stored under key in conf, or null if the key isn't present.
   * @see #setKeyValues(Configuration, String, Collection, char)
   */
  public static List<Map.Entry<String, String>> getKeyValues(Configuration conf, String key) {
    return getKeyValues(conf, key, KVP_DELIMITER);
  }

  /**
   * Retrieve a list of key value pairs from configuration, stored under the provided key
   *
   * @param conf      configuration to retrieve kvps from
   * @param key       key under which the key values are stored
   * @param delimiter character used to separate each kvp
   * @return the list of kvps stored under key in conf, or null if the key isn't present.
   * @see #setKeyValues(Configuration, String, Collection, char)
   */
  public static List<Map.Entry<String, String>> getKeyValues(Configuration conf, String key,
      char delimiter) {
    String[] kvps = conf.getStrings(key);

    if (kvps == null) {
      return null;
    }

    List<Map.Entry<String, String>> rtn = Lists.newArrayList();

    for (String kvp : kvps) {
      String[] splitKvp = StringUtils.split(kvp, delimiter);

      if (splitKvp.length != 2) {
        throw new IllegalArgumentException(
            "Expected key value pair for configuration key '" + key + "'" + " to be of form '<key>"
                + delimiter + "<value>; was " + kvp + " instead");
      }

      rtn.add(new AbstractMap.SimpleImmutableEntry<>(splitKvp[0], splitKvp[1]));
    }
    return rtn;
  }
}
