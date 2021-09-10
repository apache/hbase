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
package org.apache.hadoop.hbase.rsgroup;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Optional;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Helper class for RSGroup configuration reading and writing
 */
@InterfaceAudience.Public
public final class RSGroupConfigurationAccessor {

  public static final String REQUIRED_SERVER_NUM = "hbase.rsgroup.required.server.num";

  private RSGroupConfigurationAccessor() {
  }

  public static void set(Map<String, String> configuration, String key, String val) {
    configuration.put(requireNonNull(key), requireNonNull(val));
  }

  public static void setInt(Map<String, String> configuration, String key, Integer val) {
    set(configuration, key, requireNonNull(val).toString());
  }

  public static Optional<String> get(Map<String, String> configuration, String key) {
    return Optional.ofNullable(configuration.get(key));
  }

  public static Optional<String> get(RSGroupInfo groupInfo, String key) {
    return get(groupInfo.getConfiguration(), key);
  }

  public static Optional<Integer> getInt(RSGroupInfo groupInfo, String key) {
    return get(groupInfo, key).map(Integer::valueOf);
  }

  public static Optional<Integer> getRequiredServerNum(RSGroupInfo groupInfo) {
    return getInt(groupInfo, REQUIRED_SERVER_NUM);
  }

  public static void setRequiredServerNum(Map<String, String> configuration, Integer num) {
    setInt(configuration, REQUIRED_SERVER_NUM, num);
  }
}
