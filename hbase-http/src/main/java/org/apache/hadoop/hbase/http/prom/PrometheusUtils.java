/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.http.prom;

import org.apache.commons.lang3.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import java.util.regex.Pattern;

@InterfaceAudience.Private
class PrometheusUtils {

  private static final Pattern SPLIT_PATTERN =
    Pattern.compile("(?<!(^|[A-Z_]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])");

  public static String toPrometheusName(String recordName, String metricName) {
    String baseName = StringUtils.capitalize(recordName) + StringUtils.capitalize(metricName);
    baseName = baseName.replace('-', '_');
    String[] parts = SPLIT_PATTERN.split(baseName);
    return String.join("_", parts).toLowerCase();
  }

}
