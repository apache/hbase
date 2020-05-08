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
package org.apache.hadoop.hbase.http;

import org.apache.yetus.audience.InterfaceAudience;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Slf4jRequestLog;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * RequestLog object for use with Http
 */
@InterfaceAudience.Private
public final class HttpRequestLog {

  private static final ImmutableMap<String, String> SERVER_TO_COMPONENT =
    ImmutableMap.of("master", "master", "region", "regionserver");

  public static RequestLog getRequestLog(String name) {
    String lookup = SERVER_TO_COMPONENT.get(name);
    if (lookup != null) {
      name = lookup;
    }
    String loggerName = "http.requests." + name;
    Slf4jRequestLog logger = new Slf4jRequestLog();
    logger.setLoggerName(loggerName);
    return logger;
  }

  private HttpRequestLog() {
  }
}
