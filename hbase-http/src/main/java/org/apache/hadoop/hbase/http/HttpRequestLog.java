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

import java.util.HashMap;
import org.apache.commons.logging.LogConfigurationException;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.log4j.Appender;
import org.apache.log4j.LogManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.eclipse.jetty.server.NCSARequestLog;
import org.eclipse.jetty.server.RequestLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.Log4jLoggerAdapter;

/**
 * RequestLog object for use with Http
 */
@InterfaceAudience.Private
public final class HttpRequestLog {

  private static final Logger LOG = LoggerFactory.getLogger(HttpRequestLog.class);
  private static final HashMap<String, String> serverToComponent;

  static {
    serverToComponent = new HashMap<>();
    serverToComponent.put("master", "master");
    serverToComponent.put("region", "regionserver");
  }

  private static org.apache.log4j.Logger getLog4jLogger(String loggerName) {
    Logger logger = LoggerFactory.getLogger(loggerName);

    if (logger instanceof Log4JLogger) {
      Log4JLogger httpLog4JLog = (Log4JLogger)logger;
      return httpLog4JLog.getLogger();
    } else if (logger instanceof Log4jLoggerAdapter) {
      return LogManager.getLogger(loggerName);
    } else {
      return null;
    }
  }

  public static RequestLog getRequestLog(String name) {

    String lookup = serverToComponent.get(name);
    if (lookup != null) {
      name = lookup;
    }
    String loggerName = "http.requests." + name;
    String appenderName = name + "requestlog";

    org.apache.log4j.Logger httpLogger = getLog4jLogger(loggerName);

    if (httpLogger == null) {
      LOG.warn("Jetty request log can only be enabled using Log4j");
      return null;
    }

    Appender appender = null;

    try {
      appender = httpLogger.getAppender(appenderName);
    } catch (LogConfigurationException e) {
      LOG.warn("Http request log for " + loggerName
          + " could not be created");
      throw e;
    }

    if (appender == null) {
      LOG.info("Http request log for " + loggerName
          + " is not defined");
      return null;
    }

    if (appender instanceof HttpRequestLogAppender) {
      HttpRequestLogAppender requestLogAppender
        = (HttpRequestLogAppender)appender;
      NCSARequestLog requestLog = new NCSARequestLog();
      requestLog.setFilename(requestLogAppender.getFilename());
      requestLog.setRetainDays(requestLogAppender.getRetainDays());
      return requestLog;
    } else {
      LOG.warn("Jetty request log for " + loggerName
          + " was of the wrong class");
      return null;
    }
  }

  private HttpRequestLog() {}
}
