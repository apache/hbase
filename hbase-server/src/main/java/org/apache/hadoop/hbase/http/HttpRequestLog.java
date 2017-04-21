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

import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogConfigurationException;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;

import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.NCSARequestLog;

/**
 * RequestLog object for use with Http
 */
public class HttpRequestLog {

  private static final Log LOG = LogFactory.getLog(HttpRequestLog.class);
  private static final HashMap<String, String> serverToComponent;

  static {
    serverToComponent = new HashMap<>();
    serverToComponent.put("master", "master");
    serverToComponent.put("region", "regionserver");
  }

  public static RequestLog getRequestLog(String name) {

    String lookup = serverToComponent.get(name);
    if (lookup != null) {
      name = lookup;
    }
    String loggerName = "http.requests." + name;
    String appenderName = name + "requestlog";
    Log logger = LogFactory.getLog(loggerName);

    if (logger instanceof Log4JLogger) {
      Log4JLogger httpLog4JLog = (Log4JLogger)logger;
      Logger httpLogger = httpLog4JLog.getLogger();
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
    else {
      LOG.warn("Jetty request log can only be enabled using Log4j");
      return null;
    }
  }
}
