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
package org.apache.hadoop.hbase.rest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseRESTTestingUtility {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseRESTTestingUtility.class);

  private RESTServer server;

  public int getServletPort() {
    return server.getPort();
  }

  public void startServletContainer(Configuration conf) throws Exception {
    if (server != null) {
      LOG.error("RESTServer already running");
      return;
    }

    conf.setInt("hbase.rest.port", 0);
    conf.setInt("hbase.rest.info.port", -1);
    conf.setBoolean(RESTServer.SKIP_LOGIN_KEY, true);

    server = new RESTServer(conf);
    server.run();

    LOG.info("started " + server.getClass().getName() + " on port " +
      server.getPort());
  }

  public void shutdownServletContainer() {
    if (server != null) {
      try {
        server.stop();
        server = null;
        RESTServlet.stop();
      } catch (Exception e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
    }
  }
}
