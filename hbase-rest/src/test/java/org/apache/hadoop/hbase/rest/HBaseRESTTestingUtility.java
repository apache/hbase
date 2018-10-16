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

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.http.HttpServerUtil;
import org.apache.hadoop.util.StringUtils;

import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHolder;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;
import java.util.Arrays;
import java.util.EnumSet;

public class HBaseRESTTestingUtility {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseRESTTestingUtility.class);

  private int testServletPort;
  private Server server;

  public int getServletPort() {
    return testServletPort;
  }

  public void startServletContainer(Configuration conf) throws Exception {
    if (server != null) {
      LOG.error("ServletContainer already running");
      return;
    }

    // Inject the conf for the test by being first to make singleton
    RESTServlet.getInstance(conf, UserProvider.instantiate(conf));

    // set up the Jersey servlet container for Jetty
    ResourceConfig app = new ResourceConfig().
        packages("org.apache.hadoop.hbase.rest").register(JacksonJaxbJsonProvider.class);
    ServletHolder sh = new ServletHolder(new ServletContainer(app));

    // set up Jetty and run the embedded server
    server = new Server(0);
    LOG.info("configured " + ServletContainer.class.getName());

    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSendDateHeader(false);
    httpConfig.setSendServerVersion(false);
    ServerConnector serverConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
    serverConnector.setPort(testServletPort);

    server.addConnector(serverConnector);

    // set up context
    ServletContextHandler ctxHandler = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    ctxHandler.addServlet(sh, "/*");
    // Load filters specified from configuration.
    String[] filterClasses = conf.getStrings(Constants.FILTER_CLASSES,
        ArrayUtils.EMPTY_STRING_ARRAY);
    for (String filter : filterClasses) {
      filter = filter.trim();
      ctxHandler.addFilter(filter, "/*", EnumSet.of(DispatcherType.REQUEST));
    }
    LOG.info("Loaded filter classes :" + Arrays.toString(filterClasses));

    conf.set(RESTServer.REST_CSRF_BROWSER_USERAGENTS_REGEX_KEY, ".*");
    RESTServer.addCSRFFilter(ctxHandler, conf);

    HttpServerUtil.constrainHttpMethods(ctxHandler, true);

    // start the server
    server.start();
    // get the port
    testServletPort = ((ServerConnector)server.getConnectors()[0]).getLocalPort();

    LOG.info("started " + server.getClass().getName() + " on port " +
      testServletPort);
  }

  public void shutdownServletContainer() {
    if (server != null) try {
      server.stop();
      server = null;
      RESTServlet.stop();
    } catch (Exception e) {
      LOG.warn(StringUtils.stringifyException(e));
    }
  }
}
