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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Request;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.RequestLog;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Server;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.ServerConnector;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Slf4jRequestLog;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.handler.AbstractHandler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.RegexSet;

/**
 * A {@link org.junit.Rule} that manages a simple http server. The caller registers request
 * handlers to URI path regexp.
 */
public class MockHttpApiRule extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(MockHttpApiRule.class);

  private MockHandler handler;
  private Server server;

  /**
   * Register a callback handler for the specified path target.
   */
  public MockHttpApiRule addRegistration(
    final String pathRegex,
    final BiConsumer<String, HttpServletResponse> responder
  ) {
    handler.register(pathRegex, responder);
    return this;
  }

  /**
   * Shortcut method for calling {@link #addRegistration(String, BiConsumer)} with a 200 response.
   */
  public MockHttpApiRule registerOk(final String pathRegex, final String responseBody) {
    return addRegistration(pathRegex, (target, resp) -> {
      try {
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setCharacterEncoding("UTF-8");
        resp.setContentType(MediaType.APPLICATION_JSON_TYPE.toString());
        final PrintWriter writer = resp.getWriter();
        writer.write(responseBody);
        writer.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public void clearRegistrations() {
    handler.clearRegistrations();
  }

  /**
   * Retrieve the service URI for this service.
   */
  public URI getURI() {
    if (server == null || !server.isRunning()) {
      throw new IllegalStateException("server is not running");
    }
    return server.getURI();
  }

  @Override
  protected void before() throws Exception {
    handler = new MockHandler();
    server = new Server();
    final ServerConnector http = new ServerConnector(server);
    http.setHost("localhost");
    server.addConnector(http);
    server.setStopAtShutdown(true);
    server.setHandler(handler);
    server.setRequestLog(buildRequestLog());
    server.start();
  }

  @Override
  protected void after() {
    try {
      server.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static RequestLog buildRequestLog() {
    final Slf4jRequestLog requestLog = new Slf4jRequestLog();
    requestLog.setLoggerName(LOG.getName() + ".RequestLog");
    requestLog.setExtended(true);
    return requestLog;
  }

  private static class MockHandler extends AbstractHandler {

    private final ReadWriteLock responseMappingLock = new ReentrantReadWriteLock();
    private final Map<String, BiConsumer<String, HttpServletResponse>> responseMapping =
      new HashMap<>();
    private final RegexSet regexSet = new RegexSet();

    void register(
      final String pathRegex,
      final BiConsumer<String, HttpServletResponse> responder
    ) {
      LOG.debug("Registering responder to '{}'", pathRegex);
      responseMappingLock.writeLock().lock();
      try {
        responseMapping.put(pathRegex, responder);
        regexSet.add(pathRegex);
      } finally {
        responseMappingLock.writeLock().unlock();
      }
    }

    void clearRegistrations() {
      LOG.debug("Clearing registrations");
      responseMappingLock.writeLock().lock();
      try {
        responseMapping.clear();
        regexSet.clear();
      } finally {
        responseMappingLock.writeLock().unlock();
      }
    }

    @Override
    public void handle(
      final String target,
      final Request baseRequest,
      final HttpServletRequest request,
      final HttpServletResponse response
    ) {
      responseMappingLock.readLock().lock();
      try {
        if (!regexSet.matches(target)) {
          response.setStatus(HttpServletResponse.SC_NOT_FOUND);
          return;
        }
        responseMapping.entrySet()
          .stream()
          .filter(e -> Pattern.matches(e.getKey(), target))
          .findAny()
          .map(Map.Entry::getValue)
          .orElseThrow(() -> noMatchFound(target))
          .accept(target, response);
      } finally {
        responseMappingLock.readLock().unlock();
      }
    }

    private static RuntimeException noMatchFound(final String target) {
      return new RuntimeException(
        String.format("Target path '%s' matches no registered regex.", target));
    }
  }
}
