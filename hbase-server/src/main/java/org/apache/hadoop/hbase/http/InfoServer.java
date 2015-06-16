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

package org.apache.hadoop.hbase.http;

import java.io.IOException;
import java.net.URI;

import javax.servlet.http.HttpServlet;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal
 * is to serve up status information for the server.
 * There are three contexts:
 *   "/stacks/" -&gt; points to stack trace
 *   "/static/" -&gt; points to common static files (src/hbase-webapps/static)
 *   "/" -&gt; the jsp server code from (src/hbase-webapps/&lt;name&gt;)
 */
@InterfaceAudience.Private
public class InfoServer {
  
  private static final String HBASE_APP_DIR = "hbase-webapps";
  private final org.apache.hadoop.hbase.http.HttpServer httpServer;

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/hbase-webapps/<code>name</code>.
   * @param name The name of the server
   * @param bindAddress address to bind to
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and
   * increment by 1 until it finds a free port.
   * @throws IOException e
   */
  public InfoServer(String name, String bindAddress, int port, boolean findPort,
      final Configuration c)
  throws IOException {
    HttpConfig httpConfig = new HttpConfig(c);
    HttpServer.Builder builder =
      new org.apache.hadoop.hbase.http.HttpServer.Builder();

      builder.setName(name).addEndpoint(URI.create(httpConfig.getSchemePrefix() +
        bindAddress + ":" +
        port)).setAppDir(HBASE_APP_DIR).setFindPort(findPort).setConf(c);
      String logDir = System.getProperty("hbase.log.dir");
      if (logDir != null) {
        builder.setLogDir(logDir);
      }
    if (httpConfig.isSecure()) {
    builder.keyPassword(HBaseConfiguration.getPassword(c, "ssl.server.keystore.keypassword", null))
      .keyStore(c.get("ssl.server.keystore.location"),
        HBaseConfiguration.getPassword(c,"ssl.server.keystore.password", null),
        c.get("ssl.server.keystore.type", "jks"))
      .trustStore(c.get("ssl.server.truststore.location"),
        HBaseConfiguration.getPassword(c, "ssl.server.truststore.password", null),
        c.get("ssl.server.truststore.type", "jks"));
    }
    this.httpServer = builder.build();
  }

  public void addServlet(String name, String pathSpec,
          Class<? extends HttpServlet> clazz) {
      this.httpServer.addServlet(name, pathSpec, clazz);
  }

  public void setAttribute(String name, Object value) {
    this.httpServer.setAttribute(name, value);
  }

  public void start() throws IOException {
    this.httpServer.start();
  }

  @Deprecated
  public int getPort() {
    return this.httpServer.getPort();
  }

  public void stop() throws Exception {
    this.httpServer.stop();
  }

}
