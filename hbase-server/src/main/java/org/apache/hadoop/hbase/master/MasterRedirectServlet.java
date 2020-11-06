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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.util.DNS.MASTER_HOSTNAME_KEY;

import java.io.IOException;
import java.net.InetAddress;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
class MasterRedirectServlet extends HttpServlet {

  private static final long serialVersionUID = 2894774810058302473L;

  private static final Logger LOG = LoggerFactory.getLogger(MasterRedirectServlet.class);

  private final int regionServerInfoPort;
  private final String regionServerHostname;

  /**
   * @param infoServer that we're trying to send all requests to
   * @param hostname may be null. if given, will be used for redirects instead of host from client.
   */
  public MasterRedirectServlet(InfoServer infoServer, String hostname) {
    regionServerInfoPort = infoServer.getPort();
    regionServerHostname = hostname;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    String redirectHost = regionServerHostname;
    if (redirectHost == null) {
      redirectHost = request.getServerName();
      if (!Addressing.isLocalAddress(InetAddress.getByName(redirectHost))) {
        LOG.warn("Couldn't resolve '" + redirectHost + "' as an address local to this node and '" +
          MASTER_HOSTNAME_KEY + "' is not set; client will get an HTTP 400 response. If " +
          "your HBase deployment relies on client accessible names that the region server " +
          "process can't resolve locally, then you should set the previously mentioned " +
          "configuration variable to an appropriate hostname.");
        // no sending client provided input back to the client, so the goal host is just in the
        // logs.
        response.sendError(400,
          "Request was to a host that I can't resolve for any of the network interfaces on " +
            "this node. If this is due to an intermediary such as an HTTP load balancer or " +
            "other proxy, your HBase administrator can set '" + MASTER_HOSTNAME_KEY +
            "' to point to the correct hostname.");
        return;
      }
    }
    // TODO: this scheme should come from looking at the scheme registered in the infoserver's http
    // server for the host and port we're using, but it's buried way too deep to do that ATM.
    String redirectUrl = request.getScheme() + "://" + redirectHost + ":" + regionServerInfoPort +
      request.getRequestURI();
    response.sendRedirect(redirectUrl);
  }
}