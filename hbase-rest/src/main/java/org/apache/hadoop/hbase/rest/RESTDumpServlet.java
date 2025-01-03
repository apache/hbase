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
package org.apache.hadoop.hbase.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Date;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.monitoring.StateDumpServlet;
import org.apache.hadoop.hbase.util.LogMonitoring;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RESTDumpServlet extends StateDumpServlet {
  private static final long serialVersionUID = 1L;
  private static final String LINE = "===========================================================";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(), request, response)) {
      return;
    }

    RESTServer restServer = (RESTServer) getServletContext().getAttribute(RESTServer.REST_SERVER);
    assert restServer != null : "No REST Server in context!";

    response.setContentType("text/plain");
    OutputStream os = response.getOutputStream();
    try (PrintWriter out = new PrintWriter(os)) {

      out.println("REST Server status for " + restServer.getServerName() + " as of " + new Date());

      out.println("\n\nVersion Info:");
      out.println(LINE);
      dumpVersionInfo(out);

      out.println("\n\nStacks:");
      out.println(LINE);
      out.flush();
      PrintStream ps = new PrintStream(response.getOutputStream(), false, "UTF-8");
      Threads.printThreadInfo(ps, "");
      ps.flush();

      out.println("\n\nREST Server configuration:");
      out.println(LINE);
      Configuration conf = restServer.conf;
      out.flush();
      conf.writeXml(os);
      os.flush();

      out.println("\n\nLogs");
      out.println(LINE);
      long tailKb = getTailKbParam(request);
      LogMonitoring.dumpTailOfLogs(out, tailKb);

      out.flush();
    }
  }
}
