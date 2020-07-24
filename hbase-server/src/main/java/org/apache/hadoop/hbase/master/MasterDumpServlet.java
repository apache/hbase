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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.monitoring.StateDumpServlet;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.RSDumpServlet;
import org.apache.hadoop.hbase.util.LogMonitoring;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MasterDumpServlet extends StateDumpServlet {
  private static final long serialVersionUID = 1L;
  private static final String LINE =
    "===========================================================";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
    assert master != null : "No Master in context!";

    response.setContentType("text/plain");
    OutputStream os = response.getOutputStream();
    try (PrintWriter out = new PrintWriter(os)) {

      out.println("Master status for " + master.getServerName()
        + " as of " + new Date());

      out.println("\n\nVersion Info:");
      out.println(LINE);
      dumpVersionInfo(out);

      out.println("\n\nTasks:");
      out.println(LINE);
      TaskMonitor.get().dumpAsText(out);

      out.println("\n\nServers:");
      out.println(LINE);
      dumpServers(master, out);

      out.println("\n\nRegions-in-transition:");
      out.println(LINE);
      dumpRIT(master, out);

      out.println("\n\nExecutors:");
      out.println(LINE);
      dumpExecutors(master.getExecutorService(), out);

      out.println("\n\nStacks:");
      out.println(LINE);
      out.flush();
      PrintStream ps = new PrintStream(response.getOutputStream(), false, "UTF-8");
      Threads.printThreadInfo(ps, "");
      ps.flush();

      out.println("\n\nMaster configuration:");
      out.println(LINE);
      Configuration conf = master.getConfiguration();
      out.flush();
      conf.writeXml(os);
      os.flush();

      out.println("\n\nRecent regionserver aborts:");
      out.println(LINE);
      master.getRegionServerFatalLogBuffer().dumpTo(out);

      out.println("\n\nLogs");
      out.println(LINE);
      long tailKb = getTailKbParam(request);
      LogMonitoring.dumpTailOfLogs(out, tailKb);

      out.println("\n\nRS Queue:");
      out.println(LINE);
      if (isShowQueueDump(conf)) {
        RSDumpServlet.dumpQueue(master, out);
      }
      out.flush();
    }
  }


  private void dumpRIT(HMaster master, PrintWriter out) {
    AssignmentManager am = master.getAssignmentManager();
    if (am == null) {
      out.println("AssignmentManager is not initialized");
      return;
    }

    for (RegionStateNode rs : am.getRegionsInTransition()) {
      String rid = rs.getRegionInfo().getEncodedName();
      out.println("Region " + rid + ": " + rs.toDescriptiveString());
    }
  }

  private void dumpServers(HMaster master, PrintWriter out) {
    ServerManager sm = master.getServerManager();
    if (sm == null) {
      out.println("ServerManager is not initialized");
      return;
    }

    Map<ServerName, ServerMetrics> servers = sm.getOnlineServers();
    for (Map.Entry<ServerName, ServerMetrics> e : servers.entrySet()) {
      out.println(e.getKey() + ": " + e.getValue());
    }
  }
}
