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
package org.apache.hadoop.hbase.regionserver.http;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.ipc.CallQueueInfo;
import org.apache.hadoop.hbase.monitoring.StateDumpServlet;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.regionserver.CompactSplit;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.MemStoreFlusher;
import org.apache.hadoop.hbase.util.LogMonitoring;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RSDumpServlet extends StateDumpServlet {
  private static final long serialVersionUID = 1L;
  private static final String LINE = "===========================================================";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(), request, response)) {
      return;
    }
    HRegionServer hrs =
      (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);
    assert hrs != null : "No RS in context!";

    response.setContentType("text/plain");

    if (!hrs.isOnline()) {
      response.getWriter().write("The RegionServer is initializing!");
      response.getWriter().close();
      return;
    }

    OutputStreamWriter os =
      new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8);
    try (PrintWriter out = new PrintWriter(os)) {

      out.println("RegionServer status for " + hrs.getServerName() + " as of " + new Date());

      out.println("\n\nVersion Info:");
      out.println(LINE);
      dumpVersionInfo(out);

      out.println("\n\nTasks:");
      out.println(LINE);
      TaskMonitor.get().dumpAsText(out);

      out.println("\n\nRowLocks:");
      out.println(LINE);
      hrs.dumpRowLocks(out);

      out.println("\n\nExecutors:");
      out.println(LINE);
      dumpExecutors(hrs.getExecutorService(), out);

      out.println("\n\nStacks:");
      out.println(LINE);
      PrintStream ps = new PrintStream(response.getOutputStream(), false, StandardCharsets.UTF_8);
      Threads.printThreadInfo(ps, "");
      ps.flush();

      out.println("\n\nRS Configuration:");
      out.println(LINE);
      Configuration redactedConf = getRedactedConfiguration(hrs.getConfiguration());
      out.flush();
      redactedConf.writeXml(os);
      os.flush();

      out.println("\n\nLogs");
      out.println(LINE);
      long tailKb = getTailKbParam(request);
      LogMonitoring.dumpTailOfLogs(out, tailKb);

      out.println("\n\nRS Queue:");
      out.println(LINE);
      if (isShowQueueDump(hrs.getConfiguration())) {
        dumpQueue(hrs, out);
      }

      out.println("\n\nCall Queue Summary:");
      out.println(LINE);
      dumpCallQueues(hrs, out);

      out.flush();
    }
  }

  public static void dumpQueue(HRegionServer hrs, PrintWriter out) {
    final CompactSplit compactSplit = hrs.getCompactSplitThread();
    if (compactSplit != null) {
      // 1. Print out Compaction/Split Queue
      out.println("Compaction/Split Queue summary: " + compactSplit);
      out.println(compactSplit.dumpQueue());
    }

    final MemStoreFlusher memStoreFlusher = hrs.getMemStoreFlusher();
    if (memStoreFlusher != null) {
      // 2. Print out flush Queue
      out.println();
      out.println("Flush Queue summary: " + memStoreFlusher);
      out.println(memStoreFlusher.dumpQueue());
    }
  }

  public static void dumpCallQueues(HRegionServer hrs, PrintWriter out) {
    CallQueueInfo callQueueInfo = hrs.getRpcServer().getScheduler().getCallQueueInfo();

    for (String queueName : callQueueInfo.getCallQueueNames()) {

      out.println("\nQueue Name: " + queueName);

      long totalCallCount = 0L, totalCallSize = 0L;
      for (String methodName : callQueueInfo.getCalledMethodNames(queueName)) {
        long thisMethodCount, thisMethodSize;
        thisMethodCount = callQueueInfo.getCallMethodCount(queueName, methodName);
        thisMethodSize = callQueueInfo.getCallMethodSize(queueName, methodName);

        out.println("Method in call: " + methodName);
        out.println("Total call count for method: " + thisMethodCount);
        out.println("Total call size for method (bytes): " + thisMethodSize);

        totalCallCount += thisMethodCount;
        totalCallSize += thisMethodSize;
      }
      out.println("Total call count for queue: " + totalCallCount);
      out.println("Total call size for queue (bytes): " + totalCallSize);
    }
  }
}
