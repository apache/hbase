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
package org.apache.hadoop.hbase.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.util.Set;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility functions for reading the log4j logs that are being written by HBase.
 */
@InterfaceAudience.Private
public abstract class LogMonitoring {

  public static void dumpTailOfLogs(PrintWriter out, long tailKb) throws IOException {
    Set<File> logs = Log4jUtils.getActiveLogFiles();
    for (File f : logs) {
      out.println("+++++++++++++++++++++++++++++++");
      out.println(f.getAbsolutePath());
      out.println("+++++++++++++++++++++++++++++++");
      try {
        dumpTailOfLog(f, out, tailKb);
      } catch (IOException ioe) {
        out.println("Unable to dump log at " + f);
        ioe.printStackTrace(out);
      }
      out.println("\n\n");
    }
  }

  private static void dumpTailOfLog(File f, PrintWriter out, long tailKb) throws IOException {
    FileInputStream fis = new FileInputStream(f);
    BufferedReader r = null;
    try {
      FileChannel channel = fis.getChannel();
      channel.position(Math.max(0, channel.size() - tailKb * 1024));
      r = new BufferedReader(new InputStreamReader(fis));
      r.readLine(); // skip the first partial line
      String line;
      while ((line = r.readLine()) != null) {
        out.println(line);
      }
    } finally {
      if (r != null) {
        IOUtils.closeStream(r);
      }
      IOUtils.closeStream(fis);
    }
  }
}
