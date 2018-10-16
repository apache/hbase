/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.hadoop.hbase.util.JSONBean;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility for doing JSON and MBeans.
 */
@InterfaceAudience.Private
public final class DumpRegionServerMetrics {
  /**
   * Dump out a subset of regionserver mbeans only, not all of them, as json on System.out.
   */
  public static String dumpMetrics() throws MalformedObjectNameException, IOException {
    StringWriter sw = new StringWriter(1024 * 100); // Guess this size
    try (PrintWriter writer = new PrintWriter(sw)) {
      JSONBean dumper = new JSONBean();
      try (JSONBean.Writer jsonBeanWriter = dumper.open(writer)) {
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        jsonBeanWriter.write(mbeanServer,
          new ObjectName("java.lang:type=Memory"), null, false);
        jsonBeanWriter.write(mbeanServer,
          new ObjectName("Hadoop:service=HBase,name=RegionServer,sub=IPC"), null, false);
        jsonBeanWriter.write(mbeanServer,
          new ObjectName("Hadoop:service=HBase,name=RegionServer,sub=Replication"), null, false);
        jsonBeanWriter.write(mbeanServer,
          new ObjectName("Hadoop:service=HBase,name=RegionServer,sub=Server"), null, false);
      }
    }
    sw.close();
    return sw.toString();
  }

  public static void main(String[] args) throws IOException, MalformedObjectNameException {
    String str = dumpMetrics();
    System.out.println(str);
  }

  private DumpRegionServerMetrics() {}
}
