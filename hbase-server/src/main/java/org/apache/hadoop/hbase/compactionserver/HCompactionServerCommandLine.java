/**
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
package org.apache.hadoop.hbase.compactionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.ServerCommandLine;
import org.apache.yetus.audience.InterfaceAudience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Class responsible for parsing the command line and starting the CompactionServer.
 */
@InterfaceAudience.Private
public class HCompactionServerCommandLine extends ServerCommandLine {
  private static final Logger LOG = LoggerFactory.getLogger(HCompactionServerCommandLine.class);

  private final Class<? extends HCompactionServer> compactionServerClass;

  private static final String USAGE = "Usage: HCompactionServer [-D conf.param=value] start";

  public HCompactionServerCommandLine(Class<? extends HCompactionServer> clazz) {
    this.compactionServerClass = clazz;
  }

  @Override
  protected String getUsage() {
    return USAGE;
  }

  private int start() throws Exception {
    Configuration conf = getConf();
    TraceUtil.initTracer(conf);
    try {
      // If 'local', don't start a compaction server here. Defer to
      // LocalHBaseCluster. It manages 'local' clusters.
      if (LocalHBaseCluster.isLocal(conf)) {
        LOG.warn("Not starting a distinct compaction server because "
            + HConstants.CLUSTER_DISTRIBUTED + " is false");
      } else {
        logProcessInfo(getConf());
        HCompactionServer hcs =
            HCompactionServer.constructCompactionServer(compactionServerClass, conf);
        hcs.start();
        hcs.join();
        if (hcs.isAborted()) {
          throw new RuntimeException("HCompactionServer Aborted");
        }
      }
    } catch (Throwable t) {
      LOG.error("Compaction server exiting", t);
      return 1;
    }
    return 0;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      usage(null);
      return 1;
    }

    String cmd = args[0];

    if ("start".equals(cmd)) {
      return start();
    } else if ("stop".equals(cmd)) {
      System.err.println("To shutdown the compactionserver run "
          + "hbase-daemon.sh stop compactionserver or send a kill signal to "
          + "the compactionserver pid");
      return 1;
    } else {
      usage("Unknown command: " + args[0]);
      return 1;
    }
  }
}
