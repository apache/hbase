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
package org.apache.hadoop.hbase.util.hbck;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.io.MultipleIOException;

/**
 * This code is used to rebuild meta off line from file system data. If there
 * are any problem detected, it will fail suggesting actions for the user to do
 * to "fix" problems. If it succeeds, it will backup the previous hbase:meta and
 * -ROOT- dirs and write new tables in place.
 *
 * This is an advanced feature, so is only exposed for use if explicitly
 * mentioned.
 *
 * hbase org.apache.hadoop.hbase.util.hbck.OfflineMetaRepair ...
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class OfflineMetaRepair {
  private static final Logger LOG = LoggerFactory.getLogger(OfflineMetaRepair.class.getName());

  protected static void printUsageAndExit() {
    StringBuilder sb = new StringBuilder();
    sb.append("Usage: OfflineMetaRepair [opts]\n").
       append(" where [opts] are:\n").
       append("   -details               Display full report of all regions.\n").
       append("   -base <hdfs://>        Base Hbase Data directory.\n").
       append("   -sidelineDir <hdfs://> HDFS path to backup existing meta and root.\n").
       append("   -fix                   Auto fix as many problems as possible.\n").
       append("   -fixHoles              Auto fix as region holes.");
    System.err.println(sb.toString());
    Runtime.getRuntime().exit(-2);
  }

  /**
   * Main program
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    // create a fsck object
    Configuration conf = HBaseConfiguration.create();
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    FSUtils.setFsDefault(conf, FSUtils.getRootDir(conf));
    HBaseFsck fsck = new HBaseFsck(conf);
    boolean fixHoles = false;

    // Process command-line args.
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-details")) {
        HBaseFsck.setDisplayFullReport();
      } else if (cmd.equals("-base")) {
        if (i == args.length - 1) {
          System.err.println("OfflineMetaRepair: -base needs an HDFS path.");
          printUsageAndExit();
        }
        // update hbase root dir to user-specified base
        i++;
        FSUtils.setRootDir(conf, new Path(args[i]));
        FSUtils.setFsDefault(conf, FSUtils.getRootDir(conf));
      } else if (cmd.equals("-sidelineDir")) {
        if (i == args.length - 1) {
          System.err.println("OfflineMetaRepair: -sidelineDir needs an HDFS path.");
          printUsageAndExit();
        }
        // set the hbck sideline dir to user-specified one
        i++;
        fsck.setSidelineDir(args[i]);
      } else if (cmd.equals("-fixHoles")) {
        fixHoles = true;
      } else if (cmd.equals("-fix")) {
        // make all fix options true
        fixHoles = true;
      } else {
        String str = "Unknown command line option : " + cmd;
        LOG.info(str);
        System.out.println(str);
        printUsageAndExit();
      }
    }

    System.out.println("OfflineMetaRepair command line options: " + StringUtils.join(args, " "));

    // Fsck doesn't shutdown and and doesn't provide a way to shutdown its
    // threads cleanly, so we do a System.exit.
    boolean success = false;
    try {
      success = fsck.rebuildMeta(fixHoles);
    } catch (MultipleIOException mioes) {
      for (IOException ioe : mioes.getExceptions()) {
        LOG.error("Bailed out due to:", ioe);
      }
    } catch (Exception e) {
      LOG.error("Bailed out due to: ", e);
    } finally {
      System.exit(success ? 0 : 1);
    }
  }
}
