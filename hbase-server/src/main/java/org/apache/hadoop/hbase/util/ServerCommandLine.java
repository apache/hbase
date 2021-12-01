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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hbase.util.Threads.isNonDaemonThreadRunning;

/**
 * Base class for command lines that start up various HBase daemons.
 */
@InterfaceAudience.Private
public abstract class ServerCommandLine extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(ServerCommandLine.class);
  @SuppressWarnings("serial")
  private static final Set<String> DEFAULT_SKIP_WORDS = new HashSet<String>() {
    {
      add("secret");
      add("passwd");
      add("password");
      add("credential");
    }
  };

  /**
   * Implementing subclasses should return a usage string to print out.
   */
  protected abstract String getUsage();

  /**
   * Print usage information for this command line.
   *
   * @param message if not null, print this message before the usage info.
   */
  protected void usage(String message) {
    if (message != null) {
      System.err.println(message);
      System.err.println("");
    }

    System.err.println(getUsage());
  }

  /**
   * Log information about the currently running JVM.
   */
  public static void logJVMInfo() {
    // Print out vm stats before starting up.
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    if (runtime != null) {
      LOG.info("vmName=" + runtime.getVmName() + ", vmVendor=" +
               runtime.getVmVendor() + ", vmVersion=" + runtime.getVmVersion());
      LOG.info("vmInputArguments=" + runtime.getInputArguments());
    }
  }

  /**
   * Print into log some of the important hbase attributes.
   */
  private static void logHBaseConfigs(Configuration conf) {
    final String [] keys = new String [] {
      // Expand this list as you see fit.
      "hbase.tmp.dir",
      HConstants.HBASE_DIR,
      HConstants.CLUSTER_DISTRIBUTED,
      HConstants.ZOOKEEPER_QUORUM,

    };
    for (String key: keys) {
      LOG.info(key + ": " + conf.get(key));
    }
  }

  /**
   * Logs information about the currently running JVM process including
   * the environment variables. Logging of env vars can be disabled by
   * setting {@code "hbase.envvars.logging.disabled"} to {@code "true"}.
   * <p>If enabled, you can also exclude environment variables containing
   * certain substrings by setting {@code "hbase.envvars.logging.skipwords"}
   * to comma separated list of such substrings.
   */
  public static void logProcessInfo(Configuration conf) {
    logHBaseConfigs(conf);

    // log environment variables unless asked not to
    if (conf == null || !conf.getBoolean("hbase.envvars.logging.disabled", false)) {
      Set<String> skipWords = new HashSet<>(DEFAULT_SKIP_WORDS);
      if (conf != null) {
        String[] confSkipWords = conf.getStrings("hbase.envvars.logging.skipwords");
        if (confSkipWords != null) {
          skipWords.addAll(Arrays.asList(confSkipWords));
        }
      }

      nextEnv:
      for (Entry<String, String> entry : System.getenv().entrySet()) {
        String key = entry.getKey().toLowerCase(Locale.ROOT);
        String value = entry.getValue().toLowerCase(Locale.ROOT);
        // exclude variables which may contain skip words
        for(String skipWord : skipWords) {
          if (key.contains(skipWord) || value.contains(skipWord))
            continue nextEnv;
        }
        LOG.info("env:"+entry);
      }
    }

    // and JVM info
    logJVMInfo();
  }

  /**
   * Parse and run the given command line. This will exit the JVM with
   * the exit code returned from <code>run()</code>.
   * If return code is 0, wait for atmost 30 seconds for all non-daemon threads to quit,
   * otherwise exit the jvm
   */
  public void doMain(String args[]) {
    try {
      int ret = ToolRunner.run(HBaseConfiguration.create(), this, args);
      if (ret != 0) {
        System.exit(ret);
      }
      // Return code is 0 here.
      boolean forceStop = false;
      long startTime = EnvironmentEdgeManager.currentTime();
      while (isNonDaemonThreadRunning()) {
        if (EnvironmentEdgeManager.currentTime() - startTime > 30 * 1000) {
          forceStop = true;
          break;
        }
        Thread.sleep(1000);
      }
      if (forceStop) {
        LOG.error("Failed to stop all non-daemon threads, so terminating JVM");
        System.exit(-1);
      }
    } catch (Exception e) {
      LOG.error("Failed to run", e);
      System.exit(-1);
    }
  }
}
