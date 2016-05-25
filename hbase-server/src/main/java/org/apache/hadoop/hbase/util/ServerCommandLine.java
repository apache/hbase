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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Base class for command lines that start up various HBase daemons.
 */
@InterfaceAudience.Private
public abstract class ServerCommandLine extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(ServerCommandLine.class);
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
   * Logs information about the currently running JVM process including
   * the environment variables. Logging of env vars can be disabled by
   * setting {@code "hbase.envvars.logging.disabled"} to {@code "true"}.
   * <p>If enabled, you can also exclude environment variables containing
   * certain substrings by setting {@code "hbase.envvars.logging.skipwords"}
   * to comma separated list of such substrings.
   */
  public static void logProcessInfo(Configuration conf) {
    // log environment variables unless asked not to
    if (conf == null || !conf.getBoolean("hbase.envvars.logging.disabled", false)) {
      Set<String> skipWords = new HashSet<String>(DEFAULT_SKIP_WORDS);
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
   * Parse and run the given command line. This may exit the JVM if
   * a nonzero exit code is returned from <code>run()</code>.
   */
  public void doMain(String args[]) {
    try {
      int ret = ToolRunner.run(HBaseConfiguration.create(), this, args);
      if (ret != 0) {
        System.exit(ret);
      }
    } catch (Exception e) {
      LOG.error("Failed to run", e);
      System.exit(-1);
    }
  }
}
