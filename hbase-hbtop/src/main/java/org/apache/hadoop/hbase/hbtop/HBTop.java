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
package org.apache.hadoop.hbase.hbtop;

import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.hbtop.screen.Screen;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;


/**
 * A real-time monitoring tool for HBase like Unix top command.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class HBTop extends Configured implements Tool {

  private static final Logger LOGGER = LoggerFactory.getLogger(HBTop.class);

  public HBTop() {
    this(HBaseConfiguration.create());
  }

  public HBTop(Configuration conf) {
    super(Objects.requireNonNull(conf));
  }

  @Override
  public int run(String[] args) throws Exception {
    long initialRefreshDelay = 3 * 1000;
    Mode initialMode = Mode.REGION;
    try {
      // Command line options
      Options opts = new Options();
      opts.addOption("h", "help", false,
        "Print usage; for help while the tool is running press 'h'");
      opts.addOption("d", "delay", true,
        "The refresh delay (in seconds); default is 3 seconds");
      opts.addOption("m", "mode", true,
        "The mode; n (Namespace)|t (Table)|r (Region)|s (RegionServer)"
          + ", default is r (Region)");

      CommandLine commandLine = new DefaultParser().parse(opts, args);

      if (commandLine.hasOption("help")) {
        printUsage(opts);
        return 0;
      }

      if (commandLine.hasOption("delay")) {
        int delay = 0;
        try {
          delay = Integer.parseInt(commandLine.getOptionValue("delay"));
        } catch (NumberFormatException ignored) {
        }

        if (delay < 1) {
          LOGGER.warn("Delay set too low or invalid, using default");
        } else {
          initialRefreshDelay = delay * 1000L;
        }
      }

      if (commandLine.hasOption("mode")) {
        String mode = commandLine.getOptionValue("mode");
        switch (mode) {
          case "n":
            initialMode = Mode.NAMESPACE;
            break;

          case "t":
            initialMode = Mode.TABLE;
            break;

          case "r":
            initialMode = Mode.REGION;
            break;

          case "s":
            initialMode = Mode.REGION_SERVER;
            break;

          default:
            LOGGER.warn("Mode set invalid, using default");
            break;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Unable to parse options", e);
      return 1;
    }

    try (Screen screen = new Screen(getConf(), initialRefreshDelay, initialMode)) {
      screen.run();
    }

    return 0;
  }

  private void printUsage(Options opts) {
    new HelpFormatter().printHelp("hbase hbtop [opts] [-D<property=value>]*", opts);
    System.out.println("");
    System.out.println(" Note: -D properties will be applied to the conf used.");
    System.out.println("  For example:");
    System.out.println("   -Dhbase.client.zookeeper.quorum=<zookeeper quorum>");
    System.out.println("   -Dzookeeper.znode.parent=<znode parent>");
    System.out.println("");
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new HBTop(), args);
    System.exit(res);
  }
}
