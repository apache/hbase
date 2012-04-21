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

package org.apache.hadoop.hbase.thrift;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.thrift.ThriftServerRunner.ImplType;
import org.apache.hadoop.util.Shell.ExitCodeException;

import com.google.common.base.Joiner;

/**
 * ThriftServer- this class starts up a Thrift server which implements the
 * Hbase API specified in the Hbase.thrift IDL file. The server runs in an
 * independent process.
 */
public class ThriftServer {

  public static final String CONF_PREFIX = HConstants.THRIFT_PROXY_PREFIX;

  private static final Log LOG = LogFactory.getLog(ThriftServer.class);

  private static final String MIN_WORKERS_OPTION = "minWorkers";
  private static final String MAX_WORKERS_OPTION = "workers";
  private static final String MAX_QUEUE_SIZE_OPTION = "queue";
  private static final String KEEP_ALIVE_SEC_OPTION = "keepAliveSec";
  static final String BIND_OPTION = "bind";
  static final String COMPACT_OPTION = "compact";
  static final String FRAMED_OPTION = "framed";
  static final String PORT_OPTION = "port";

  private Configuration conf;
  ThriftServerRunner serverRunner;

  //
  // Main program and support routines
  //

  public ThriftServer(Configuration conf) {
    this.conf = HBaseConfiguration.create(conf);
  }

  private static void printUsageAndExit(Options options, int exitCode)
      throws ExitCodeException {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Thrift", null, options,
        "To start the Thrift server run 'bin/hbase-daemon.sh start thrift'\n" +
        "To shutdown the thrift server run 'bin/hbase-daemon.sh stop " +
        "thrift' or send a kill signal to the thrift server pid",
        true);
    throw new ExitCodeException(exitCode, "");
  }

  /**
   * Start up or shuts down the Thrift server, depending on the arguments.
   * @param args
   */
   void doMain(final String[] args) throws Exception {
     processOptions(args);
     serverRunner = new ThriftServerRunner(conf, HConstants.THRIFT_PROXY_PREFIX);
     serverRunner.run();
  }

  /**
   * Parse the command line options to set parameters the conf.
   */
  private void processOptions(final String[] args) throws Exception {
    Options options = new Options();
    options.addOption("b", BIND_OPTION, true, "Address to bind " +
        "the Thrift server to. Not supported by the following server types: " +
        Joiner.on(", ").join(ImplType.serversThatCannotSpecifyBindIP()) +
        ". Default value: " + HConstants.DEFAULT_HOST + ".");
    options.addOption("p", PORT_OPTION, true, "Port to bind to [default: " +
        HConstants.DEFAULT_THRIFT_PROXY_PORT + "]");
    options.addOption("f", FRAMED_OPTION, false, "Use framed transport");
    options.addOption("c", COMPACT_OPTION, false, "Use the compact protocol");
    options.addOption("h", "help", false, "Print help information");

    options.addOption("m", MIN_WORKERS_OPTION, true,
        "The minimum number of worker threads for " +
        ImplType.THREAD_POOL.simpleClassName());

    options.addOption("w", MAX_WORKERS_OPTION, true,
        "The maximum number of worker threads for " +
        ImplType.THREAD_POOL.simpleClassName());

    options.addOption("q", MAX_QUEUE_SIZE_OPTION, true,
        "The maximum number of queued requests in " +
        ImplType.THREAD_POOL.simpleClassName());

    options.addOption("k", KEEP_ALIVE_SEC_OPTION, true,
        "The amount of time in secods to keep a thread alive when idle in " +
        ImplType.THREAD_POOL.simpleClassName());

    options.addOptionGroup(ImplType.createOptionGroup());

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    // This is so complicated to please both bin/hbase and bin/hbase-daemon.
    // hbase-daemon provides "start" and "stop" arguments
    // hbase should print the help if no argument is provided
    List<String> commandLine = Arrays.asList(args);
    boolean stop = commandLine.contains("stop");
    boolean start = commandLine.contains("start");
    boolean invalidStartStop = (start && stop) || (!start && !stop);
    if (cmd.hasOption("help") || invalidStartStop) {
      if (invalidStartStop) {
        LOG.error("Exactly one of 'start' and 'stop' has to be specified");
      }
      printUsageAndExit(options, 1);
    }

    // Get port to bind to
    try {
      int listenPort = Integer.parseInt(cmd.getOptionValue(PORT_OPTION,
          String.valueOf(HConstants.DEFAULT_THRIFT_PROXY_PORT)));
      conf.setInt(CONF_PREFIX + HConstants.THRIFT_PORT_SUFFIX, listenPort);
    } catch (NumberFormatException e) {
      LOG.error("Could not parse the value provided for the port option", e);
      printUsageAndExit(options, -1);
    }

    // Make optional changes to the configuration based on command-line options
    optionToConf(cmd, MIN_WORKERS_OPTION,
        conf, CONF_PREFIX + TBoundedThreadPoolServer.MIN_WORKER_THREADS_SUFFIX);
    optionToConf(cmd, MAX_WORKERS_OPTION,
        conf, CONF_PREFIX + TBoundedThreadPoolServer.MAX_WORKER_THREADS_SUFFIX);
    optionToConf(cmd, MAX_QUEUE_SIZE_OPTION,
        conf, CONF_PREFIX + TBoundedThreadPoolServer.MAX_QUEUED_REQUESTS_SUFFIX);
    optionToConf(cmd, KEEP_ALIVE_SEC_OPTION,
        conf, CONF_PREFIX + TBoundedThreadPoolServer.THREAD_KEEP_ALIVE_TIME_SEC_SUFFIX);

    // Set general thrift server options
    conf.setBoolean(CONF_PREFIX + HConstants.THRIFT_COMPACT_SUFFIX, cmd.hasOption(COMPACT_OPTION));
    conf.setBoolean(CONF_PREFIX + HConstants.THRIFT_FRAMED_SUFFIX, cmd.hasOption(FRAMED_OPTION));
    if (cmd.hasOption(BIND_OPTION)) {
      conf.set(CONF_PREFIX + HConstants.THRIFT_BIND_SUFFIX, cmd.getOptionValue(BIND_OPTION));
    }

    ImplType.setServerImpl(cmd, conf, CONF_PREFIX);
  }

  public void stop() {
    serverRunner.shutdown();
  }

  private static void optionToConf(CommandLine cmd, String option,
      Configuration conf, String destConfKey) {
    if (cmd.hasOption(option)) {
      String value = cmd.getOptionValue(option);
      LOG.info("Set configuration key:" + destConfKey + " value:" + value);
      conf.set(destConfKey, value);
    }
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String [] args) throws Exception {
    try {
      new ThriftServer(HBaseConfiguration.create()).doMain(args);
    } catch (ExitCodeException ex) {
      System.exit(ex.getExitCode());
    }
  }
}
