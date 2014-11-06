/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Common base class used for HBase command-line tools. Simplifies workflow and
 * command-line argument parsing.
 */
@InterfaceAudience.Private
public abstract class AbstractHBaseTool implements Tool {

  protected static final int EXIT_SUCCESS = 0;
  protected static final int EXIT_FAILURE = 1;

  private static final String SHORT_HELP_OPTION = "h";
  private static final String LONG_HELP_OPTION = "help";

  private static final Log LOG = LogFactory.getLog(AbstractHBaseTool.class);

  private final Options options = new Options();

  protected Configuration conf = null;

  private static final Set<String> requiredOptions = new TreeSet<String>();

  protected String[] cmdLineArgs = null;

  /**
   * Override this to add command-line options using {@link #addOptWithArg}
   * and similar methods.
   */
  protected abstract void addOptions();

  /**
   * This method is called to process the options after they have been parsed.
   */
  protected abstract void processOptions(CommandLine cmd);

  /** The "main function" of the tool */
  protected abstract int doWork() throws Exception;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public final int run(String[] args) throws IOException {
    if (conf == null) {
      LOG.error("Tool configuration is not initialized");
      throw new NullPointerException("conf");
    }

    CommandLine cmd;
    try {
      // parse the command line arguments
      cmd = parseArgs(args);
      cmdLineArgs = args;
    } catch (ParseException e) {
      LOG.error("Error when parsing command-line arguments", e);
      printUsage();
      return EXIT_FAILURE;
    }

    if (cmd.hasOption(SHORT_HELP_OPTION) || cmd.hasOption(LONG_HELP_OPTION) ||
        !sanityCheckOptions(cmd)) {
      printUsage();
      return EXIT_FAILURE;
    }

    processOptions(cmd);

    int ret = EXIT_FAILURE;
    try {
      ret = doWork();
    } catch (Exception e) {
      LOG.error("Error running command-line tool", e);
      return EXIT_FAILURE;
    }
    return ret;
  }

  private boolean sanityCheckOptions(CommandLine cmd) {
    boolean success = true;
    for (String reqOpt : requiredOptions) {
      if (!cmd.hasOption(reqOpt)) {
        LOG.error("Required option -" + reqOpt + " is missing");
        success = false;
      }
    }
    return success;
  }

  protected CommandLine parseArgs(String[] args) throws ParseException {
    options.addOption(SHORT_HELP_OPTION, LONG_HELP_OPTION, false, "Show usage");
    addOptions();
    CommandLineParser parser = new BasicParser();
    return parser.parse(options, args);
  }

  protected void printUsage() {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.setWidth(80);
    String usageHeader = "Options:";
    String usageFooter = "";
    String usageStr = "bin/hbase " + getClass().getName() + " <options>";

    helpFormatter.printHelp(usageStr, usageHeader, options,
        usageFooter);
  }

  protected void addRequiredOptWithArg(String opt, String description) {
    requiredOptions.add(opt);
    addOptWithArg(opt, description);
  }

  protected void addRequiredOptWithArg(String shortOpt, String longOpt, String description) {
    requiredOptions.add(longOpt);
    addOptWithArg(shortOpt, longOpt, description);
  }

  protected void addOptNoArg(String opt, String description) {
    options.addOption(opt, false, description);
  }

  protected void addOptNoArg(String shortOpt, String longOpt, String description) {
    options.addOption(shortOpt, longOpt, false, description);
  }

  protected void addOptWithArg(String opt, String description) {
    options.addOption(opt, true, description);
  }

  protected void addOptWithArg(String shortOpt, String longOpt, String description) {
    options.addOption(shortOpt, longOpt, true, description);
  }

  /**
   * Parse a number and enforce a range.
   */
  public static long parseLong(String s, long minValue, long maxValue) {
    long l = Long.parseLong(s);
    if (l < minValue || l > maxValue) {
      throw new IllegalArgumentException("The value " + l
          + " is out of range [" + minValue + ", " + maxValue + "]");
    }
    return l;
  }

  public static int parseInt(String s, int minValue, int maxValue) {
    return (int) parseLong(s, minValue, maxValue);
  }

  /** Call this from the concrete tool class's main function. */
  protected void doStaticMain(String args[]) {
    int ret;
    try {
      ret = ToolRunner.run(HBaseConfiguration.create(), this, args);
    } catch (Exception ex) {
      LOG.error("Error running command-line tool", ex);
      ret = EXIT_FAILURE;
    }
    System.exit(ret);
  }

}
