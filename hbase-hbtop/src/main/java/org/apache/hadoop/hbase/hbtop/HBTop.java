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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.hbtop.field.Field;
import org.apache.hadoop.hbase.hbtop.field.FieldInfo;
import org.apache.hadoop.hbase.hbtop.mode.Mode;
import org.apache.hadoop.hbase.hbtop.screen.Screen;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A real-time monitoring tool for HBase like Unix top command.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class HBTop extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(HBTop.class);

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
    List<Field> initialFields = null;
    Field initialSortField = null;
    Boolean initialAscendingSort = null;
    List<RecordFilter> initialFilters = null;
    long numberOfIterations = Long.MAX_VALUE;
    boolean batchMode = false;
    try {
      Options opts = getOptions();
      CommandLine commandLine = new BasicParser().parse(opts, args);

      if (commandLine.hasOption("help")) {
        printUsage(opts);
        return 0;
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
            LOG.warn("Mode set invalid, using default");
            break;
        }
      }

      if (commandLine.hasOption("outputFieldNames")) {
        for (FieldInfo fieldInfo : initialMode.getFieldInfos()) {
          System.out.println(fieldInfo.getField().getHeader());
        }
        return 0;
      }

      if (commandLine.hasOption("delay")) {
        int delay = 0;
        try {
          delay = Integer.parseInt(commandLine.getOptionValue("delay"));
        } catch (NumberFormatException ignored) {
        }

        if (delay < 1) {
          LOG.warn("Delay set too low or invalid, using default");
        } else {
          initialRefreshDelay = delay * 1000L;
        }
      }

      if (commandLine.hasOption("numberOfIterations")) {
        try {
          numberOfIterations = Long.parseLong(commandLine.getOptionValue("numberOfIterations"));
        } catch (NumberFormatException ignored) {
          LOG.warn("The number of iterations set invalid, ignoring");
        }
      }

      if (commandLine.hasOption("sortField")) {
        String sortField = commandLine.getOptionValue("sortField");

        String field;
        boolean ascendingSort;
        if (sortField.startsWith("+")) {
          field = sortField.substring(1);
          ascendingSort = false;
        } else if (sortField.startsWith("-")) {
          field = sortField.substring(1);
          ascendingSort = true;
        } else {
          field = sortField;
          ascendingSort = false;
        }

        FieldInfo fieldInfo = null;
        for (FieldInfo info : initialMode.getFieldInfos()) {
          if (info.getField().getHeader().equals(field)) {
            fieldInfo = info;
            break;
          }
        }
        if (fieldInfo != null) {
          initialSortField = fieldInfo.getField();
          initialAscendingSort = ascendingSort;
        } else {
          LOG.warn("The specified sort field " + field + " is not found, using default");
        }
      }

      if (commandLine.hasOption("fields")) {
        String[] fields = commandLine.getOptionValue("fields").split(",");
        initialFields = new ArrayList<>();
        for (String field : fields) {
          FieldInfo fieldInfo = null;
          for (FieldInfo info : initialMode.getFieldInfos()) {
            if (info.getField().getHeader().equals(field)) {
              fieldInfo = info;
              break;
            }
          }
          if (fieldInfo != null) {
            initialFields.add(fieldInfo.getField());
          } else {
            LOG.warn("The specified field " + field + " is not found, ignoring");
          }
        }
      }

      if (commandLine.hasOption("filters")) {
        String[] filters = commandLine.getOptionValue("filters").split(",");

        List<Field> fields = new ArrayList<>();
        for (FieldInfo fieldInfo : initialMode.getFieldInfos()) {
          fields.add(fieldInfo.getField());
        }

        for (String filter : filters) {
          RecordFilter f = RecordFilter.parse(filter, fields, false);
          if (f != null) {
            if (initialFilters == null) {
              initialFilters = new ArrayList<>();
            }
            initialFilters.add(f);
          } else {
            LOG.warn("The specified filter " + filter + " is invalid, ignoring");
          }
        }
      }

      if (commandLine.hasOption("batchMode")) {
        batchMode = true;
      }
    } catch (Exception e) {
      LOG.error("Unable to parse options", e);
      return 1;
    }

    try (Screen screen = new Screen(getConf(), initialRefreshDelay, initialMode, initialFields,
      initialSortField, initialAscendingSort, initialFilters, numberOfIterations, batchMode)) {
      screen.run();
    }

    return 0;
  }

  private Options getOptions() {
    Options opts = new Options();
    opts.addOption("h", "help", false,
      "Print usage; for help while the tool is running press 'h'");
    opts.addOption("d", "delay", true,
      "The refresh delay (in seconds); default is 3 seconds");
    opts.addOption("m", "mode", true,
      "The mode; n (Namespace)|t (Table)|r (Region)|s (RegionServer), default is r");
    opts.addOption("n", "numberOfIterations", true,
      "The number of iterations");
    opts.addOption("s", "sortField", true,
      "The initial sort field. You can prepend a `+' or `-' to the field name to also override"
        + " the sort direction. A leading `+' will force sorting high to low, whereas a `-' will"
        + " ensure a low to high ordering");
    opts.addOption("O", "outputFieldNames", false,
      "Print each of the available field names on a separate line, then quit");
    opts.addOption("f", "fields", true,
      "Show only the given fields. Specify comma separated fields to show multiple fields");
    opts.addOption("i", "filters", true,
      "The initial filters. Specify comma separated filters to set multiple filters");
    opts.addOption("b", "batchMode", false,
      "Starts hbtop in Batch mode, which could be useful for sending output from hbtop to other"
        + " programs or to a file. In this mode, hbtop will not accept input and runs until the"
        + " iterations limit you've set with the `-n' command-line option or until killed");
    return opts;
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
