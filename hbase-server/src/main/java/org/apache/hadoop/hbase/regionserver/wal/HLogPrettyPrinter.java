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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * HLogPrettyPrinter prints the contents of a given HLog with a variety of
 * options affecting formatting and extent of content.
 * 
 * It targets two usage cases: pretty printing for ease of debugging directly by
 * humans, and JSON output for consumption by monitoring and/or maintenance
 * scripts.
 * 
 * It can filter by row, region, or sequence id.
 * 
 * It can also toggle output of values.
 * 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HLogPrettyPrinter {
  private boolean outputValues;
  private boolean outputJSON;
  // The following enable filtering by sequence, region, and row, respectively
  private long sequence;
  private String region;
  private String row;
  // enable in order to output a single list of transactions from several files
  private boolean persistentOutput;
  private boolean firstTxn;
  // useful for programatic capture of JSON output
  private PrintStream out;
  // for JSON encoding
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Basic constructor that simply initializes values to reasonable defaults.
   */
  public HLogPrettyPrinter() {
    outputValues = false;
    outputJSON = false;
    sequence = -1;
    region = null;
    row = null;
    persistentOutput = false;
    firstTxn = true;
    out = System.out;
  }

  /**
   * Fully specified constructor.
   * 
   * @param outputValues
   *          when true, enables output of values along with other log
   *          information
   * @param outputJSON
   *          when true, enables output in JSON format rather than a
   *          "pretty string"
   * @param sequence
   *          when nonnegative, serves as a filter; only log entries with this
   *          sequence id will be printed
   * @param region
   *          when not null, serves as a filter; only log entries from this
   *          region will be printed
   * @param row
   *          when not null, serves as a filter; only log entries from this row
   *          will be printed
   * @param persistentOutput
   *          keeps a single list running for multiple files. if enabled, the
   *          endPersistentOutput() method must be used!
   * @param out
   *          Specifies an alternative to stdout for the destination of this 
   *          PrettyPrinter's output.
   */
  public HLogPrettyPrinter(boolean outputValues, boolean outputJSON,
      long sequence, String region, String row, boolean persistentOutput,
      PrintStream out) {
    this.outputValues = outputValues;
    this.outputJSON = outputJSON;
    this.sequence = sequence;
    this.region = region;
    this.row = row;
    this.persistentOutput = persistentOutput;
    if (persistentOutput) {
      beginPersistentOutput();
    }
    this.out = out;
    this.firstTxn = true;
  }

  /**
   * turns value output on
   */
  public void enableValues() {
    outputValues = true;
  }

  /**
   * turns value output off
   */
  public void disableValues() {
    outputValues = false;
  }

  /**
   * turns JSON output on
   */
  public void enableJSON() {
    outputJSON = true;
  }

  /**
   * turns JSON output off, and turns on "pretty strings" for human consumption
   */
  public void disableJSON() {
    outputJSON = false;
  }

  /**
   * sets the region by which output will be filtered
   * 
   * @param sequence
   *          when nonnegative, serves as a filter; only log entries with this
   *          sequence id will be printed
   */
  public void setSequenceFilter(long sequence) {
    this.sequence = sequence;
  }

  /**
   * sets the region by which output will be filtered
   * 
   * @param region
   *          when not null, serves as a filter; only log entries from this
   *          region will be printed
   */
  public void setRegionFilter(String region) {
    this.region = region;
  }

  /**
   * sets the region by which output will be filtered
   * 
   * @param row
   *          when not null, serves as a filter; only log entries from this row
   *          will be printed
   */
  public void setRowFilter(String row) {
    this.row = row;
  }

  /**
   * enables output as a single, persistent list. at present, only relevant in
   * the case of JSON output.
   */
  public void beginPersistentOutput() {
    if (persistentOutput)
      return;
    persistentOutput = true;
    firstTxn = true;
    if (outputJSON)
      out.print("[");
  }

  /**
   * ends output of a single, persistent list. at present, only relevant in the
   * case of JSON output.
   */
  public void endPersistentOutput() {
    if (!persistentOutput)
      return;
    persistentOutput = false;
    if (outputJSON)
      out.print("]");
  }

  /**
   * reads a log file and outputs its contents, one transaction at a time, as
   * specified by the currently configured options
   * 
   * @param conf
   *          the HBase configuration relevant to this log file
   * @param p
   *          the path of the log file to be read
   * @throws IOException
   *           may be unable to access the configured filesystem or requested
   *           file.
   */
  public void processFile(final Configuration conf, final Path p)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.isFile(p)) {
      throw new IOException(p + " is not a file");
    }
    if (outputJSON && !persistentOutput) {
      out.print("[");
      firstTxn = true;
    }
    Reader log = HLogFactory.createReader(fs, p, conf);
    try {
      FSHLog.Entry entry;
      while ((entry = log.next()) != null) {
        HLogKey key = entry.getKey();
        WALEdit edit = entry.getEdit();
        // begin building a transaction structure
        Map<String, Object> txn = key.toStringMap();
        long writeTime = key.getWriteTime();
        // check output filters
        if (sequence >= 0 && ((Long) txn.get("sequence")) != sequence)
          continue;
        if (region != null && !((String) txn.get("region")).equals(region))
          continue;
        // initialize list into which we will store atomic actions
        List<Map> actions = new ArrayList<Map>();
        for (KeyValue kv : edit.getKeyValues()) {
          // add atomic operation to txn
          Map<String, Object> op = 
            new HashMap<String, Object>(kv.toStringMap());
          if (outputValues)
            op.put("value", Bytes.toStringBinary(kv.getValue()));
          // check row output filter
          if (row == null || ((String) op.get("row")).equals(row))
            actions.add(op);
        }
        if (actions.size() == 0)
          continue;
        txn.put("actions", actions);
        if (outputJSON) {
          // JSON output is a straightforward "toString" on the txn object
          if (firstTxn)
            firstTxn = false;
          else
            out.print(",");
          // encode and print JSON
          out.print(MAPPER.writeValueAsString(txn));
        } else {
          // Pretty output, complete with indentation by atomic action
          out.println("Sequence " + txn.get("sequence") + " "
              + "from region " + txn.get("region") + " " + "in table "
              + txn.get("table") + " at write timestamp: " + new Date(writeTime));
          for (int i = 0; i < actions.size(); i++) {
            Map op = actions.get(i);
            out.println("  Action:");
            out.println("    row: " + op.get("row"));
            out.println("    column: " + op.get("family") + ":"
                + op.get("qualifier"));
            out.println("    timestamp: "
                + (new Date((Long) op.get("timestamp"))));
            if(op.get("tag") != null) {
              out.println("    tag: " + op.get("tag"));
            }
            if (outputValues)
              out.println("    value: " + op.get("value"));
          }
        }
      }
    } finally {
      log.close();
    }
    if (outputJSON && !persistentOutput) {
      out.print("]");
    }
  }

  public static void main(String[] args) throws IOException {
    run(args);
  }

  /**
   * Pass one or more log file names and formatting options and it will dump out
   * a text version of the contents on <code>stdout</code>.
   * 
   * @param args
   *          Command line arguments
   * @throws IOException
   *           Thrown upon file system errors etc.
   * @throws ParseException
   *           Thrown if command-line parsing fails.
   */
  public static void run(String[] args) throws IOException {
    // create options
    Options options = new Options();
    options.addOption("h", "help", false, "Output help message");
    options.addOption("j", "json", false, "Output JSON");
    options.addOption("p", "printvals", false, "Print values");
    options.addOption("r", "region", true,
        "Region to filter by. Pass region name; e.g. 'hbase:meta,,1'");
    options.addOption("s", "sequence", true,
        "Sequence to filter by. Pass sequence number.");
    options.addOption("w", "row", true, "Row to filter by. Pass row name.");

    HLogPrettyPrinter printer = new HLogPrettyPrinter();
    CommandLineParser parser = new PosixParser();
    List files = null;
    try {
      CommandLine cmd = parser.parse(options, args);
      files = cmd.getArgList();
      if (files.size() == 0 || cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("HLog <filename...>", options, true);
        System.exit(-1);
      }
      // configure the pretty printer using command line options
      if (cmd.hasOption("p"))
        printer.enableValues();
      if (cmd.hasOption("j"))
        printer.enableJSON();
      if (cmd.hasOption("r"))
        printer.setRegionFilter(cmd.getOptionValue("r"));
      if (cmd.hasOption("s"))
        printer.setSequenceFilter(Long.parseLong(cmd.getOptionValue("s")));
      if (cmd.hasOption("w"))
        printer.setRowFilter(cmd.getOptionValue("w"));
    } catch (ParseException e) {
      e.printStackTrace();
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("HFile filename(s) ", options, true);
      System.exit(-1);
    }
    // get configuration, file system, and process the given files
    Configuration conf = HBaseConfiguration.create();
    FSUtils.setFsDefault(conf, FSUtils.getRootDir(conf));

    // begin output
    printer.beginPersistentOutput();
    for (Object f : files) {
      Path file = new Path((String) f);
      FileSystem fs = file.getFileSystem(conf);
      if (!fs.exists(file)) {
        System.err.println("ERROR, file doesnt exist: " + file);
        return;
      }
      printer.processFile(conf, file);
    }
    printer.endPersistentOutput();
  }
}
