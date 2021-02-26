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
package org.apache.hadoop.hbase.wal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;
import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.PosixParser;

/**
 * WALPrettyPrinter prints the contents of a given WAL with a variety of
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
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class WALPrettyPrinter {
  private static final Logger LOG = LoggerFactory.getLogger(WALPrettyPrinter.class);

  // Output template for pretty printing.
  private static final String outputTmpl =
      "Sequence=%s, table=%s, region=%s, at write timestamp=%s";

  private boolean outputValues;
  private boolean outputJSON;
  // The following enable filtering by sequence, region, and row, respectively
  private long sequence;

  // List of tables for filter
  private final Set<String> tableSet;
  private String region;

  // exact row which needs to be filtered
  private String row;
  // prefix of rows which needs to be filtered
  private String rowPrefix;

  private boolean outputOnlyRowKey;
  // enable in order to output a single list of transactions from several files
  private boolean persistentOutput;
  private boolean firstTxn;
  // useful for programmatic capture of JSON output
  private PrintStream out;
  // for JSON encoding
  private static final Gson GSON = GsonUtil.createGson().create();
  //allows for jumping straight to a given portion of the file
  private long position;

  /**
   * Basic constructor that simply initializes values to reasonable defaults.
   */
  public WALPrettyPrinter() {
    this(false, false, -1, new HashSet<>(), null,
      null, null, false, false, System.out);
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
   * @param tableSet
   *          when non null, serves as a filter. only entries corresponding to tables
   *          in the tableSet are printed
   * @param region
   *          when not null, serves as a filter; only log entries from this
   *          region will be printed
   * @param row
   *          when not null, serves as a filter; only log entries from this row
   *          will be printed
   * @param rowPrefix
   *          when not null, serves as a filter; only log entries with row key
   *          having this prefix will be printed
   * @param persistentOutput
   *          keeps a single list running for multiple files. if enabled, the
   *          endPersistentOutput() method must be used!
   * @param out
   *          Specifies an alternative to stdout for the destination of this
   *          PrettyPrinter's output.
   */
  public WALPrettyPrinter(boolean outputValues, boolean outputJSON, long sequence,
    Set<String> tableSet, String region, String row, String rowPrefix, boolean outputOnlyRowKey,
    boolean persistentOutput, PrintStream out) {
    this.outputValues = outputValues;
    this.outputJSON = outputJSON;
    this.sequence = sequence;
    this.tableSet = tableSet;
    this.region = region;
    this.row = row;
    this.rowPrefix = rowPrefix;
    this.outputOnlyRowKey = outputOnlyRowKey;
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
   * Sets the tables filter. Only log entries for these tables are printed.
   * @param tablesWithDelimiter table names separated with comma.
   */
  public void setTableFilter(String tablesWithDelimiter) {
    Collections.addAll(tableSet, tablesWithDelimiter.split(","));
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
   * sets the row key by which output will be filtered
   *
   * @param row
   *          when not null, serves as a filter; only log entries from this row
   *          will be printed
   */
  public void setRowFilter(String row) {
    this.row = row;
  }

  /**
   * sets the rowPrefix key prefix by which output will be filtered
   *
   * @param rowPrefix
   *          when not null, serves as a filter; only log entries with rows
   *          having this prefix will be printed
   */
  public void setRowPrefixFilter(String rowPrefix) {
    this.rowPrefix = rowPrefix;
  }

  /**
   * Option to print the row key only in case you just need the row keys from the WAL
   */
  public void setOutputOnlyRowKey() {
    this.outputOnlyRowKey = true;
  }

  /**
   * sets the position to start seeking the WAL file
   * @param position
   *          initial position to start seeking the given WAL file
   */
  public void setPosition(long position) {
    this.position = position;
  }

  /**
   * enables output as a single, persistent list. at present, only relevant in
   * the case of JSON output.
   */
  public void beginPersistentOutput() {
    if (persistentOutput) {
      return;
    }
    persistentOutput = true;
    firstTxn = true;
    if (outputJSON) {
      out.print("[");
    }
  }

  /**
   * ends output of a single, persistent list. at present, only relevant in the
   * case of JSON output.
   */
  public void endPersistentOutput() {
    if (!persistentOutput) {
      return;
    }
    persistentOutput = false;
    if (outputJSON) {
      out.print("]");
    }
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
    FileSystem fs = p.getFileSystem(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    if (!fs.isFile(p)) {
      throw new IOException(p + " is not a file");
    }

    WAL.Reader log = WALFactory.createReader(fs, p, conf);

    if (log instanceof ProtobufLogReader) {
      List<String> writerClsNames = ((ProtobufLogReader) log).getWriterClsNames();
      if (writerClsNames != null && writerClsNames.size() > 0) {
        out.print("Writer Classes: ");
        for (int i = 0; i < writerClsNames.size(); i++) {
          out.print(writerClsNames.get(i));
          if (i != writerClsNames.size() - 1) {
            out.print(" ");
          }
        }
        out.println();
      }

      String cellCodecClsName = ((ProtobufLogReader) log).getCodecClsName();
      if (cellCodecClsName != null) {
        out.println("Cell Codec Class: " + cellCodecClsName);
      }
    }

    if (outputJSON && !persistentOutput) {
      out.print("[");
      firstTxn = true;
    }

    if (position > 0) {
      log.seek(position);
    }

    try {
      WAL.Entry entry;
      while ((entry = log.next()) != null) {
        WALKey key = entry.getKey();
        WALEdit edit = entry.getEdit();
        // begin building a transaction structure
        Map<String, Object> txn = key.toStringMap();
        long writeTime = key.getWriteTime();
        // check output filters
        if (!tableSet.isEmpty() &&
          !tableSet.contains(txn.get("table").toString())) {
          continue;
        }
        if (sequence >= 0 && ((Long) txn.get("sequence")) != sequence) {
          continue;
        }
        if (region != null && !txn.get("region").equals(region)) {
          continue;
        }
        // initialize list into which we will store atomic actions
        List<Map<String, Object>> actions = new ArrayList<>();
        for (Cell cell : edit.getCells()) {
          // add atomic operation to txn
          Map<String, Object> op =
            new HashMap<>(toStringMap(cell, outputOnlyRowKey, rowPrefix, row, outputValues));
          if (op.isEmpty()) {
            continue;
          }
          actions.add(op);
        }
        if (actions.isEmpty()) {
          continue;
        }
        txn.put("actions", actions);
        if (outputJSON) {
          // JSON output is a straightforward "toString" on the txn object
          if (firstTxn) {
            firstTxn = false;
          } else {
            out.print(",");
          }
          // encode and print JSON
          out.print(GSON.toJson(txn));
        } else {
          // Pretty output, complete with indentation by atomic action
          if (!outputOnlyRowKey) {
            out.println(String.format(outputTmpl,
              txn.get("sequence"), txn.get("table"), txn.get("region"), new Date(writeTime)));
          }
          for (int i = 0; i < actions.size(); i++) {
            Map<String, Object> op = actions.get(i);
            printCell(out, op, outputValues, outputOnlyRowKey);
          }
        }
        if (!outputOnlyRowKey) {
          out.println("edit heap size: " + entry.getEdit().heapSize());
          out.println("position: " + log.getPosition());
        }
      }
    } finally {
      log.close();
    }
    if (outputJSON && !persistentOutput) {
      out.print("]");
    }
  }

  public static void printCell(PrintStream out, Map<String, Object> op,
    boolean outputValues, boolean outputOnlyRowKey) {
    String rowDetails = "row=" + op.get("row");
    if (outputOnlyRowKey) {
      out.println(rowDetails);
      return;
    }

    rowDetails += ", column=" + op.get("family") + ":" + op.get("qualifier");
    rowDetails += ", type=" + op.get("type");
    out.println(rowDetails);
    if (op.get("tag") != null) {
      out.println("    tag: " + op.get("tag"));
    }
    if (outputValues) {
      out.println("    value: " + op.get("value"));
    }
    out.println("cell total size sum: " + op.get("total_size_sum"));
  }

  public static Map<String, Object> toStringMap(Cell cell,
    boolean printRowKeyOnly, String rowPrefix, String row, boolean outputValues) {
    Map<String, Object> stringMap = new HashMap<>();
    String rowKey = Bytes.toStringBinary(cell.getRowArray(),
      cell.getRowOffset(), cell.getRowLength());
    // Row and row prefix are mutually options so both cannot be true at the
    // same time. We can include checks in the same condition
    // Check if any of the filters are satisfied by the row, if not return empty map
    if ((!Strings.isNullOrEmpty(rowPrefix) && !rowKey.startsWith(rowPrefix)) ||
      (!Strings.isNullOrEmpty(row) && !rowKey.equals(row))) {
      return stringMap;
    }

    stringMap.put("row", rowKey);
    if (printRowKeyOnly) {
      return stringMap;
    }
    stringMap.put("type", cell.getType());
    stringMap.put("family", Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(),
      cell.getFamilyLength()));
    stringMap.put("qualifier",
      Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength()));
    stringMap.put("timestamp", cell.getTimestamp());
    stringMap.put("vlen", cell.getValueLength());
    stringMap.put("total_size_sum", cell.heapSize());
    if (cell.getTagsLength() > 0) {
      List<String> tagsString = new ArrayList<>();
      Iterator<Tag> tagsIterator = PrivateCellUtil.tagsIterator(cell);
      while (tagsIterator.hasNext()) {
        Tag tag = tagsIterator.next();
        tagsString
          .add((tag.getType()) + ":" + Bytes.toStringBinary(Tag.cloneValue(tag)));
      }
      stringMap.put("tag", tagsString);
    }
    if (outputValues) {
      stringMap.put("value", Bytes.toStringBinary(CellUtil.cloneValue(cell)));
    }
    return stringMap;
  }

  public static Map<String, Object> toStringMap(Cell cell) {
    return toStringMap(cell, false, null, null, false);
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
   */
  public static void run(String[] args) throws IOException {
    // create options
    Options options = new Options();
    options.addOption("h", "help", false, "Output help message");
    options.addOption("j", "json", false, "Output JSON");
    options.addOption("p", "printvals", false, "Print values");
    options.addOption("t", "tables", true,
      "Table names (comma separated) to filter by; eg: test1,test2,test3 ");
    options.addOption("r", "region", true,
        "Region to filter by. Pass encoded region name; e.g. '9192caead6a5a20acb4454ffbc79fa14'");
    options.addOption("s", "sequence", true,
        "Sequence to filter by. Pass sequence number.");
    options.addOption("k", "outputOnlyRowKey", false,
      "Print only row keys");
    options.addOption("w", "row", true, "Row to filter by. Pass row name.");
    options.addOption("f", "rowPrefix", true, "Row prefix to filter by.");
    options.addOption("g", "goto", true, "Position to seek to in the file");

    WALPrettyPrinter printer = new WALPrettyPrinter();
    CommandLineParser parser = new PosixParser();
    List<?> files = null;
    try {
      CommandLine cmd = parser.parse(options, args);
      files = cmd.getArgList();
      if (files.isEmpty() || cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("WAL <filename...>", options, true);
        System.exit(-1);
      }
      // configure the pretty printer using command line options
      if (cmd.hasOption("p")) {
        printer.enableValues();
      }
      if (cmd.hasOption("j")) {
        printer.enableJSON();
      }
      if (cmd.hasOption("k")) {
        printer.setOutputOnlyRowKey();
      }
      if (cmd.hasOption("t")) {
        printer.setTableFilter(cmd.getOptionValue("t"));
      }
      if (cmd.hasOption("r")) {
        printer.setRegionFilter(cmd.getOptionValue("r"));
      }
      if (cmd.hasOption("s")) {
        printer.setSequenceFilter(Long.parseLong(cmd.getOptionValue("s")));
      }
      if (cmd.hasOption("w")) {
        if (cmd.hasOption("f")) {
          throw new ParseException("Row and Row-prefix cannot be supplied together");
        }
        printer.setRowFilter(cmd.getOptionValue("w"));
      }
      if (cmd.hasOption("f")) {
        if (cmd.hasOption("w")) {
          throw new ParseException("Row and Row-prefix cannot be supplied together");
        }
        printer.setRowPrefixFilter(cmd.getOptionValue("f"));
      }
      if (cmd.hasOption("g")) {
        printer.setPosition(Long.parseLong(cmd.getOptionValue("g")));
      }
    } catch (ParseException e) {
      LOG.error("Failed to parse commandLine arguments", e);
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("HFile filename(s) ", options, true);
      System.exit(-1);
    }
    // get configuration, file system, and process the given files
    Configuration conf = HBaseConfiguration.create();
    CommonFSUtils.setFsDefault(conf, CommonFSUtils.getRootDir(conf));

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
