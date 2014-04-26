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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.util.Bytes;
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
public class HLogPrettyPrinter {
  private boolean outputValues;
  private boolean outputJSON;
  // The following enable filtering by sequence, region, and row, respectively
  private long sequence;
  private String region;
  private String row;
  private String family;
  private String qualifier;
  private long offset;
  private long transactions;

  // enable in order to output a single list of transactions from several files
  private boolean persistentOutput;
  private boolean firstTxn;
  // useful for programmatic capture of JSON output
  private PrintStream out;
  // for JSON encoding
  private ObjectMapper mapper;

  // analysis mode for HLog stats
  private boolean analysisMode = false;

  /**
   * Basic constructor that simply initializes values to reasonable defaults.
   */
  public HLogPrettyPrinter() {
    outputValues = false;
    outputJSON = false;
    sequence = -1;
    offset = 0;
    transactions = Long.MAX_VALUE;
    region = null;
    row = null;
    family = null;
    qualifier = null;
    persistentOutput = false;
    firstTxn = true;
    out = System.out;
    mapper = new ObjectMapper();
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
   * @param offset
   *          starting offset of the hlog file
   * @param transactions
   *          number of transactions to process
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
      long sequence, long offset, long transactions, String region, String row,
      String family, String qualifier, boolean persistentOutput,
      PrintStream out) {
    this.outputValues = outputValues;
    this.outputJSON = outputJSON;
    this.sequence = sequence;
    this.offset = 0;
    this.transactions = transactions;
    this.region = region;
    this.row = row;
    this.family = family;
    this.qualifier = qualifier;
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

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public void setTransactions(long transactions) {
    this.transactions = transactions;
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
   * turn analysis mode on.
   */
  private void setAnalyzeOn() {
    this.analysisMode = true;
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

  public void setColumnFamilyFilter(String family) {
    this.family = family;
  }

  public void setColumnQualifierFilter(String qualifier) {
    this.qualifier = qualifier;
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

    // analysis mode counters and structures
    long numTxns = 0;
    long numKVs = 0;
    long totalKVSize = 0;
    long numCFsUpdated = 0;
    long beginTime = 0;
    long endTime = 0;
    long processedTxns = 0;

    HashMap<String, MutableLong> txnsPerPutSignature = new HashMap<String, MutableLong>();
    HashMap<String, MutableLong> kvCountPerCF = new HashMap<String, MutableLong>();
    HashMap<String, MutableLong> kvSizePerCF = new HashMap<String, MutableLong>();

    Reader log = HLog.getReader(fs, p, conf);
    if (offset > 0) {
      log.seek(offset);
    }
    try {
      HLog.Entry entry;
      while ((entry = log.next()) != null) {
        HLogKey key = entry.getKey();
        WALEdit edit = entry.getEdit();

        numTxns++;

        // begin building a transaction structure
        Map<String, Object> txn = key.toStringMap();

        if (analysisMode) {
          // track the time of the first transaction
          // and the last transaction as a way to get
          // the total time for all transactions in
          // this HLog.
          if (numTxns == 1) {
            beginTime = (Long)(txn.get("writeTime"));
          }
          endTime = (Long)(txn.get("writeTime"));
        }

        // check output filters

        if (sequence >= 0 && ((Long) txn.get("sequence")) != sequence)
          continue;

        if (region != null && !((String) txn.get("region")).equals(region))
          continue;

        String table = (String)(txn.get("table"));

        // initialize list into which we will store atomic actions
        List<Map> actions = new ArrayList<Map>();
        for (KeyValue kv : edit.getKeyValues()) {
          // add atomic operation to txn
          Map<String, Object> op =
            new HashMap<String, Object>(kv.toStringMap());
          if (outputValues)
            op.put("value", Bytes.toStringBinary(kv.getValue()));
          // check row output filter
          if ((row == null || ((String) op.get("row")).equals(row)) &&
               (family == null || ((String) op.get("family")).startsWith(family))
              && (qualifier == null || ((String) op.get("qualifier"))
                  .startsWith(qualifier))) {
            actions.add(op);

            if (analysisMode) {
              // track total number of KVs and their size
              numKVs++;
              totalKVSize += kv.getLength();

              // track number of KVs per CF
              String table_and_cf = table + ":" + op.get("family");
              MutableLong count = kvCountPerCF.get(table_and_cf);
              if (count != null) {
                count.increment();
              } else {
                kvCountPerCF.put(table_and_cf, new MutableLong(1));
              }

              // track size of KVs writter per CF
              MutableLong kvSize = kvSizePerCF.get(table_and_cf);
              if (kvSize != null) {
                kvSize.add(kv.getLength());
              } else {
                kvSizePerCF.put(table_and_cf, new MutableLong(kv.getLength()));
              }
            }
          }
        }

        if (actions.isEmpty())
          continue;
        txn.put("actions", actions);
        if (outputJSON) {
          // JSON output is a straightforward "toString" on the txn object
          if (firstTxn)
            firstTxn = false;
          else
            out.print(",");
          // encode and print JSON
          out.print(mapper.writeValueAsString(txn));
        } else {
          if (analysisMode) {
            // track how many CFs each txn touches
            TreeSet<String> cfs = new TreeSet<String>();
            for (int i = 0; i < actions.size(); i++) {
              // track unique CFs in this txn in a set
              String cf = (String)(actions.get(i).get("family"));
              cfs.add(cf);
            }
            numCFsUpdated += cfs.size();
            String putSignature = table + ":" + cfs.toString();
            MutableLong count = txnsPerPutSignature.get(putSignature);
            if (count != null) {
              count.increment();
            } else {
              txnsPerPutSignature.put(putSignature, new MutableLong(1));
            }

          } else {
            // Pretty output, complete with indentation by atomic action
            out.println("Sequence " + txn.get("sequence") + " "
                + "from region " + txn.get("region") + " " + "in table "
                + table);
            for (int i = 0; i < actions.size(); i++) {
              Map op = actions.get(i);
              out.println("  Action:");
              out.println("    row: " + op.get("row"));
              out.println("    column: " + op.get("family") + ":"
                  + op.get("qualifier"));
              Long ts = (Long)op.get("timestamp");
              out.println("    at time: "
                  + (new Date(ts)) + " (" + ts + ")");
              if (outputValues)
                out.println("    value: " + op.get("value"));
            }
          }
        }

        processedTxns++;
        if (processedTxns >= transactions) {
          break;
        }
      }

      if (analysisMode && (numTxns > 0) && (numKVs > 0)) {

        out.println("=== SUMMARY ===");
        out.println("Number of txns: " + numTxns);
        out.println("Number of KVs: " + numKVs);
        out.println("Total Size of KVs: " + totalKVSize);
        out.println("Avg number of KVs/txn: " + numKVs/numTxns);
        out.println("Avg size of txn: " + totalKVSize/numTxns);
        out.println("Avg size of KV: " + totalKVSize/numKVs);
        out.println("Avg CFs written to per txn: " + numCFsUpdated/numTxns);

        if (endTime > beginTime) {
          out.println("Txns/sec: " + (numTxns * 1000 / (endTime - beginTime)));
          out.println(" KVs/sec: " + (numKVs * 1000 / (endTime - beginTime)));
        }

        out.println("======= CF Level Stats ======");
        out.format("%12s|%s\n", "# of KVs", "CF Name");
        for (Map.Entry<String, MutableLong> e : sortDescByValue(kvCountPerCF)) {
          String key = e.getKey();
          long   kvCount = e.getValue().toLong();
          out.format("%12d %s\n", kvCount, key);
        }
        out.println("=====");
        out.format("%12s|%s\n", "Total KVSize", "CF Name");
        for (Map.Entry<String, MutableLong> e : sortDescByValue(kvSizePerCF)) {
          String key = e.getKey();
          long   kvSize = e.getValue().toLong();
          out.format("%12d %s\n", kvSize, key);
        }
        out.println("=====");
        out.format("%12s|%s\n", "== TxnCount", "Put/Delete's CF Signature ==");
        for (Map.Entry<String, MutableLong> e : sortDescByValue(txnsPerPutSignature)) {
          String putSignature = e.getKey();
          long txnCount = e.getValue().toLong();
          out.format("%12d %s\n", txnCount, putSignature);
        }
        out.println("===========================");

      }

    } finally {
      log.close();
    }
    if (outputJSON && !persistentOutput) {
      out.print("]");
    }
  }

  // helper function to do descending sort by value of entries in the map
  private List<Map.Entry<String, MutableLong>> sortDescByValue(Map<String, MutableLong> map) {
    List<Map.Entry<String, MutableLong>> entrySet =
      new ArrayList<Map.Entry<String, MutableLong>>(map.entrySet());

    Collections.sort(entrySet,
          new Comparator<Map.Entry<String, MutableLong>>() {
            @Override
            public int compare(Map.Entry<String, MutableLong> o1,
                               Map.Entry<String, MutableLong> o2) {
              return o2.getValue().compareTo(o1.getValue());
            }
          }
    );

    return entrySet;
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
        "Region to filter by. Pass region name; e.g. '.META.,,1'");
    options.addOption("s", "sequence", true,
        "Sequence to filter by. Pass sequence number.");
    options.addOption("w", "row", true, "Row to filter by. Pass row name.");
    options.addOption("f", "family", true,
        "Column family prefix. Pass column family prefix.");
    options.addOption("q", "qualifier", true,
        "Column qualifier prefix. Pass column qualifier prefix.");
    options.addOption("a", "analyze", false, "analyze the log");
    options.addOption("o", "offset", true, "starting offset of the log");
    options.addOption("t", "transactions", true,
        "number of transactions to analyze");

    HLogPrettyPrinter printer = new HLogPrettyPrinter();
    CommandLineParser parser = new PosixParser();
    List files = null;
    try {
      CommandLine cmd = parser.parse(options, args);
      files = cmd.getArgList();
      if (files.isEmpty() || cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("HLog filename(s) ", options, true);
        System.exit(-1);
      }
      // configure the pretty printer using command line options
      if (cmd.hasOption("p"))
        printer.enableValues();
      if (cmd.hasOption("j"))
        printer.enableJSON();
      if (cmd.hasOption("r"))
        printer.setRegionFilter(cmd.getOptionValue("r"));
      if (cmd.hasOption("f"))
        printer.setColumnFamilyFilter(cmd.getOptionValue("f"));
      if (cmd.hasOption("q"))
        printer.setColumnQualifierFilter(cmd.getOptionValue("q"));
      if (cmd.hasOption("s"))
        printer.setSequenceFilter(Long.parseLong(cmd.getOptionValue("s")));
      if (cmd.hasOption("w"))
        printer.setRowFilter(cmd.getOptionValue("w"));
      if (cmd.hasOption("a"))
        printer.setAnalyzeOn();
      if (cmd.hasOption("o")) {
        printer.setOffset(Long.parseLong(cmd.getOptionValue("o")));
      }
      if (cmd.hasOption("t")) {
        printer.setTransactions(Long.parseLong(cmd.getOptionValue("t")));
      }
    } catch (ParseException e) {
      e.printStackTrace();
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("HLog filename(s) ", options, true);
      System.exit(-1);
    }
    // get configuration, file system, and process the given files
    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.defaultFS",
        conf.get(org.apache.hadoop.hbase.HConstants.HBASE_DIR));
    conf.set("fs.default.name",
        conf.get(org.apache.hadoop.hbase.HConstants.HBASE_DIR));
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
