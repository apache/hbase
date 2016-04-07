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
package org.apache.hadoop.hbase.procedure2.store.wal;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALEntry;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALHeader;

/**
 * ProcedureWALPrettyPrinter prints the contents of a given ProcedureWAL file
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class ProcedureWALPrettyPrinter {
  private PrintStream out;

  public ProcedureWALPrettyPrinter() {
    out = System.out;
  }

  /**
   * Reads a log file and outputs its contents.
   *
   * @param conf   HBase configuration relevant to this log file
   * @param p       path of the log file to be read
   * @throws IOException  IOException
   */
  public void processFile(final Configuration conf, final Path p)
      throws IOException {

    FileSystem fs = p.getFileSystem(conf);
    if (!fs.exists(p)) {
      System.err.println("ERROR, file doesnt exist: " + p);
      return;
    }
    if (!fs.isFile(p)) {
      System.err.println(p + " is not a file");
      return;
    }

    FileStatus logFile = fs.getFileStatus(p);
    if (logFile.getLen() == 0) {
      out.println("Zero length file: " + p);
      return;
    }

    out.println("Opening procedure state-log: " + p);
    ProcedureWALFile log = new ProcedureWALFile(fs, logFile);
    processProcedureWALFile(log);
  }

  public void processProcedureWALFile(ProcedureWALFile log) throws IOException {

    log.open();
    ProcedureWALHeader header = log.getHeader();
    printHeader(header);

    FSDataInputStream stream = log.getStream();
    try {
      boolean hasMore = true;
      while (hasMore) {
        ProcedureWALEntry entry = ProcedureWALFormat.readEntry(stream);
        if (entry == null) {
          out.print("No more entry, exiting with missing EOF");
          hasMore = false;
          break;
        }
        switch (entry.getType()) {
          case PROCEDURE_WAL_EOF:
            hasMore = false;
            break;
          default:
            printEntry(entry);
        }
      }
    } catch (IOException e) {
      out.print("got an exception while reading the procedure WAL " + e.getMessage());
    }
    finally {
      log.close();
    }
  }

  private void printEntry(ProcedureWALEntry entry) throws IOException {
    out.println("EntryType=" + entry.getType());
    int procCount = entry.getProcedureCount();
    for (int i = 0; i < procCount; i++) {
      Procedure<?> proc = Procedure.convert(entry.getProcedure(i));
      printProcedure(proc);
    }
  }

  private void printProcedure(Procedure<?> proc) {
    out.println(proc.toStringDetails());
  }

  private void printHeader(ProcedureWALHeader header) {
    out.println("ProcedureWALHeader: ");
    out.println("  Version: " + header.getVersion());
    out.println("  Type: " + header.getType());
    out.println("  LogId: " + header.getLogId());
    out.println("  MinProcId: " + header.getMinProcId());
    out.println();
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
    options.addOption("f", "file", true, "File to print");

    List<Path> files = new ArrayList<Path>();

    ProcedureWALPrettyPrinter printer = new ProcedureWALPrettyPrinter();
    CommandLineParser parser = new PosixParser();
    try {
      CommandLine cmd = parser.parse(options, args);

      if (cmd.hasOption("f")) {
        files.add(new Path(cmd.getOptionValue("f")));
      }

      if (files.size() == 0 || cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ProcedureWALPrettyPrinter ", options, true);
        System.exit(-1);
      }

    } catch (ParseException e) {
      e.printStackTrace();
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("ProcedureWALPrettyPrinter ", options, true);
      System.exit(-1);
    }
    // get configuration, file system, and process the given files
    Configuration conf = HBaseConfiguration.create();
    for (Path file : files) {
      printer.processFile(conf, file);
    }
  }
}
