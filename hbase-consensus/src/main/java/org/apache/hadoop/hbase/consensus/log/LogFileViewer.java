package org.apache.hadoop.hbase.consensus.log;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.commons.cli.*;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.MemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class LogFileViewer {
  private static final Logger
    LOG = LoggerFactory.getLogger(LogFileViewer.class);

  /**
   * @param args
   * @throws ParseException
   */
  public static void main(String[] args) throws ParseException, IOException {

    Options options = new Options();

    options.addOption("f", "filepath", true,
      "location of the file.");
    options.addOption("d", "detail", true,
      "Dump a detailed information about the transactions in the file");

    if (args.length == 0) {
      printHelp(options);
      return;
    }

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, args);

    boolean detailed = false;
    String filePath = null;

    if (!cmd.hasOption("f")) {
      printHelp(options);
      return;
    }

    filePath = cmd.getOptionValue("f");

    if (cmd.hasOption("d")) {
      detailed = true;
    }

    final File logFile = new File(filePath);

    if (!logFile.exists()) {
      LOG.error("The specified file " + filePath + " does not exists.");
      return ;
    }

    dumpFileInfo(logFile, detailed);
  }

  public static void printHelp(final Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("LogFileViewer", options, true);
  }

  public static void dumpFileInfo(final File logFile, boolean detailed)
    throws IOException {
    long index;
    LogReader reader = new LogReader(logFile);
    reader.initialize();

    index = reader.getInitialIndex();

    LOG.info("File Name: " + reader.getFile().getName());
    LOG.info("Term: " + reader.getCurrentTerm());
    LOG.info("Initial Index: " + index);

    MemoryBuffer buffer;

    try {
      while (true) {
        buffer = reader.seekAndRead(index, null);
        if (detailed) {
          LOG.info("(index:" + index + ", offset:" +
            reader.getCurrentIndexFileOffset() + ", ");
          try {
            List<WALEdit> edits = WALEdit.deserializeFromByteBuffer(
              buffer.getBuffer());
            LOG.info("Size: " + buffer.getBuffer().limit() +
              ", Number of edits : " + edits.size());
          } catch (Exception e) {
            LOG.info("(" + index + ":" + buffer.getBuffer().position() +
              ")");
          }
          LOG.info("),");
        }
        buffer.getBuffer().clear();
        ++index;
      }
    } catch (IOException e) {
      LOG.error("Last Readable Index: " + (--index), e);
    }

    reader.close();
  }

}
