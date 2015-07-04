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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

/**
 * Write table content out to map output files.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TsvImporterTextMapper
extends Mapper<LongWritable, Text, ImmutableBytesWritable, Text>
{

  /** Column seperator */
  private String separator;

  /** Should skip bad lines */
  private boolean skipBadLines;
  private Counter badLineCount;
  private boolean logBadLines;

  private ImportTsv.TsvParser parser;

  public boolean getSkipBadLines() {
    return skipBadLines;
  }

  public Counter getBadLineCount() {
    return badLineCount;
  }

  public void incrementBadLineCount(int count) {
    this.badLineCount.increment(count);
  }

  /**
   * Handles initializing this class with objects specific to it (i.e., the parser).
   * Common initialization that might be leveraged by a subsclass is done in
   * <code>doSetup</code>. Hence a subclass may choose to override this method
   * and call <code>doSetup</code> as well before handling it's own custom params.
   *
   * @param context
   */
  @Override
  protected void setup(Context context) {
    doSetup(context);

    Configuration conf = context.getConfiguration();

    parser = new ImportTsv.TsvParser(conf.get(ImportTsv.COLUMNS_CONF_KEY), separator);
    if (parser.getRowKeyColumnIndex() == -1) {
      throw new RuntimeException("No row key column specified");
    }
  }

  /**
   * Handles common parameter initialization that a subclass might want to leverage.
   * @param context
   */
  protected void doSetup(Context context) {
    Configuration conf = context.getConfiguration();

    // If a custom separator has been used,
    // decode it back from Base64 encoding.
    separator = conf.get(ImportTsv.SEPARATOR_CONF_KEY);
    if (separator == null) {
      separator = ImportTsv.DEFAULT_SEPARATOR;
    } else {
      separator = new String(Base64.decode(separator));
    }

    skipBadLines = context.getConfiguration().getBoolean(ImportTsv.SKIP_LINES_CONF_KEY, true);
    logBadLines = context.getConfiguration().getBoolean(ImportTsv.LOG_BAD_LINES_CONF_KEY, false);
    badLineCount = context.getCounter("ImportTsv", "Bad Lines");
  }

  /**
   * Convert a line of TSV text into an HBase table row.
   */
  @Override
  public void map(LongWritable offset, Text value, Context context) throws IOException {
    try {
      Pair<Integer,Integer> rowKeyOffests = parser.parseRowKey(value.getBytes(), value.getLength());
      ImmutableBytesWritable rowKey = new ImmutableBytesWritable(
          value.getBytes(), rowKeyOffests.getFirst(), rowKeyOffests.getSecond());
      context.write(rowKey, value);
    } catch (ImportTsv.TsvParser.BadTsvLineException|IllegalArgumentException badLine) {
      if (logBadLines) {
        System.err.println(value);
      }
      System.err.println("Bad line at offset: " + offset.get() + ":\n" + badLine.getMessage());
      if (skipBadLines) {
        incrementBadLineCount(1);
        return;
      } 
      throw new IOException(badLine);
    } catch (InterruptedException e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
    } 
  }
}
