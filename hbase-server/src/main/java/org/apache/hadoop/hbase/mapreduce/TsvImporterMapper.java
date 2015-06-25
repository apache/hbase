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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ImportTsv.TsvParser.BadTsvLineException;
import org.apache.hadoop.hbase.security.visibility.CellVisibility;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Write table content out to files in hdfs.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class TsvImporterMapper
extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>
{

  /** Timestamp for all inserted rows */
  protected long ts;

  /** Column seperator */
  private String separator;

  /** Should skip bad lines */
  private boolean skipBadLines;
  private Counter badLineCount;
  private boolean logBadLines;

  protected ImportTsv.TsvParser parser;

  protected Configuration conf;

  protected String cellVisibilityExpr;

  protected long ttl;

  protected CellCreator kvCreator;

  private String hfileOutPath;

  public long getTs() {
    return ts;
  }

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

    conf = context.getConfiguration();
    parser = new ImportTsv.TsvParser(conf.get(ImportTsv.COLUMNS_CONF_KEY),
                           separator);
    if (parser.getRowKeyColumnIndex() == -1) {
      throw new RuntimeException("No row key column specified");
    }
    this.kvCreator = new CellCreator(conf);
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
    // Should never get 0 as we are setting this to a valid value in job
    // configuration.
    ts = conf.getLong(ImportTsv.TIMESTAMP_CONF_KEY, 0);

    skipBadLines = context.getConfiguration().getBoolean(
        ImportTsv.SKIP_LINES_CONF_KEY, true);
    badLineCount = context.getCounter("ImportTsv", "Bad Lines");
    logBadLines = context.getConfiguration().getBoolean(ImportTsv.LOG_BAD_LINES_CONF_KEY, false);
    hfileOutPath = conf.get(ImportTsv.BULK_OUTPUT_CONF_KEY);
  }

  /**
   * Convert a line of TSV text into an HBase table row.
   */
  @Override
  public void map(LongWritable offset, Text value,
    Context context)
  throws IOException {
    byte[] lineBytes = value.getBytes();

    try {
      ImportTsv.TsvParser.ParsedLine parsed = parser.parse(
          lineBytes, value.getLength());
      ImmutableBytesWritable rowKey =
        new ImmutableBytesWritable(lineBytes,
            parsed.getRowKeyOffset(),
            parsed.getRowKeyLength());
      // Retrieve timestamp if exists
      ts = parsed.getTimestamp(ts);
      cellVisibilityExpr = parsed.getCellVisibility();
      ttl = parsed.getCellTTL();

      Put put = new Put(rowKey.copyBytes());
      for (int i = 0; i < parsed.getColumnCount(); i++) {
        if (i == parser.getRowKeyColumnIndex() || i == parser.getTimestampKeyColumnIndex()
            || i == parser.getAttributesKeyColumnIndex() || i == parser.getCellVisibilityColumnIndex()
            || i == parser.getCellTTLColumnIndex()) {
          continue;
        }
        populatePut(lineBytes, parsed, put, i);
      }
      context.write(rowKey, put);
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
    }
  }

  protected void populatePut(byte[] lineBytes, ImportTsv.TsvParser.ParsedLine parsed, Put put,
      int i) throws BadTsvLineException, IOException {
    Cell cell = null;
    if (hfileOutPath == null) {
      cell = new KeyValue(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
          parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0,
          parser.getQualifier(i).length, ts, KeyValue.Type.Put, lineBytes,
          parsed.getColumnOffset(i), parsed.getColumnLength(i));
      if (cellVisibilityExpr != null) {
        // We won't be validating the expression here. The Visibility CP will do
        // the validation
        put.setCellVisibility(new CellVisibility(cellVisibilityExpr));
      }
      if (ttl > 0) {
        put.setTTL(ttl);
      }
    } else {
      // Creating the KV which needs to be directly written to HFiles. Using the Facade
      // KVCreator for creation of kvs.
      List<Tag> tags = new ArrayList<Tag>();
      if (cellVisibilityExpr != null) {
        tags.addAll(kvCreator.getVisibilityExpressionResolver()
          .createVisibilityExpTags(cellVisibilityExpr));
      }
      // Add TTL directly to the KV so we can vary them when packing more than one KV
      // into puts
      if (ttl > 0) {
        tags.add(new Tag(TagType.TTL_TAG_TYPE, Bytes.toBytes(ttl)));
      }
      cell = this.kvCreator.create(lineBytes, parsed.getRowKeyOffset(), parsed.getRowKeyLength(),
          parser.getFamily(i), 0, parser.getFamily(i).length, parser.getQualifier(i), 0,
          parser.getQualifier(i).length, ts, lineBytes, parsed.getColumnOffset(i),
          parsed.getColumnLength(i), tags);
    }
    put.add(cell);
  }
}
