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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.security.visibility.InvalidLabelException;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

/**
 * Emits Sorted KeyValues. Parse the passed text and creates KeyValues. Sorts them before emit.
 * @see HFileOutputFormat
 * @see KeyValueSortReducer
 * @see PutSortReducer
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TextSortReducer extends
    Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, KeyValue> {
  
  /** Timestamp for all inserted rows */
  private long ts;

  /** Column seperator */
  private String separator;

  /** Should skip bad lines */
  private boolean skipBadLines;
  
  private Counter badLineCount;

  private ImportTsv.TsvParser parser;

  /** Cell visibility expr **/
  private String cellVisibilityExpr;

  /** Cell TTL */
  private long ttl;

  private CellCreator kvCreator;

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

    Configuration conf = context.getConfiguration();

    parser = new ImportTsv.TsvParser(conf.get(ImportTsv.COLUMNS_CONF_KEY), separator);
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

    // Should never get 0 as we are setting this to a valid value in job configuration.
    ts = conf.getLong(ImportTsv.TIMESTAMP_CONF_KEY, 0);

    skipBadLines = context.getConfiguration().getBoolean(ImportTsv.SKIP_LINES_CONF_KEY, true);
    badLineCount = context.getCounter("ImportTsv", "Bad Lines");
  }
  
  @Override
  protected void reduce(
      ImmutableBytesWritable rowKey,
      java.lang.Iterable<Text> lines,
      Reducer<ImmutableBytesWritable, Text,
              ImmutableBytesWritable, KeyValue>.Context context)
      throws java.io.IOException, InterruptedException
  {
    // although reduce() is called per-row, handle pathological case
    long threshold = context.getConfiguration().getLong(
        "reducer.row.threshold", 1L * (1<<30));
    Iterator<Text> iter = lines.iterator();
    while (iter.hasNext()) {
      Set<KeyValue> kvs = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
      long curSize = 0;
      // stop at the end or the RAM threshold
      while (iter.hasNext() && curSize < threshold) {
        Text line = iter.next();
        byte[] lineBytes = line.getBytes();
        try {
          ImportTsv.TsvParser.ParsedLine parsed = parser.parse(lineBytes, line.getLength());
          // Retrieve timestamp if exists
          ts = parsed.getTimestamp(ts);
          cellVisibilityExpr = parsed.getCellVisibility();
          ttl = parsed.getCellTTL();

          for (int i = 0; i < parsed.getColumnCount(); i++) {
            if (i == parser.getRowKeyColumnIndex() || i == parser.getTimestampKeyColumnIndex()
                || i == parser.getAttributesKeyColumnIndex() || i == parser.getCellVisibilityColumnIndex()
                || i == parser.getCellTTLColumnIndex()) {
              continue;
            }
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
            Cell cell = this.kvCreator.create(lineBytes, parsed.getRowKeyOffset(),
                parsed.getRowKeyLength(), parser.getFamily(i), 0, parser.getFamily(i).length,
                parser.getQualifier(i), 0, parser.getQualifier(i).length, ts, lineBytes,
                parsed.getColumnOffset(i), parsed.getColumnLength(i), tags);
            KeyValue kv = KeyValueUtil.ensureKeyValueTypeForMR(cell);
            kvs.add(kv);
            curSize += kv.heapSize();
          }
        } catch (ImportTsv.TsvParser.BadTsvLineException | IllegalArgumentException
            | InvalidLabelException badLine) {
          if (skipBadLines) {
            System.err.println("Bad line." + badLine.getMessage());
            incrementBadLineCount(1);
            continue;
          }
          throw new IOException(badLine);
        }
      }
      context.setStatus("Read " + kvs.size() + " entries of " + kvs.getClass()
          + "(" + StringUtils.humanReadableInt(curSize) + ")");
      int index = 0;
      for (KeyValue kv : kvs) {
        context.write(rowKey, kv);
        if (++index > 0 && index % 100 == 0)
          context.setStatus("Wrote " + index + " key values.");
      }

      // if we have more entries to process
      if (iter.hasNext()) {
        // force flush because we cannot guarantee intra-row sorted order
        context.write(null, null);
      }
    }
  }
}
