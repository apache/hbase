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
package org.apache.hadoop.hbase.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Tests switching writing to WAL on and off in the Thrift Mutation API */
@Category(LargeTests.class)
public class TestMutationWriteToWAL extends ThriftServerTestBase {

  private static Log LOG = LogFactory.getLog(TestMutationWriteToWAL.class);

  private static final int NUM_ROWS = 10;
  private static final int NUM_COLS_PER_ROW = 25;

  private static final String getRow(int i) {
    return String.format("row%05d", i);
  }

  private static final String getCol(int j) {
    return String.format("col%05d", j);
  }

  private static final String getValue(int i, int j) {
    return "value" + i + "_" + j;
  }

  private boolean shouldDelete(int i, int j) {
    return (i + j) % 2 == 0;
  }

  @Test
  public void testMutationWriteToWAL() throws Exception{
    HBaseTestingUtility.setThreadNameFromMethod();
    final Configuration conf = TEST_UTIL.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    List<String> expectedLogEntries = new ArrayList<String>();

    try {
      Hbase.Client client = createClient();
      client.createTable(HTestConst.DEFAULT_TABLE_BYTE_BUF, HTestConst.DEFAULT_COLUMN_DESC_LIST);
      int expectedEntriesForRow[] = new int[NUM_ROWS];
      for (int i = NUM_ROWS - 1; i >= 0; --i) {
        final String row = getRow(i);
        List<Mutation> mutations = new ArrayList<Mutation>();

        // writeToWAL cannot depend on column, only on the row
        boolean writeToWAL = i % 3 == 0;

        for (int j = 0; j < NUM_COLS_PER_ROW; ++j) {
          final String qual = getCol(j);

          boolean isDelete = shouldDelete(i, j);
          if (!isDelete) {
            expectedEntriesForRow[i]++;
          }

          final String value = isDelete ? "" : getValue(i, j);

          Mutation m = new Mutation(false, ByteBuffer.wrap(Bytes.toBytes(
              HTestConst.DEFAULT_CF_STR + ":" + qual)),
              ByteBuffer.wrap(Bytes.toBytes(value)), writeToWAL, HConstants.LATEST_TIMESTAMP);
          m.isDelete = isDelete;

          mutations.add(m);
          if (writeToWAL) {
            expectedLogEntries.add(row + "," + qual + "," + value + "," + m.isDelete);
          }
        }
        final ByteBuffer rowBuf = ByteBuffer.wrap(Bytes.toBytes(row));
        // Exercise both APIs.
        if (i % 2 == 0) {
          client.mutateRow(HTestConst.DEFAULT_TABLE_BYTE_BUF, rowBuf, mutations, null, null);
        } else {
          List<BatchMutation> rowBatches = new ArrayList<BatchMutation>();
          BatchMutation bm = new BatchMutation(rowBuf, mutations);
          rowBatches.add(bm);
          client.mutateRows(HTestConst.DEFAULT_TABLE_BYTE_BUF, rowBatches, null, null);
        }
      }
      client.disableTable(HTestConst.DEFAULT_TABLE_BYTE_BUF);
      client.enableTable(HTestConst.DEFAULT_TABLE_BYTE_BUF);

      // Check that all the data is there
      for (int i = 0; i < NUM_ROWS; ++i) {
        final String row = getRow(i);
        List<TRowResult> results = client.getRow(HTestConst.DEFAULT_TABLE_BYTE_BUF,
            ByteBuffer.wrap(Bytes.toBytes(row)), null);
        TRowResult result = results.get(0);
        assertEquals("No results found for row " + row, expectedEntriesForRow[i],
            result.getColumnsSize());
        Map<ByteBuffer, TCell> sortedColumns = new TreeMap<ByteBuffer, TCell>(result.getColumns());
        int j = -1;
        for (Map.Entry<ByteBuffer, TCell> entry : sortedColumns.entrySet()) {
          ++j;
          while (shouldDelete(i, j)) {
            ++j;
          }
          assertEquals(HTestConst.DEFAULT_CF_STR + ":" + getCol(j),
              Bytes.toStringBinaryRemaining(entry.getKey()));
          assertEquals(getValue(i, j), Bytes.toStringBinary(entry.getValue().getValue()));
        }
      }
    } finally {
      closeClientSockets();
    }

    TEST_UTIL.shutdownMiniHBaseCluster();

    final Path baseDir = new Path(conf.get(HConstants.HBASE_DIR));
    final Path oldLogDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    int nLogFilesRead = 0;
    List<String> actualLogEntries = new ArrayList<String>();
    ArrayDeque<FileStatus> checkQueue = new ArrayDeque<FileStatus>(
      java.util.Arrays.asList(fs.listStatus(oldLogDir)));
    while (!checkQueue.isEmpty()) {
      FileStatus logFile = checkQueue.pop();
      if (logFile.isDir()) {
        checkQueue.addAll(java.util.Arrays.asList(fs.listStatus(logFile.getPath())));
        continue;
      }
      HLog.Reader r = HLog.getReader(fs, logFile.getPath(), conf);
      LOG.info("Reading HLog: " + logFile.getPath());
      HLog.Entry entry = null;
      while ((entry = r.next(entry)) != null) {
        if (!Bytes.equals(entry.getKey().getTablename(), HTestConst.DEFAULT_TABLE_BYTES)) {
          continue;
        }
        for (KeyValue kv : entry.getEdit().getKeyValues()) {
          if (Bytes.equals(kv.getRow(), HLog.METAROW) || Bytes.equals(kv.getFamily(), HLog.METAFAMILY)) {
            continue;
          }
          actualLogEntries.add(Bytes.toStringBinary(kv.getRow()) + "," +
              Bytes.toStringBinary(kv.getQualifier()) + "," + Bytes.toStringBinary(kv.getValue()) +
              "," + (kv.getType() == KeyValue.Type.DeleteColumn.getCode()));
        }
      }
      r.close();
      ++nLogFilesRead;
    }
    assertTrue(nLogFilesRead > 0);
    Collections.sort(expectedLogEntries);
    Collections.sort(actualLogEntries);
    LOG.info("Expected log entries: " + expectedLogEntries.size() + ", actual log entries: " +
        actualLogEntries.size());
    assertEquals(expectedLogEntries.toString(), actualLogEntries.toString());
  }

}
