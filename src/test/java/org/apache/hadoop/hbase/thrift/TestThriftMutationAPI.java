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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.BatchMutation;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.hbase.HTestConst.getRowBytes;
import static org.apache.hadoop.hbase.HTestConst.getCFQualBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestThriftMutationAPI extends ThriftServerTestBase {

  private static final int NUM_ROWS = 3;
  private static final int NUM_COLS = 3;
  private static final int NUM_ITER = 10000;

  private static final int TIMESTAMP_STEP = 10;

  private static final double DELETE_PROB = 0.1;

  /** Enable this when debugging */
  private static final boolean VERBOSE = false;

  private Map<String, Mutation> latestPut = new HashMap<String, Mutation>();
  private Map<String, Long> latestDeleteTS = new HashMap<String, Long>();

  private int numPut = 0;
  private int numDelete = 0;
  private int numMutateRow = 0;
  private int numCheckAndMutate = 0;
  private int numMutateRows = 0;

  private static byte[] getValue(int iRow, int iCol, long ts) {
    return Bytes.toBytes("value_row" + iRow + "_col" + iCol + "_ts" + ts);
  }

  private static String getMutationKey(ByteBuffer row, Mutation m) {
    return Bytes.toStringBinary(row) + "_" + Bytes.toStringBinary(m.column);
  }

  private Mutation getExpectedResult(String mutationKey) {
    Mutation m = latestPut.get(mutationKey);
    if (m == null) {
      return m;
    }
    Long delTS = latestDeleteTS.get(mutationKey);
    if (delTS == null || delTS < m.getTimestamp()) {
      return m;
    }
    return null;
  }

  private void applyMutation(ByteBuffer row, Mutation m, long defaultTS) {
    long ts = m.getTimestamp();
    if (ts == HConstants.LATEST_TIMESTAMP) {
      ts = defaultTS;
    }
    String mutationKey = getMutationKey(row, m);
    if (m.isDelete) {
      if (VERBOSE) {
        System.err.println("Delete: " + mutationKey + ", " + ts);
      }
      Long curDeleteTS = latestDeleteTS.get(mutationKey);
      if (curDeleteTS == null || curDeleteTS < ts) {
        latestDeleteTS.put(mutationKey, ts);
      }
    } else {
      if (VERBOSE) {
        System.err.println("Put: " + mutationKey + ", " + ts);
      }
      Mutation curPut = latestPut.get(mutationKey);
      if (curPut == null || curPut.getTimestamp() < ts) {
        m = new Mutation(m);
        m.setTimestamp(ts);
        latestPut.put(mutationKey, m);
      }
    }
  }

  @Test
  public void testMutations() throws Exception {
    HBaseTestingUtility.setThreadNameFromMethod();
    Hbase.Client client = createClient();
    client.createTable(HTestConst.DEFAULT_TABLE_BYTE_BUF, HTestConst.DEFAULT_COLUMN_DESC_LIST);
    Random rand = new Random(91729817987L);
    List<BatchMutation> bms = new ArrayList<BatchMutation>();
    Set<String> updatedKeys = new HashSet<String>();
    Map<String, TRowResult> rowResults = new HashMap<String, TRowResult>();
    for (int iter = 0; iter < NUM_ITER; ++iter) {
      bms.clear();
      updatedKeys.clear();
      rowResults.clear();
      long baseTS = iter * TIMESTAMP_STEP;
      long defaultTS = baseTS + TIMESTAMP_STEP / 2;

      generateRandomMutations(rand, bms, updatedKeys, baseTS, defaultTS);

      if (bms.size() == 1) {
        doSingleRowMutation(client, rand, bms, defaultTS);
      } else {
        client.mutateRowsTs(HTestConst.DEFAULT_TABLE_BYTE_BUF, bms, defaultTS, null, null);
        numMutateRows++;
      }
      
      for (BatchMutation bm : bms) {
        for (Mutation m : bm.getMutations()) {
          applyMutation(bm.bufferForRow(), m, defaultTS);
        }
      }

      for (String k : updatedKeys) {
        checkRowCol(client, rowResults, k);
      }
    }

    assertTrue(numPut > 0);
    assertTrue(numDelete > 0);
    assertTrue(numMutateRow > 0);
    assertTrue(numCheckAndMutate > 0);
    assertTrue(numMutateRows > 0);
  }

  private void checkRowCol(Hbase.Client client, Map<String, TRowResult> rowResults, String k)
      throws IOError, TException {
    String[] rowCol = k.split("_");
    String row = rowCol[0];
    String col = rowCol[1];
    ByteBuffer rowBuf = ByteBuffer.wrap(Bytes.toBytes(row));
    ByteBuffer colBuf = ByteBuffer.wrap(Bytes.toBytes(col));
    TRowResult rowResult = rowResults.get(row);
    if (rowResult == null) {
      List<TRowResult> result = client.getRow(HTestConst.DEFAULT_TABLE_BYTE_BUF, rowBuf, null);
      if (!result.isEmpty()) {
        assertEquals(1, result.size());
        rowResult = result.get(0);
        rowResults.put(row, rowResult);
      }
    }
    TCell c = null;
    if (rowResult != null) {
      Map<ByteBuffer, TCell> columns = rowResult.getColumns();
      c = columns.get(colBuf);
    }
    Mutation expectedResult = getExpectedResult(k);
    if (expectedResult == null) {
      if (c != null) {
        assertNull("Expecting " + k + " to be absent, but found " +
            Bytes.toStringBinaryRemaining(c.bufferForValue()), c);
      }
    } else {
      assertEquals(Bytes.toString(expectedResult.getValue()),
          Bytes.toString(c.getValue()));
    }
  }

  private void doSingleRowMutation(Hbase.Client client, Random rand, List<BatchMutation> bms,
      long defaultTS) throws IOError, IllegalArgument, TException {
    BatchMutation bm = bms.get(0);
    if (rand.nextBoolean()) {
      // Exercise check-and-mutate operations too. Must have an existing value to check,
      // and all mutations should be of the same type (put/delete).
      int numDeletes = 0;
      for (Mutation m : bm.getMutations()) {
        if (m.isDelete) {
          ++numDeletes;
        }
      }
      if (numDeletes == 0 || numDeletes == bm.getMutations().size()) {
        for (Mutation m : bm.getMutations()) {
          String mutationKey = getMutationKey(bm.bufferForRow(), m);
          Mutation currentValue = getExpectedResult(mutationKey);
          if (currentValue != null) {
            if (VERBOSE) {
              System.err.println("Will do checkAndMutate on " + mutationKey);
            }
            client.checkAndMutateRowTs(HTestConst.DEFAULT_TABLE_BYTE_BUF, bm.bufferForRow(),
                currentValue.bufferForColumn(), currentValue.bufferForValue(),
                bm.getMutations(), defaultTS, null);
            numCheckAndMutate++;
            return;
          }
        }
      }
    }
    
    // We did not check-and-mutate, do a normal operation.
    client.mutateRowTs(HTestConst.DEFAULT_TABLE_BYTE_BUF,
        bm.bufferForRow(), bm.getMutations(), defaultTS, null, null);
    numMutateRow++;
  }

  private void generateRandomMutations(Random rand, List<BatchMutation> bms,
      Set<String> updatedKeys, long baseTS, long defaultTS) {
    while (bms.isEmpty()) {
      for (int iRow = 0; iRow < NUM_ROWS; ++iRow) {
        if (rand.nextBoolean()) {
          BatchMutation bm = new BatchMutation();
          bm.setRow(getRowBytes(iRow));
          bms.add(bm);
          while (bm.mutations == null || bm.mutations.isEmpty()) {
            for (int iCol = 0; iCol < NUM_COLS; ++iCol) {
              if (rand.nextBoolean()) {
                Mutation m = new Mutation();
                m.setIsDelete(rand.nextFloat() < DELETE_PROB);
                if (m.isDelete) {
                  numDelete++;
                } else {
                  numPut++;
                }
                m.setColumn(getCFQualBytes(iCol));
                // Make timestamps correlated with the iteration number but with random
                // shuffling.
                long ts = rand.nextBoolean() ? (baseTS + rand.nextInt(100))
                    : HConstants.LATEST_TIMESTAMP;
                long effectiveTS = ts == HConstants.LATEST_TIMESTAMP ? defaultTS : ts;  
                m.setTimestamp(ts);
                m.setValue(getValue(iRow, iCol, effectiveTS));
                bm.addToMutations(m);
                updatedKeys.add(getMutationKey(bm.row, m));
              }
            }
          }
        }
      }
    }
  }

}
