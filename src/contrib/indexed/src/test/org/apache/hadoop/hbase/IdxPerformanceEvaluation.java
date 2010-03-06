/*
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxQualifierType;
import org.apache.hadoop.hbase.client.idx.IdxScan;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.apache.hadoop.hbase.regionserver.IdxRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * This class provides the ability to do a side-by-side comparison with the
 * {@link PerformanceEvaluation}.  It demonstrates the performance gains and
 * impacts when using the IdxRegion with an indexed table.
 * <p/>
 * <p>It's important to note that the table schema used by the PerformanceEvaluation
 * does not reflect the use case that the IdxRegion was aiming to solve.
 * Basically, the index impl. hasn't been written with an index on millions of
 * unique 1KB values in mind.  The index has to store each unique value in
 * memory and as a result the performance evaluation requires an unreasonable
 * amount of memory to complete.  Also, the cost of re-building the index on
 * split in a single node cluster is very high which can cause timeout issues on
 * the client side, especially during the sequentialWrite test.
 * <p/>
 * <p>This evaluation creates an index on the first two bytes to the value.  This
 * still provides a big performance boost without requiring huge amounts of memory.
 * </p>
 */
public class IdxPerformanceEvaluation extends PerformanceEvaluation {
  protected static final Log LOG = LogFactory.getLog(IdxPerformanceEvaluation.class);

  private static final byte[] TABLE_NAME = Bytes.toBytes("IdxPerformanceEvaluation");
  private static final HTableDescriptor TABLE_DESCRIPTOR;

  static {
    TABLE_DESCRIPTOR = new HTableDescriptor(TABLE_NAME);
    IdxColumnDescriptor idxColumnDescriptor = new IdxColumnDescriptor(FAMILY_NAME);
    try {
      idxColumnDescriptor.addIndexDescriptor(
        new IdxIndexDescriptor(QUALIFIER_NAME, IdxQualifierType.BYTE_ARRAY, 0, 2)
      );
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    TABLE_DESCRIPTOR.addFamily(idxColumnDescriptor);
  }

  @Override
  protected HTableDescriptor getTableDescriptor() {
    return TABLE_DESCRIPTOR;
  }

  /**
   * Constructor
   *
   * @param c Configuration object
   */
  public IdxPerformanceEvaluation(final HBaseConfiguration c) {
    super(c);

    c.set(HConstants.REGION_IMPL, IdxRegion.class.getName());
    // sequential writes really slow down region splits, increasing the retry
    // count prevents the client from giving up when this occurs
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER_KEY, 50);

    addCommandDescriptor(
      IndexedFilteredScanTest.class,
      "idxFilterScan",
      "The same as 'filterScan' but takes advantage of an index on the value"
    );
  }

  @Override
  protected void printUsage(String message) {
    System.err.println("");
    System.err.println(
      "NOTE: In order to run this evaluation you need to ensure you have \n" +
        "enabled the IdxRegion in your hbase-site.xml."
    );
    System.err.println("");
    super.printUsage(message);
  }

  static class IndexedFilteredScanTest extends FilteredScanTest {
    IndexedFilteredScanTest(HBaseConfiguration conf, TestOptions options, Status status) {
      super(conf, options, status);
    }

    @Override
    protected Scan constructScan(byte[] valuePrefix) throws IOException {
      return new IdxScan(
        super.constructScan(valuePrefix),
        new Comparison(FAMILY_NAME, QUALIFIER_NAME, Comparison.Operator.EQ, valuePrefix)
      );
    }
  }

  /**
   * @param args
   */
  public static void main(final String[] args) {
    HBaseConfiguration c = new HBaseConfiguration();
    System.exit(new IdxPerformanceEvaluation(c).doCommandLine(args));
  }
}
