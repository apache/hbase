/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor.example;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This RegionObserver replaces the values of Puts from one value to another on compaction.
 */
@InterfaceAudience.Private
public class ValueRewritingObserver implements RegionObserver, RegionCoprocessor {
  public static final String ORIGINAL_VALUE_KEY =
      "hbase.examples.coprocessor.value.rewrite.orig";
  public static final String REPLACED_VALUE_KEY =
      "hbase.examples.coprocessor.value.rewrite.replaced";

  private byte[] sourceValue = null;
  private byte[] replacedValue = null;
  private Bytes.ByteArrayComparator comparator;
  private CellBuilder cellBuilder;


  @Override
  public Optional<RegionObserver> getRegionObserver() {
    // Extremely important to be sure that the coprocessor is invoked as a RegionObserver
    return Optional.of(this);
  }

  @Override
  public void start(
      @SuppressWarnings("rawtypes") CoprocessorEnvironment env) throws IOException {
    RegionCoprocessorEnvironment renv = (RegionCoprocessorEnvironment) env;
    sourceValue = Bytes.toBytes(renv.getConfiguration().get(ORIGINAL_VALUE_KEY));
    replacedValue = Bytes.toBytes(renv.getConfiguration().get(REPLACED_VALUE_KEY));
    comparator = new Bytes.ByteArrayComparator();
    cellBuilder = CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY);
  }

  @Override
  public InternalScanner preCompact(
      ObserverContext<RegionCoprocessorEnvironment> c, Store store,
      final InternalScanner scanner, ScanType scanType, CompactionLifeCycleTracker tracker,
      CompactionRequest request) {
    InternalScanner modifyingScanner = new InternalScanner() {
      @Override
      public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
        boolean ret = scanner.next(result, scannerContext);
        for (int i = 0; i < result.size(); i++) {
          Cell c = result.get(i);
          // Replace the Cell if the value is the one we're replacing
          if (CellUtil.isPut(c) &&
              comparator.compare(CellUtil.cloneValue(c), sourceValue) == 0) {
            try {
              cellBuilder.setRow(CellUtil.copyRow(c));
              cellBuilder.setFamily(CellUtil.cloneFamily(c));
              cellBuilder.setQualifier(CellUtil.cloneQualifier(c));
              cellBuilder.setTimestamp(c.getTimestamp());
              cellBuilder.setType(Cell.Type.Put);
              // Make sure each cell gets a unique value
              byte[] clonedValue = new byte[replacedValue.length];
              System.arraycopy(replacedValue, 0, clonedValue, 0, replacedValue.length);
              cellBuilder.setValue(clonedValue);
              result.set(i, cellBuilder.build());
            } finally {
              cellBuilder.clear();
            }
          }
        }
        return ret;
      }

      @Override
      public void close() throws IOException {
        scanner.close();
      }
    };

    return modifyingScanner;
  }
}
