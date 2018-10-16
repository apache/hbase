/**
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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for testing WALObserver coprocessor. It will monitor WAL writing and restoring, and modify
 * passed-in WALEdit, i.e, ignore specified columns when writing, or add a KeyValue. On the other
 * side, it checks whether the ignored column is still in WAL when Restoreed at region reconstruct.
 */
public class SampleRegionWALCoprocessor implements WALCoprocessor, RegionCoprocessor,
    WALObserver, RegionObserver {

  private static final Logger LOG = LoggerFactory.getLogger(SampleRegionWALCoprocessor.class);

  private byte[] tableName;
  private byte[] row;
  private byte[] ignoredFamily;
  private byte[] ignoredQualifier;
  private byte[] addedFamily;
  private byte[] addedQualifier;
  private byte[] changedFamily;
  private byte[] changedQualifier;

  private boolean preWALWriteCalled = false;
  private boolean postWALWriteCalled = false;
  private boolean preWALRestoreCalled = false;
  private boolean postWALRestoreCalled = false;
  private boolean preWALRollCalled = false;
  private boolean postWALRollCalled = false;

  /**
   * Set values: with a table name, a column name which will be ignored, and
   * a column name which will be added to WAL.
   */
  public void setTestValues(byte[] tableName, byte[] row, byte[] igf, byte[] igq,
      byte[] chf, byte[] chq, byte[] addf, byte[] addq) {
    this.row = row;
    this.tableName = tableName;
    this.ignoredFamily = igf;
    this.ignoredQualifier = igq;
    this.addedFamily = addf;
    this.addedQualifier = addq;
    this.changedFamily = chf;
    this.changedQualifier = chq;
    preWALWriteCalled = false;
    postWALWriteCalled = false;
    preWALRestoreCalled = false;
    postWALRestoreCalled = false;
    preWALRollCalled = false;
    postWALRollCalled = false;
  }

  @Override public Optional<WALObserver> getWALObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public void postWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> env,
      RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    postWALWriteCalled = true;
  }

  @Override
  public void preWALWrite(ObserverContext<? extends WALCoprocessorEnvironment> env,
      RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    // check table name matches or not.
    if (!Bytes.equals(info.getTable().toBytes(), this.tableName)) {
      return;
    }
    preWALWriteCalled = true;
    // here we're going to remove one keyvalue from the WALEdit, and add
    // another one to it.
    List<Cell> cells = logEdit.getCells();
    Cell deletedCell = null;
    for (Cell cell : cells) {
      // assume only one kv from the WALEdit matches.
      byte[] family = CellUtil.cloneFamily(cell);
      byte[] qulifier = CellUtil.cloneQualifier(cell);

      if (Arrays.equals(family, ignoredFamily) &&
          Arrays.equals(qulifier, ignoredQualifier)) {
        LOG.debug("Found the KeyValue from WALEdit which should be ignored.");
        deletedCell = cell;
      }
      if (Arrays.equals(family, changedFamily) &&
          Arrays.equals(qulifier, changedQualifier)) {
        LOG.debug("Found the KeyValue from WALEdit which should be changed.");
        cell.getValueArray()[cell.getValueOffset()] =
            (byte) (cell.getValueArray()[cell.getValueOffset()] + 1);
      }
    }
    if (null != row) {
      cells.add(new KeyValue(row, addedFamily, addedQualifier));
    }
    if (deletedCell != null) {
      LOG.debug("About to delete a KeyValue from WALEdit.");
      cells.remove(deletedCell);
    }
  }

  /**
   * Triggered before  {@link org.apache.hadoop.hbase.regionserver.HRegion} when WAL is
   * Restoreed.
   */
  @Override
  public void preWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env,
    RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    preWALRestoreCalled = true;
  }

  @Override
  public void preWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      Path oldPath, Path newPath) throws IOException {
    preWALRollCalled = true;
  }

  @Override
  public void postWALRoll(ObserverContext<? extends WALCoprocessorEnvironment> ctx,
      Path oldPath, Path newPath) throws IOException {
    postWALRollCalled = true;
  }

  /**
   * Triggered after {@link org.apache.hadoop.hbase.regionserver.HRegion} when WAL is
   * Restoreed.
   */
  @Override
  public void postWALRestore(ObserverContext<? extends RegionCoprocessorEnvironment> env,
      RegionInfo info, WALKey logKey, WALEdit logEdit) throws IOException {
    postWALRestoreCalled = true;
  }

  public boolean isPreWALWriteCalled() {
    return preWALWriteCalled;
  }

  public boolean isPostWALWriteCalled() {
    return postWALWriteCalled;
  }

  public boolean isPreWALRestoreCalled() {
    LOG.debug(SampleRegionWALCoprocessor.class.getName() +
      ".isPreWALRestoreCalled is called.");
    return preWALRestoreCalled;
  }

  public boolean isPostWALRestoreCalled() {
    LOG.debug(SampleRegionWALCoprocessor.class.getName() +
      ".isPostWALRestoreCalled is called.");
    return postWALRestoreCalled;
  }

  public boolean isPreWALRollCalled() {
    return preWALRollCalled;
  }

  public boolean isPostWALRollCalled() {
    return postWALRollCalled;
  }
}
