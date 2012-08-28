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
package org.apache.hadoop.hbase.util.hbck;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.util.HBaseFsck.HbckInfo;
import org.apache.hadoop.hbase.util.HBaseFsck.TableInfo;

/**
 * Simple implementation of TableIntegrityErrorHandler. Can be used as a base
 * class.
 */
abstract public class TableIntegrityErrorHandlerImpl implements
    TableIntegrityErrorHandler {
  TableInfo ti;

  /**
   * {@inheritDoc}
   */
  @Override
  public TableInfo getTableInfo() {
    return ti;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setTableInfo(TableInfo ti2) {
    this.ti = ti2;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void handleRegionStartKeyNotEmpty(HbckInfo hi) throws IOException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void handleRegionEndKeyNotEmpty(byte[] curEndKey) throws IOException {
  }
  
  /**
   * {@inheritDoc}
   */
  @Override
  public void handleDegenerateRegion(HbckInfo hi) throws IOException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void handleDuplicateStartKeys(HbckInfo hi1, HbckInfo hi2)
      throws IOException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void handleOverlapInRegionChain(HbckInfo hi1, HbckInfo hi2)
      throws IOException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void handleHoleInRegionChain(byte[] holeStart, byte[] holeEnd)
      throws IOException {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void handleOverlapGroup(Collection<HbckInfo> overlap)
      throws IOException {
  }

}
