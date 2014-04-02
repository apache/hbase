/**
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

package org.apache.hadoop.hbase.mock;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;

public class HTableMockImpl implements HTableInterface {

  @Override
  public byte[] getTableName() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Configuration getConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HTableDescriptor getTableDescriptor() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean exists(Get get) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Result get(Get get) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Result[] get(List<Get> gets) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultScanner getScanner(Scan scan) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultScanner getScanner(byte[] family) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultScanner getScanner(byte[] family, byte[] qualifier)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void put(Put put) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void put(List<Put> puts) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Put put) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void delete(Delete delete) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      byte[] value, Delete delete) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier,
      long amount, boolean writeToWAL) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void mutateRow(RowMutations arm) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void mutateRow(List<RowMutations> armList) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isAutoFlush() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void flushCommits() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public RowLock lockRow(byte[] row) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void unlockRow(RowLock rl) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setProfiling(boolean prof) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean getProfiling() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setTag(String tag) {
    // TODO Auto-generated method stub

  }

  @Override
  public String getTag() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Result[] batchGet(List<Get> actions) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void batchMutate(List<Mutation> actions) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public Collection<HRegionLocation> getCachedHRegionLocations(
      boolean forceRefresh) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }
}
