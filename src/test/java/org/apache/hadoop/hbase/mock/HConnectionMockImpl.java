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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HRegionFailureInfo;
import org.apache.hadoop.hbase.client.MultiPut;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationContext;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.ipc.HBaseRPCOptions;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.util.StringBytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

public class HConnectionMockImpl implements HConnection {

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public ZooKeeperWrapper getZooKeeperWrapper() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HMasterInterface getMaster() throws MasterNotRunningException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isMasterRunning() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean tableExists(StringBytes tableName)
      throws MasterNotRunningException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isTableEnabled(StringBytes tableName) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isTableDisabled(StringBytes tableName) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isTableAvailable(StringBytes tableName) throws IOException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HTableDescriptor getHTableDescriptor(StringBytes tableName)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HRegionLocation locateRegion(StringBytes tableName, byte[] row)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void clearRegionCache() {
    // TODO Auto-generated method stub

  }

  @Override
  public HRegionLocation relocateRegion(StringBytes tableName, byte[] row)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HRegionInterface getHRegionConnection(HServerAddress regionServer,
      HBaseRPCOptions options) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HRegionInterface getHRegionConnection(HServerAddress regionServer,
      boolean getMaster, HBaseRPCOptions options) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HRegionInterface getHRegionConnection(HServerAddress regionServer)
      throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HRegionInterface getHRegionConnection(HServerAddress regionServer,
      boolean getMaster) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public HRegionLocation getRegionLocation(StringBytes tableName, byte[] row,
      boolean reload) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T getRegionServerWithRetries(ServerCallable<T> callable)
      throws IOException, RuntimeException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T getRegionServerWithoutRetries(ServerCallable<T> callable)
      throws IOException, RuntimeException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T getRegionServerWithoutRetries(ServerCallable<T> callable,
      boolean instantiateRegionLocation) throws IOException, RuntimeException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Result[] processBatchOfGets(List<Get> actions, StringBytes tableName,
      HBaseRPCOptions options) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int processBatchOfRows(ArrayList<Put> list, StringBytes tableName,
      HBaseRPCOptions options) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int processBatchOfDeletes(List<Delete> list, StringBytes tableName,
      HBaseRPCOptions options) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void processBatchOfPuts(List<Put> list, StringBytes tableName,
      HBaseRPCOptions options) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public int processBatchOfRowMutations(List<RowMutations> list,
      StringBytes tableName, HBaseRPCOptions options) throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void processBatchedGets(List<Get> actions, StringBytes tableName,
      ExecutorService pool, Result[] results, HBaseRPCOptions options)
      throws IOException, InterruptedException {
    // TODO Auto-generated method stub

  }

  @Override
  public void processBatchedMutations(List<Mutation> actions,
      StringBytes tableName, ExecutorService pool, List<Mutation> failures,
      HBaseRPCOptions options) throws IOException, InterruptedException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Put> processListOfMultiPut(List<MultiPut> mputs,
      StringBytes tableName, HBaseRPCOptions options,
      Map<String, HRegionFailureInfo> failureInfo) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteCachedLocation(StringBytes tableName, byte[] row,
      HServerAddress oldLoc) {
    // TODO Auto-generated method stub

  }

  @Override
  public void setRegionCachePrefetch(StringBytes tableName, boolean enable) {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean getRegionCachePrefetch(StringBytes tableName) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void prewarmRegionCache(StringBytes tableName,
      Map<HRegionInfo, HServerAddress> regions) {
    // TODO Auto-generated method stub

  }

  @Override
  public void startBatchedLoad(StringBytes tableName) {
    // TODO Auto-generated method stub

  }

  @Override
  public void endBatchedLoad(StringBytes tableName, HBaseRPCOptions options)
      throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<OperationContext> getAndResetOperationContext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void resetOperationContext() {
    // TODO Auto-generated method stub

  }

  @Override
  public Configuration getConf() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void flushRegionAndWait(HRegionInfo regionInfo, HServerAddress addr,
      long acceptableWindowForLastFlush, long maximumWaitTime)
      throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public String getServerConfProperty(String prop) throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<HRegionLocation> getCachedHRegionLocations(
      StringBytes tableName, boolean forceRefresh) {
    // TODO Auto-generated method stub
    return null;
  }

}
