/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiPut;
import org.apache.hadoop.hbase.client.MultiPutResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TMultiResponse;
import org.apache.hadoop.hbase.client.TRowMutations;
import org.apache.hadoop.hbase.io.hfile.histogram.HFileHistogram.Bucket;
import org.apache.hadoop.hbase.ipc.thrift.exceptions.ThriftHBaseException;
import org.apache.hadoop.hbase.master.AssignmentPlan;

import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Clients interact with ThriftHRegionServers using a handle to the
 * {@link ThriftHRegionInterface}.
 *
 * This interface is just to capture the swift-version of the methods in
 * {@link HRegionInterface}.
 *
 */
@ThriftService
public interface ThriftHRegionInterface extends ThriftClientInterface {

  /**
   * Calls an endpoint on an region server.
   *
   * TODO make regionName/startRow/stopRow a list.
   *
   * @param epName  the endpoint name.
   * @param methodName  the method name.
   * @param regionName  the name of the region
   * @param startRow  the start row, inclusive
   * @param stopRow  the stop row, exclusive
   * @return  the computed value.
   */
  @ThriftMethod(value = "callEndpoint", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public byte[] callEndpoint(@ThriftField(name = "epName") String epName,
      @ThriftField(name = "methodName") String methodName,
      @ThriftField(name = "regionName") byte[] regionName,
      @ThriftField(name = "startRow") byte[] startRow,
      @ThriftField(name = "stopRow") byte[] stopRow)
      throws ThriftHBaseException;

  /**
   * Get metainfo about an HRegion
   *
   * @param regionName name of the region
   * @return HRegionInfo object for region
   * @throws ThriftHBaseException
   */
  @ThriftMethod(value = "getRegionInfo", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public HRegionInfo getRegionInfo(
      @ThriftField(name="regionName") final byte[] regionName)
      throws ThriftHBaseException;

  /**
   * Return all the data for the row that matches <i>row</i> exactly,
   * or the one that immediately proceeds it.
   *
   * @param regionName region name
   * @param row row key
   * @param family Column family to look for row in.
   * @return map of values
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "getClosestRowBefore", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public Result getClosestRowBefore(
      @ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="row") final byte[] row,
      @ThriftField(name="family") final byte[] family) throws ThriftHBaseException;


  @ThriftMethod(value = "getClosestRowBeforeAsync", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1)})
  public ListenableFuture<Result> getClosestRowBeforeAsync(
      @ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="row") final byte[] row,
      @ThriftField(name="family") final byte[] family);

  /**
   * Flush the given region
   */
  @ThriftMethod(value = "flushRegion", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1)})
  public void flushRegion(@ThriftField(name="regionName") byte[] regionName)
      throws ThriftHBaseException;

  /**
   * Flush the given region if lastFlushTime < ifOlderThanTS
   */
  @ThriftMethod(value = "flushRegionIfOlderThanTS", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1)})
  public void flushRegion(@ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="ifOlderThanTS") long ifOlderThanTS)
      throws ThriftHBaseException;

  /**
   * Gets last flush time (in milli sec) for the given region
   * @return the last flush time for a region
   */
  @ThriftMethod("getLastFlushTime")
  public long getLastFlushTime(@ThriftField(name="regionName") byte[] regionName);

  /**
   * Gets last flush time (in milli sec) for all regions on the server
   * @return a map of regionName to the last flush time for the region
   */
  @ThriftMethod(value = "getLastFlushTimes")
  public Map<byte[], Long> getLastFlushTimes();

  /**
   * Gets the current time (in milli sec) at the region server
   * @return time in milli seconds at the regionserver.
   */
  @ThriftMethod(value = "getCurrentTimeMillis")
  public long getCurrentTimeMillis();

  /**
   * Gets the current startCode at the region server
   * @return startCode -- time in milli seconds when the regionserver started.
   */
  @ThriftMethod(value = "getStartCode")
  public long getStartCode();

  /**
   * Get a list of store files for a particular CF in a particular region
   * @param region name
   * @param CF name
   * @return the list of store files
   */
  @ThriftMethod(value = "getStoreFileList", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1)})
  public List<String> getStoreFileList(
      @ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="columnFamily") byte[] columnFamily)
      throws ThriftHBaseException;

  /**
   * Get a list of store files for a set of CFs in a particular region
   * @param region name
   * @param CF names
   * @return the list of store files
   */
  @ThriftMethod(value = "getStoreFileListForColumnFamilies", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public List<String> getStoreFileListForColumnFamilies(
      @ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="columnFamilies") List<byte[]> columnFamilies)
      throws ThriftHBaseException;

  /**
   * Get a list of store files for all CFs in a particular region
   * @param region name
   * @return the list of store files
   */
  @ThriftMethod(value = "getStoreFileListForAllColumnFamilies", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public List<String> getStoreFileListForAllColumnFamilies(
      @ThriftField(name="regionName") byte[] regionName)
      throws ThriftHBaseException;

  /**
  * @param rollCurrentHLog if true, the current HLog is rolled and will be
  * included in the list returned
  * @return list of HLog files
  */
  @ThriftMethod(value = "getHLogsList",  exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public List<String> getHLogsList(
      @ThriftField(name="rollCurrentHLog") boolean rollCurrentHLog)
      throws ThriftHBaseException;

  /**
   * TODO: deprecate this
   * Perform Get operation.
   * @param regionName name of region to get from
   * @param get Get operation
   * @return Result
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "processGet", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public Result get(@ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="get") Get get)
      throws ThriftHBaseException;

  @ThriftMethod(value = "getAsync", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1)})
  public ListenableFuture<Result> getAsync(
      @ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="get") Get get);

  @ThriftMethod(value = "getRows", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public List<Result> getRows(@ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="gets") List<Get> gets)
      throws ThriftHBaseException;

  /**
   * Perform exists operation.
   * @param regionName name of region to get from
   * @param get Get operation describing cell to test
   * @return true if exists
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "exists", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public boolean exists(@ThriftField(name="regionName") byte [] regionName,
      @ThriftField(name="get") Get get)
      throws ThriftHBaseException;

  /**
   * Put data into the specified region
   * @param regionName region name
   * @param put the data to be put
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "processPut", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public void put(@ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="put") final Put put)
      throws ThriftHBaseException;

  /**
   * Put an array of puts into the specified region
   *
   * @param regionName region name
   * @param puts List of puts to execute
   * @return The number of processed put's.  Returns -1 if all Puts
   * processed successfully.
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "putRows", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public int putRows(@ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="puts") final List<Put> puts)
      throws ThriftHBaseException;

  /**
   * Deletes all the KeyValues that match those found in the Delete object,
   * if their ts <= to the Delete. In case of a delete with a specific ts it
   * only deletes that specific KeyValue.
   * @param regionName region name
   * @param delete delete object
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "processDelete", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public void processDelete(@ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="deleteArg") final Delete delete)
      throws ThriftHBaseException;

  @ThriftMethod(value = "deleteAsync", exception =  {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public ListenableFuture<Void> deleteAsync(@ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="deleteArg") final Delete delete);

  /**
   * Put an array of deletes into the specified region
   *
   * @param regionName region name
   * @param deletes delete List to execute
   * @return The number of processed deletes.  Returns -1 if all Deletes
   * processed successfully.
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "processListOfDeletes", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public int processListOfDeletes(
      @ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="deletes") final List<Delete> deletes)
      throws ThriftHBaseException;

  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the put. If passed expected value is null, then the
   * check is for non-existance of the row/column.
   *
   * @param regionName region name
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param put data to put if check succeeds
   * @throws ThriftHBaseException e
   * @return true if the new put was execute, false otherwise
   */
  @ThriftMethod(value = "checkAndPut", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public boolean checkAndPut(
      @ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="row") final byte[] row,
      @ThriftField(name="family") final byte[] family,
      @ThriftField(name="qualifier") final byte[] qualifier,
      @ThriftField(name="value") final byte[] value,
      @ThriftField(name="put") final Put put)
      throws ThriftHBaseException;


  /**
   * Atomically checks if a row/family/qualifier value match the expectedValue.
   * If it does, it adds the delete. If passed expected value is null, then the
   * check is for non-existance of the row/column.
   *
   * @param regionName region name
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param value the expected value
   * @param delete data to delete if check succeeds
   * @throws ThriftHBaseException e
   * @return true if the new delete was execute, false otherwise
   */
  @ThriftMethod(value = "checkAndDelete", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public boolean checkAndDelete(
      @ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="row") final byte[] row,
      @ThriftField(name="family") final byte[] family,
      @ThriftField(name="qualifier") final byte[] qualifier,
      @ThriftField(name="value") final byte[] value,
      @ThriftField(name="deleteArg") final Delete delete)
      throws ThriftHBaseException;

  /**
   * Atomically increments a column value. If the column value isn't long-like,
   * this could throw an exception. If passed expected value is null, then the
   * check is for non-existance of the row/column.
   *
   * @param regionName region name
   * @param row row to check
   * @param family column family
   * @param qualifier column qualifier
   * @param amount long amount to increment
   * @param writeToWAL whether to write the increment to the WAL
   * @return new incremented column value
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "incrementColumnValue", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public long incrementColumnValue(
      @ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="row") byte[] row,
      @ThriftField(name="family") byte[] family,
      @ThriftField(name="qualifier") byte[] qualifier,
      @ThriftField(name="amount") long amount,
      @ThriftField(name="writeToWAL") boolean writeToWAL)
      throws ThriftHBaseException;


  //
  // remote scanner interface
  //

  /**
   * Opens a remote scanner with a RowFilter.
   *
   * @param regionName name of region to scan
   * @param scan configured scan object
   * @return scannerId scanner identifier used in other calls
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "openScanner", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public long openScanner(@ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="scan") final Scan scan)
      throws ThriftHBaseException;

  @ThriftMethod(value = "mutateRow", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public void mutateRow(@ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="arm") TRowMutations arm)
      throws ThriftHBaseException;

  @ThriftMethod(value = "mutateRowAsync", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public ListenableFuture<Void> mutateRowAsync(
      @ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="arm") TRowMutations arm);

  @ThriftMethod(value = "mutateRows", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public void mutateRows(@ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="armList") List<TRowMutations> armList)
      throws ThriftHBaseException;

  /**
   * Get the next set of values. Do not use with thrift
   * @param scannerId clientId passed to openScanner
   * @return map of values; returns null if no results.
   * @throws ThriftHBaseException e
   */
  @Deprecated
  @ThriftMethod(value = "next", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public Result next(@ThriftField(name="scannerId") long scannerId)
      throws ThriftHBaseException;

  /**
   * Get the next set of values
   * @param scannerId clientId passed to openScanner
   * @param numberOfRows the number of rows to fetch
   * @return Array of Results (map of values); array is empty if done with this
   * region and null if we are NOT to go to the next region (happens when a
   * filter rules that the scan is done).
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "nextRows", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public List<Result> nextRows(@ThriftField(name="scannerId") long scannerId,
      @ThriftField(name="numberOfRows") int numberOfRows)
      throws ThriftHBaseException;

  /**
   * Close a scanner
   *
   * @param scannerId the scanner id returned by openScanner
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "close", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public void close(@ThriftField(name="scannerId") long scannerId) throws ThriftHBaseException;

  /**
   * Opens a remote row lock.
   *
   * @param regionName name of region
   * @param row row to lock
   * @return lockId lock identifier
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "lockRow", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public long lockRow(@ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="row") final byte[] row)
      throws ThriftHBaseException;

  @ThriftMethod(value = "lockRowAsync", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public ListenableFuture<RowLock> lockRowAsync(
      @ThriftField(name="regionName") byte[] regionName,
     @ThriftField(name="row") byte[] row);

  /**
   * Releases a remote row lock.
   *
   * @param regionName region name
   * @param lockId the lock id returned by lockRow
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "unlockRow", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public void unlockRow(@ThriftField(name="regionName") final byte[] regionName,
      @ThriftField(name="lockId") final long lockId)
      throws ThriftHBaseException;

  @ThriftMethod(value = "unlockRowAsync", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public ListenableFuture<Void> unlockRowAsync(
      @ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="lockId") long lockId);

  /**
   * Method used when a master is taking the place of another failed one.
   * @return All regions assigned on this region server
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "getRegionsAssignment", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public List<HRegionInfo> getRegionsAssignment() throws ThriftHBaseException;

  /**
   * Method used when a master is taking the place of another failed one.
   * @return The HSI
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "getHServerInfo", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public HServerInfo getHServerInfo() throws ThriftHBaseException;

  /**
   * Method used for doing multiple actions(Deletes, Gets and Puts) in one call
   * @param multi
   * @return MultiResult
   * @throws ThriftHBaseException
   */
  @ThriftMethod(value = "multiAction", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public TMultiResponse multiAction(@ThriftField(name="multi") MultiAction multi)
      throws ThriftHBaseException;

  /**
   * Multi put for putting multiple regions worth of puts at once.
   *
   * @param puts the request
   * @return the reply
   * @throws ThriftHBaseException e
   */
  @ThriftMethod(value = "multiPut", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public MultiPutResponse multiPut(@ThriftField(name="puts") MultiPut puts)
      throws ThriftHBaseException;

  /**
   * Bulk load an HFile into an open region
   */
  @ThriftMethod(value = "bulkLoadHFile", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public void bulkLoadHFile(@ThriftField(name="hfilePath") String hfilePath,
      @ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="familyName") byte[] familyName)
      throws ThriftHBaseException;

  @ThriftMethod(exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) },
      value = "bulkLoadHFileSeqNum")
  public void bulkLoadHFile(
      @ThriftField(name="hfilePath") String hfilePath,
      @ThriftField(name="regionName") byte[] regionName,
      @ThriftField(name="familyName") byte[] familyName,
      @ThriftField(name="assignSeqNum") boolean assignSeqNum)
      throws ThriftHBaseException;

  /**
   * Closes the specified region.
   * @param hri region to be closed
   * @param reportWhenCompleted whether to report to master
   * @throws ThriftHBaseException
   */
  @ThriftMethod(value = "closeRegion", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public void closeRegion(@ThriftField(name="hri") final HRegionInfo hri,
      @ThriftField(name="reportWhenCompleted") final boolean reportWhenCompleted)
      throws ThriftHBaseException;

  /**
   * Update the assignment plan for each region server.
   * @param updatedFavoredNodesMap
   */
  @ThriftMethod(value = "updateFavoredNodes", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public int updateFavoredNodes(@ThriftField(name="plan") AssignmentPlan plan)
      throws ThriftHBaseException;

  /**
   * Update the configuration.
   */
  @ThriftMethod(value = "updateConfiguration", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public void updateConfiguration() throws ThriftHBaseException;

  /**
   * Stop this service.
   * @param why Why we're stopping.
   */
  @ThriftMethod(value = "stop")
  public void stop(@ThriftField(name="why") String why);

  /** @return why we are stopping */
  @ThriftMethod(value = "getStopReason")
  public String getStopReason();


  /**
   * Set the number of threads to be used for HDFS Quorum reads
   *
   * @param maxThreads. quourm reads will be disabled if set to <= 0
   *
   */
  @ThriftMethod(value = "setNumHDFSQuorumReadThreads")
  public void setNumHDFSQuorumReadThreads(
      @ThriftField(name="maxThreads") int maxThreads);

  /**
   * Set the amount of time we wait before initiating a second read when
   * using HDFS Quorum reads
   *
   * @param timeoutMillis.
   *
   */
  @ThriftMethod(value = "setHDFSQuorumReadTimeoutMillis")
  public void setHDFSQuorumReadTimeoutMillis(
      @ThriftField(name="timeoutMillis") long timeoutMillis);

  @ThriftMethod(value = "stopForRestart")
  public void stopForRestart();

  @ThriftMethod(value = "isStopped")
  public boolean isStopped();

  /**
   * Get a configuration property from an HRegion
   *
   * @param String propName name of configuration property
   * @return String value of property
   * @throws IOException e
   */
  @ThriftMethod(value = "getConfProperty", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public String getConfProperty(String paramName) throws ThriftHBaseException;

  @ThriftMethod(value = "getHistogram", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public List<Bucket> getHistogram(byte[] regionName)
      throws ThriftHBaseException;

  /**
   * Returns the list of buckets which represent the uniform depth histogram
   * for a given store.
   * @param regionName
   * @param family
   * @return
   * @throws IOException
   */
  @ThriftMethod(value = "getHistogramForStore", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public List<Bucket> getHistogramForStore(byte[] regionName, byte[] family)
      throws ThriftHBaseException;

  @ThriftMethod(value = "getHistograms", exception = {
      @ThriftException(type = ThriftHBaseException.class, id = 1) })
  public List<List<Bucket>> getHistograms(List<byte[]> regionNames)
      throws ThriftHBaseException;
}
