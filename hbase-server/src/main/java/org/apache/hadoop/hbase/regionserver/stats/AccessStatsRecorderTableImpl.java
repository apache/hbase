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
package org.apache.hadoop.hbase.regionserver.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.stats.AccessStats.AccessStatsGranularity;
import org.apache.hadoop.hbase.regionserver.stats.AccessStats.AccessStatsType;
import org.apache.hadoop.hbase.regionserver.stats.AccessStats.KeyPartDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class AccessStatsRecorderTableImpl implements IAccessStatsRecorder {

  private static final Log LOG = LogFactory.getLog(AccessStatsRecorderTableImpl.class);

  private Connection connection;
  private TableName tableNameToStoreStats;
  private UidGenerator uidGenerator;

  private boolean isInitializationDoneCorrectly = false;

  public AccessStatsRecorderTableImpl(Configuration configuration) {
    this.tableNameToStoreStats = AccessStatsRecorderUtils.getInstance().getTableNameToRecordStats();

    try {
      connection = ConnectionFactory.createConnection(configuration);
      uidGenerator = new UidGenerator(configuration);
      isInitializationDoneCorrectly = true;
    } catch (Exception e) {
      LOG.error(
        "Exception while creating connection in AccessStatsRecorderTableImpl " + e.getMessage());
    }
  }

  @Override
  public void writeAccessStats(List<AccessStats> listAccessStats) {
    if (!isInitializationDoneCorrectly) {
      LOG.error(
        "AccessStatsRecorderTableImpl is not initialized properly so skipping writeAccessStats call");

      return;
    }

    try (HTable tableToStoreStats = (HTable) connection.getTable(tableNameToStoreStats)) {
      List<Put> puts = new ArrayList<>();

      for (AccessStats accessStats : listAccessStats) {
        byte[] rowKey = getRowKey(accessStats.getTable(), accessStats.getKeyPartDescriptors(),
          accessStats.normalizedTimeInEpoch);

        Put put = new Put(rowKey);
        put.addColumn(AccessStatsRecorderConstants.STATS_COLUMN_FAMILY,
          accessStats.getAccessStatsType(), accessStats.getValueInBytes());
        put.addColumn(AccessStatsRecorderConstants.STATS_COLUMN_FAMILY,
          AccessStatsRecorderConstants.STATS_COLUMN_QUALIFIER_START_KEY,
          accessStats.getKeyRangeStart());
        put.addColumn(AccessStatsRecorderConstants.STATS_COLUMN_FAMILY,
          AccessStatsRecorderConstants.STATS_COLUMN_QUALIFIER_END_KEY,
          accessStats.getKeyRangeEnd());
        puts.add(put);

        LOG.info("Access stats details " + accessStats.getTable().getNameAsString() + " "
            + accessStats.normalizedTimeInEpoch + " "
            + Bytes.toString(accessStats.getAccessStatsType()) + " " + accessStats.getValue());
      }

      tableToStoreStats.put(puts);

      LOG.info("Access stats written to the table");

    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Exception in writeAccessStats with message " + e.getMessage());
    }

  }

  @Override
  public List<AccessStats> readAccessStats(TableName table, AccessStatsType accessStatsType,
      AccessStatsGranularity accessStatsGranularity, long epochTimeTo, int numIterations) {
    int iterationDurationInMinutes = AccessStatsRecorderUtils.getInstance().getIterationDuration();
    epochTimeTo = AccessStatsRecorderUtils.getInstance().getNormalizedTime(epochTimeTo);

    List<AccessStats> accessStatsList = new ArrayList<>();

    try (HTable tableToStoreStats = (HTable) connection.getTable(tableNameToStoreStats)) {
      long epochCurrent;

      for (int i = 0; i < numIterations; i++) {
        epochCurrent = epochTimeTo - i * iterationDurationInMinutes * 60 * 1000;

        Scan scan = new Scan();
        byte[] rowKeyPrefix = getRowKeyPrefix(table, epochCurrent);
        scan.setRowPrefixFilter(rowKeyPrefix);
        ResultScanner scanner = tableToStoreStats.getScanner(scan);

        for (Result result = scanner.next(); result != null; result = scanner.next()) {
          byte[] rowKey = result.getRow();
          AccessStats accessStats = AccessStatsFactory.getAccessStatsObj(table,
            accessStatsGranularity, accessStatsType, 
            readValue(AccessStatsRecorderConstants.STATS_COLUMN_QUALIFIER_START_KEY, result),
            readValue(AccessStatsRecorderConstants.STATS_COLUMN_QUALIFIER_END_KEY, result),
            epochCurrent, Bytes.toLong(readValue(Bytes.toBytes(accessStatsType.toString()), result)));

          byte[][] byteArrayOfArrays =
              accessStats.convertKeySuffixToByteArrayList(rowKey, rowKeyPrefix.length);

          List<String> stringList = new ArrayList<>();
          for (int j = 0; j < byteArrayOfArrays.length; j++) {
            stringList.add(uidGenerator.getStringForUID(byteArrayOfArrays[j]));
          }

          accessStats.setFieldsUsingKeyParts(stringList);

          accessStatsList.add(accessStats);
        }
      }
    } catch (Exception e) {
    	e.printStackTrace();
      LOG.error("Exception in readAccessStats with message " + e.getMessage());
    }
    return accessStatsList;
  }

  private byte[] readValue(byte[] columnQualifier, Result result) {
    return result.getValue(AccessStatsRecorderConstants.STATS_COLUMN_FAMILY, columnQualifier);
  }

  /*
   * Row key is byte encoded collection of strings -
   * <namespace_uid - 1 byte><table_name_uid - 3 byte><timestamp - 4 bytes><KeyPartDescriptor List returned by AccessStats>
   */
  private byte[] getRowKey(TableName table, List<KeyPartDescriptor> keyPartDescriptors,
      long epochTime) throws Exception {
    epochTime = AccessStatsRecorderUtils.getInstance().getNormalizedTime(epochTime);
    int currentPointer = 0;
    byte[] rowKeyPrefix = getRowKeyPrefix(table, epochTime);

    // first count number of bytes needed in byte array
    int numBytes = rowKeyPrefix.length;

    for (KeyPartDescriptor keyPartDescriptor : keyPartDescriptors) {
      numBytes += keyPartDescriptor.getUidLengthInBytes();
    }

    byte[] rowKey = new byte[numBytes];
    currentPointer = copyByteArray(currentPointer, rowKey, rowKeyPrefix);

    for (KeyPartDescriptor keyPartDescriptor : keyPartDescriptors) {
      byte[] uid = uidGenerator.getUIDForString(keyPartDescriptor.getKeyStr(),
        keyPartDescriptor.getUidLengthInBytes());
      
      currentPointer = copyByteArray(currentPointer, rowKey, uid);
    }

    return rowKey;
  }

  /*
   * row key prefix is fixed length byte array
   * <namespace_uid - 1 byte><table_name_uid - 3 byte><timestamp - 4 bytes>
   */
  private byte[] getRowKeyPrefix(TableName table, long epochTime) throws Exception {
    epochTime = AccessStatsRecorderUtils.getInstance().getNormalizedTime(epochTime);
    int numBytes = 1 + 3 + 4;
    int currentPointer = 0;

    byte[] rowKeyPrefix = new byte[numBytes];
    byte[] uid = uidGenerator.getUIDForString(table.getNameAsString(), 1);
    currentPointer = copyByteArray(currentPointer, rowKeyPrefix, uid);
    uid = uidGenerator.getUIDForString(table.getNamespaceAsString(), 3);
    currentPointer = copyByteArray(currentPointer, rowKeyPrefix, uid);

    int unixTime = (int) (epochTime / 1000);
    byte[] timeStampInBytes = new byte[] { (byte) (unixTime >> 24), (byte) (unixTime >> 16),
        (byte) (unixTime >> 8), (byte) unixTime

    };

    currentPointer = copyByteArray(currentPointer, rowKeyPrefix, timeStampInBytes);

    return rowKeyPrefix;
  }

  private int copyByteArray(int startPointerInDestArray, byte[] destArray, byte[] sourceArray) {
    for (int i = 0; i < sourceArray.length; i++) {
      destArray[startPointerInDestArray++] = sourceArray[i];
    }
    return startPointerInDestArray;
  }

  @Override
  public void close() throws IOException {
    connection.close();
  }
}