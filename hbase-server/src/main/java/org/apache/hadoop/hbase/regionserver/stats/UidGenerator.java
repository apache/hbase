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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

/*
 * This class is to generate UID for storing access stats. One special row in the access stats will
 * be used to store columns for generating monotonically increasing and unique ids. The idea is to
 * use HBase's incrementColumnValue feature to generate monotonically increasing numbers and use
 * those numbers as uids. This class support generation of UID with given length. For each length,
 * specific column qualifies is used to make sure no collision happens across different byte array
 * lengths.
 */
@InterfaceAudience.Private
public class UidGenerator implements Closeable {

  private static final Log LOG = LogFactory.getLog(UidGenerator.class);

  private Connection connection;
  private TableName tableName;

  String zookeeperParentNode = "/salesforce/uid-generator";
  ZKWatcher zooKeeperWatcher;

  private HashMap<Integer, HashMap<String, Long>> stringToByteArrayHashMap = new HashMap<>();
  private HashMap<Integer, HashMap<String, String>> byteArrayToStringHashMap = new HashMap<>();
  
  boolean storeUIDsInMemory = true;

  public UidGenerator(Configuration configuration) throws IOException, KeeperException {
    this.tableName = AccessStatsRecorderUtils.getInstance().getTableNameToRecordStats();
    connection = ConnectionFactory.createConnection(configuration);

    zooKeeperWatcher = new ZKWatcher(configuration, "UidGenerator", null);
    LOG.info("Creating the parent lock node:" + zookeeperParentNode);
    ZKUtil.createWithParents(zooKeeperWatcher, zookeeperParentNode);
    
    for(int i=1; i<= AccessStatsRecorderConstants.MAX_UID_BYTE_ARRAY_LENGTH_SUPPORTED; i++)
    {
      stringToByteArrayHashMap.put(i, new HashMap<String, Long>());
      byteArrayToStringHashMap.put(i, new HashMap<String, String>());
    }
  }
  
  /*
   * If UIDs need not be cached in memory then use this constructor.
   * Currently its being used only for testing purpose.
   */
  public UidGenerator(Configuration configuration, boolean storeUIDsInMemory) throws IOException, KeeperException {
   this(configuration);
   this.storeUIDsInMemory = storeUIDsInMemory;
  }

  public byte[] getUIDForString(String keyStr, int uidLengthInBytes)
      throws Exception, InterruptedException {
    if(uidLengthInBytes > 4)
    {
      throw new Exception("UID only upto size 4 is supported.");
    }
    
    byte[] uid = null;
    for (int i = 0; i < AccessStatsRecorderConstants.NUM_RETRIES_FOR_GENERATING_UID; i++) {
      uid = getUIDForStringInternal(keyStr, uidLengthInBytes);
      if (uid != null) {
        break;
      }
      
      LOG.info("getUIDForString returned null, Retry Count :" + i);
      Thread.sleep(AccessStatsRecorderConstants.WAIT_TIME_BEFORE_RETRY_FOR_GENERATING_UID); 
    }

    if (uid == null) {
      throw new Exception("Couldn't generate UID even after multiple retries.");
    }

    return uid;
  }

  public String getStringForUID(byte[] uidByteArray) throws IOException {
    String string = byteArrayToStringHashMap.get(uidByteArray.length).get(Bytes.toString(uidByteArray));

    if (string == null) {
      LOG.info("String for UID not found in memory, looking in table.");
      try (HTable table = (HTable) connection.getTable(tableName)) {
        Get getVal = new Get(uidByteArray);
        Result result = table.get(getVal);
        byte[] value = result.getValue(AccessStatsRecorderConstants.UID_COLUMN_FAMILY,
          getColumnQualifierBasedOnUidLength(uidByteArray.length));
        if(value != null)
        {
          string = new String(value);
        }
      }
    }

    return string;
  }

  @Override
  public void close() throws IOException {
    if (connection != null) {
      connection.close();
    }

    if (zooKeeperWatcher != null) {
      zooKeeperWatcher.close();
    }
  }

  private byte[] getColumnQualifierBasedOnUidLength(int uidLengthInBytes) {
    byte[] columnQualifierByteArray =
        AccessStatsRecorderConstants.UID_AUTO_INCREMENT_COLUMN_QUALIFIER_1;
    switch (uidLengthInBytes) {
    case 1:
      columnQualifierByteArray = AccessStatsRecorderConstants.UID_AUTO_INCREMENT_COLUMN_QUALIFIER_1;
      break;

    case 2:
      columnQualifierByteArray = AccessStatsRecorderConstants.UID_AUTO_INCREMENT_COLUMN_QUALIFIER_2;
      break;

    case 3:
      columnQualifierByteArray = AccessStatsRecorderConstants.UID_AUTO_INCREMENT_COLUMN_QUALIFIER_3;
      break;

    case 4:
      columnQualifierByteArray = AccessStatsRecorderConstants.UID_AUTO_INCREMENT_COLUMN_QUALIFIER_4;
      break;

    default:
      break;
    }
    return columnQualifierByteArray;
  }

  private byte[] readUIDFromTable(String keyStr, HTable table, int uidLengthInBytes)
      throws IOException {
    byte[] byteArray = null;

    Get getVal = new Get(Bytes.toBytes(keyStr));
    Result result = table.get(getVal);

    byteArray = result.getValue(AccessStatsRecorderConstants.UID_COLUMN_FAMILY,
      getColumnQualifierBasedOnUidLength(uidLengthInBytes));

    return byteArray;
  }

  /*
   * Following is rough sketch of the steps followed -

   * 1. Check if there is an entry in the table with given string as key
   * 2. If yes, then use the value as uid.
   * 3. If not, then follow next set of steps -
   * 4. take a lock at zookeeper level for the given string.
   * 5. Check again if there is an entry in the table with given string as key; 
   * this is important as someone might have taken the lock, finished the work and 
   * eventually released the lock between steps #1 and #4. If there is an entry present, 
   * simply use that value as uid. If no entry present then move to next steps -
   * 6. get uid by HTable#incrementColumnValue
   * 7. store string equivalent as row key and uid as value 
   * 8. store uid as row key and string equivalent as value
   * 9. release zookeeper lock
   */
  private byte[] getUIDForStringInternal(String keyStr, int uidLengthInBytes) throws Exception {
    byte[] byteArray = null;
    
    Long byteArrayinLong = stringToByteArrayHashMap.get(uidLengthInBytes).get(keyStr);
    if(byteArrayinLong != null)
    {
      LOG.info("UID found in memory; keyStr - " + keyStr);
      
      byteArray = convertLongToByteArray(uidLengthInBytes, byteArrayinLong);
    }

    if (byteArray == null) {
      LOG.info("UID not found in memory; reading table. keyStr - " + keyStr);
      long nextUid =0;
      try (HTable table = (HTable) connection.getTable(tableName)) {
        byteArray = readUIDFromTable(keyStr, table, uidLengthInBytes);

        if (byteArray == null) {
          LOG.info("UID not found in table; need to generate. keyStr - " + keyStr);

          if (acquireLock(uidLengthInBytes + "_" + keyStr)) {
            LOG.info("Acquired zookeeper lock.");
            // check the table again
            byteArray = readUIDFromTable(keyStr, table, uidLengthInBytes);

            if (byteArray == null) { // generate uid
              LOG.info("Generating UID for keyStr - " + keyStr);

              byte[] columnQualifierByteArray =
                  getColumnQualifierBasedOnUidLength(uidLengthInBytes);

              nextUid = table.incrementColumnValue(
                AccessStatsRecorderConstants.UID_AUTO_INCREMENT_ROW_KEY,
                AccessStatsRecorderConstants.UID_AUTO_INCREMENT_COLUMN_FAMILY,
                columnQualifierByteArray, 1);
              
              byteArray = convertLongToByteArray(uidLengthInBytes, nextUid);
              
              List<Put> putList = new ArrayList<>();

              Put put = new Put(Bytes.toBytes(keyStr));
              put.addColumn(AccessStatsRecorderConstants.UID_COLUMN_FAMILY,
                columnQualifierByteArray, byteArray);
              putList.add(put);

              put = new Put(byteArray);
              put.addColumn(AccessStatsRecorderConstants.UID_COLUMN_FAMILY,
                columnQualifierByteArray, Bytes.toBytes(keyStr));
              putList.add(put);

              table.put(putList);
            } else {
              LOG.info("Someone else had already generated required UID; no worries. keyStr - "+ keyStr);
            }
            releaseLock(uidLengthInBytes + "_" + keyStr);
          } else {
            LOG.warn(
              "Couldn't acquire zookeeper lock; this means someone is already generating UID for this keyStr. Please wait and retry. keyStr - "+ keyStr);
          }
        }
      }
      if (storeUIDsInMemory && byteArray != null) { // fill the in-memory hashmap
        stringToByteArrayHashMap.get(uidLengthInBytes).put(keyStr, converyByteArrayToLong(byteArray));
        byteArrayToStringHashMap.get(uidLengthInBytes).put(Bytes.toString(byteArray), keyStr); 
      }
    }

    return byteArray;
  }

  private long converyByteArrayToLong(byte[] byteArray)
  {
    long value = 0;
    for (int i = 0; i < byteArray.length; i++)
    {
       value += ((long) byteArray[i] & 0xffL) << (8 * i);
    }
    
    return value;
  }
  
  private byte[] convertLongToByteArray(int uidLengthInBytes, Long byteArrayinLong) {
    byte[] byteArray = new byte[uidLengthInBytes];
    for(int i =0; i< uidLengthInBytes; i++)
    {
      byteArray[i] = (byte) (byteArrayinLong >> (i*8));
    }
    return byteArray;
  }

  private boolean acquireLock(String lockName) throws KeeperException, InterruptedException {
     String lockNode = zookeeperParentNode + "/" + lockName;
    String nodeValue = UUID.randomUUID().toString();
    LOG.info("Trying to acquire the lock by creating node:" + lockNode + " value:" + nodeValue);
    try {
      zooKeeperWatcher.getRecoverableZooKeeper().create(lockNode, Bytes.toBytes(nodeValue),
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException.NodeExistsException e) {
      LOG.info("Node " + lockName + " already exists. Another process has the lock. "
          + ". This may not be an error condition." + e.getMessage());
      return false;
    }
    LOG.info("Obtained the lock :" + lockNode);
    return true;
  }

  private boolean releaseLock(String lockName) throws KeeperException, InterruptedException {
    String lockNode = zookeeperParentNode + "/" + lockName;
    LOG.info("Releasing lock node:" + lockNode);
    try {
      zooKeeperWatcher.getRecoverableZooKeeper().delete(lockNode, 0);
    } catch (KeeperException.NodeExistsException e) {
      LOG.info("Node " + lockName + " already exists. Another process has the lock. "
          + ". This may not be an error condition." + e.getMessage());
      return false;
    }
    LOG.info("Deleted the lock :" + lockNode);
    return true;
  }
}