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
package org.apache.hadoop.hbase.manual.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;


import java.math.BigInteger;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.lang.StringUtils;

public class HBaseUtils
{
  private static final Log LOG = LogFactory.getLog(HBaseUtils.class);
  
  private final static String MAXMD5 = "FFFFFFFF";
  private final static int rowComparisonLength = MAXMD5.length();
  private static int DEFAULT_REGIONS_PER_SERVER = 5;

  public static void sleep(int millisecs) {
    try {
      Thread.sleep(millisecs);
    } catch (InterruptedException e) {
    }
  }
  
  public static HTable getHTable(Configuration conf, byte[] tableName) {
    HTable table = null;
    try {
      table = new HTable(conf, tableName);
    }
    catch (IOException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return table;
  }

  // the table will be pre-split assuming a MD5 prefixed key space.
  public static void createTableIfNotExists(HBaseConfiguration conf, byte[] tableName, byte[][] columnFamilies) {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for(byte[] cfName : columnFamilies) {
      desc.addFamily(new HColumnDescriptor(cfName));
    }
    try
    {
      HBaseAdmin admin = new HBaseAdmin(conf);
      
      // create a table a pre-splits regions.
      // The number of splits is set as:
      //    region servers * regions per region server).
      int numberOfServers = admin.getClusterStatus().getServers();
      int totalNumberOfRegions = numberOfServers * DEFAULT_REGIONS_PER_SERVER;
      byte[][] splits =splitKeysMD5(totalNumberOfRegions);
      admin.createTable(desc, splits);
    }
    catch(MasterNotRunningException e) {
      LOG.error("Master not running.");
      e.printStackTrace();
    }
    catch(TableExistsException e) {
      LOG.info("Table already exists.");
    }
    catch (IOException e)
    {
      LOG.error("IO Exception.");
      e.printStackTrace();
    }
  }

  public static HServerAddress getMetaRS(HBaseConfiguration conf) throws IOException {
    HTable table = new HTable(conf, HConstants.META_TABLE_NAME);
    HRegionLocation hloc = table.getRegionLocation(Bytes.toBytes(""));
    return hloc.getServerAddress();
  }
  
  public static HBaseConfiguration getHBaseConfFromZkNode(String zkNodeName) {
    Configuration c = new Configuration();
    c.set("hbase.zookeeper.quorum", zkNodeName);
    return new HBaseConfiguration(c);
  }

  /**
   * Creates splits for MD5 hashing.
   * @param numberOfSplits
   * @return Byte array of size (numberOfSplits-1) corresponding to the
   * boundaries between splits.
   */
  public static byte[][] splitKeysMD5(int numberOfSplits) {
    BigInteger max = new BigInteger(MAXMD5, 16);
    BigInteger[] bigIntegerSplits = split(max, numberOfSplits);
    byte[][] byteSplits = convertToBytes(bigIntegerSplits);
    return byteSplits;
  }

  /**
   * Splits the given BigInteger into numberOfSplits parts
   * @param maxValue
   * @param numberOfSplits
   * @return array of BigInteger which is of size (numberOfSplits-1)
   */
  private static BigInteger[] split(BigInteger maxValue, int numberOfSplits) {
    BigInteger[] splits = new BigInteger[numberOfSplits-1];
    BigInteger sizeOfEachSplit = maxValue.divide(BigInteger.
        valueOf(numberOfSplits));
    for (int i = 1; i < numberOfSplits; i++) {
      splits[i-1] = sizeOfEachSplit.multiply(BigInteger.valueOf(i));
    }
    return splits;
  }

  /**
   * Returns the bytes corresponding to the BigInteger
   * @param bigInteger
   * @return byte corresponding to input BigInteger
   */
  private static byte[] convertToByte(BigInteger bigInteger) {
    String bigIntegerString = bigInteger.toString(16);
    bigIntegerString = StringUtils.leftPad(bigIntegerString,
        rowComparisonLength, '0');
    return Bytes.toBytes(bigIntegerString);
  }

  /**
   * Returns an array of bytes corresponding to an array of BigIntegers
   * @param bigIntegers
   * @return bytes corresponding to the bigIntegers
   */
  private static byte[][] convertToBytes(BigInteger[] bigIntegers) {
    byte[][] returnBytes = new byte[bigIntegers.length][];
    for (int i = 0; i < bigIntegers.length; i++) {
      returnBytes[i] = convertToByte(bigIntegers[i]);
    }
    return returnBytes;
  }
}
