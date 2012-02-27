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
package org.apache.hadoop.hbase.loadtest;

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
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtils {
  private static final Log LOG = LogFactory.getLog(HBaseUtils.class);

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
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return table;
  }

  public static boolean createTableIfNotExists(Configuration conf,
      byte[] tableName, ColumnFamilyProperties[] familyProperties,
      int regionsPerServer) {
    // LOG.info("Creating table if not exists: "+Bytes.toString(tableName));
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (ColumnFamilyProperties familyProperty : familyProperties) {
      // LOG.info("CF Name: "+familyProperty.familyName);
      String bloomType = familyProperty.bloomType;
      if (bloomType == null) {
        bloomType = "NONE";
      }
      String compressionType = familyProperty.compressionType;
      if (compressionType == null) {
        compressionType = "NONE";
      }
      desc.addFamily(new HColumnDescriptor(familyProperty.familyName)
          .setMaxVersions(familyProperty.maxVersions)
          .setCompressionType(compressionType)
          .setBloomFilterType(bloomType));
    }
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      int numberOfServers = admin.getClusterStatus().getServers();
      int totalNumberOfRegions = numberOfServers * regionsPerServer;
      if (totalNumberOfRegions == 0) {
        admin.createTable(desc);
      } else {
        byte[][] splits = RegionSplitter.splitKeys(HashingSchemes.MD5,
            totalNumberOfRegions);
        admin.createTable(desc, splits);
      }
      return true;
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running.");
      e.printStackTrace();
      return false;
    } catch (TableExistsException e) {
      LOG.info("Table already exists.");
      return true;
    } catch (IOException e) {
      LOG.error("IO Exception.");
      e.printStackTrace();
      return false;
    } catch (Exception e) {
      LOG.error("Exception.");
      e.printStackTrace();
      return false;
    }
  }

  public static boolean deleteTable(Configuration conf, byte[] tableName) {
    // LOG.info("Deleting table: "+Bytes.toString(tableName) + "....");
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
      return true;
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running.");
      e.printStackTrace();
      return false;
    } catch (TableNotFoundException e) {
      LOG.info("Table does not exist.");
      return false;
    } catch (TableNotDisabledException e) {
      LOG.info("Table not disabled.");
      return false;
    } catch (IOException e) {
      LOG.error("IO Exception.");
      e.printStackTrace();
      return false;
    } catch (Exception e) {
      LOG.error("Exception.");
      e.printStackTrace();
      return false;
    }
  }

  public static boolean flushTable(Configuration conf, byte[] tableName) {
    // LOG.info("Flushing table: "+Bytes.toString(tableName) + "....");
    try {
      HBaseAdmin admin = new HBaseAdmin(conf);
      admin.flush(tableName);
      return true;
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running.");
      e.printStackTrace();
      return false;
    } catch (TableNotFoundException e) {
      LOG.info("Table not found.");
      return false;
    } catch (IOException e) {
      LOG.error("IO Exception.");
      e.printStackTrace();
      return false;
    } catch (Exception e) {
      LOG.error("Exception.");
      e.printStackTrace();
      return false;
    }
  }

  public static HServerAddress getMetaRS(HBaseConfiguration conf)
      throws IOException {
    HTable table = new HTable(conf, HConstants.META_TABLE_NAME);
    HRegionLocation hloc = table.getRegionLocation(Bytes.toBytes(""));
    return hloc.getServerAddress();
  }

  public static Configuration getHBaseConfFromZkNode(String zkNodeName) {
    Configuration c = HBaseConfiguration.create();
    if (zkNodeName != null) {
      c.set("hbase.zookeeper.quorum", zkNodeName);
    }
    return c;
  }
}
