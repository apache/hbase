/*
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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class was created by moving all load test related code from HFileTestUtil and
 * HBaseTestingUtil as part of refactoring for hbase-diagnostics module creation in HBASE-28432
 */
@InterfaceAudience.Private
public class LoadTestUtil {
  private static final Logger LOG = LoggerFactory.getLogger(LoadTestUtil.class);

  public static final String OPT_DATA_BLOCK_ENCODING_USAGE = "Encoding algorithm (e.g. prefix "
    + "compression) to use for data blocks in the test column family, " + "one of "
    + Arrays.toString(DataBlockEncoding.values()) + ".";
  public static final String OPT_DATA_BLOCK_ENCODING =
    ColumnFamilyDescriptorBuilder.DATA_BLOCK_ENCODING.toLowerCase(Locale.ROOT);

  /**
   * The default number of regions per regionserver when creating a pre-split table.
   */
  private static final int DEFAULT_REGIONS_PER_SERVER = 3;

  /**
   * Creates a pre-split table for load testing. If the table already exists, logs a warning and
   * continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf, TableName tableName,
    byte[] columnFamily, Algorithm compression, DataBlockEncoding dataBlockEncoding)
    throws IOException {
    return createPreSplitLoadTestTable(conf, tableName, columnFamily, compression,
      dataBlockEncoding, DEFAULT_REGIONS_PER_SERVER, 1, Durability.USE_DEFAULT);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists, logs a warning and
   * continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf, TableName tableName,
    byte[] columnFamily, Algorithm compression, DataBlockEncoding dataBlockEncoding,
    int numRegionsPerServer, int regionReplication, Durability durability) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setDurability(durability);
    builder.setRegionReplication(regionReplication);
    ColumnFamilyDescriptorBuilder cfBuilder =
      ColumnFamilyDescriptorBuilder.newBuilder(columnFamily);
    cfBuilder.setDataBlockEncoding(dataBlockEncoding);
    cfBuilder.setCompressionType(compression);
    return createPreSplitLoadTestTable(conf, builder.build(), cfBuilder.build(),
      numRegionsPerServer);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists, logs a warning and
   * continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf, TableName tableName,
    byte[][] columnFamilies, Algorithm compression, DataBlockEncoding dataBlockEncoding,
    int numRegionsPerServer, int regionReplication, Durability durability) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setDurability(durability);
    builder.setRegionReplication(regionReplication);
    ColumnFamilyDescriptor[] hcds = new ColumnFamilyDescriptor[columnFamilies.length];
    for (int i = 0; i < columnFamilies.length; i++) {
      ColumnFamilyDescriptorBuilder cfBuilder =
        ColumnFamilyDescriptorBuilder.newBuilder(columnFamilies[i]);
      cfBuilder.setDataBlockEncoding(dataBlockEncoding);
      cfBuilder.setCompressionType(compression);
      hcds[i] = cfBuilder.build();
    }
    return createPreSplitLoadTestTable(conf, builder.build(), hcds, numRegionsPerServer);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists, logs a warning and
   * continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf, TableDescriptor desc,
    ColumnFamilyDescriptor hcd) throws IOException {
    return createPreSplitLoadTestTable(conf, desc, hcd, DEFAULT_REGIONS_PER_SERVER);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists, logs a warning and
   * continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf, TableDescriptor desc,
    ColumnFamilyDescriptor hcd, int numRegionsPerServer) throws IOException {
    return createPreSplitLoadTestTable(conf, desc, new ColumnFamilyDescriptor[] { hcd },
      numRegionsPerServer);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists, logs a warning and
   * continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf, TableDescriptor desc,
    ColumnFamilyDescriptor[] hcds, int numRegionsPerServer) throws IOException {
    return createPreSplitLoadTestTable(conf, desc, hcds, new RegionSplitter.HexStringSplit(),
      numRegionsPerServer);
  }

  /**
   * Creates a pre-split table for load testing. If the table already exists, logs a warning and
   * continues.
   * @return the number of regions the table was split into
   */
  public static int createPreSplitLoadTestTable(Configuration conf, TableDescriptor td,
    ColumnFamilyDescriptor[] cds, SplitAlgorithm splitter, int numRegionsPerServer)
    throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(td);
    for (ColumnFamilyDescriptor cd : cds) {
      if (!td.hasColumnFamily(cd.getName())) {
        builder.setColumnFamily(cd);
      }
    }
    td = builder.build();
    int totalNumberOfRegions = 0;
    Connection unmanagedConnection = ConnectionFactory.createConnection(conf);
    Admin admin = unmanagedConnection.getAdmin();

    try {
      // create a table a pre-splits regions.
      // The number of splits is set as:
      // region servers * regions per region server).
      int numberOfServers = admin.getRegionServers().size();
      if (numberOfServers == 0) {
        throw new IllegalStateException("No live regionservers");
      }

      totalNumberOfRegions = numberOfServers * numRegionsPerServer;
      LOG.info("Number of live regionservers: " + numberOfServers + ", "
        + "pre-splitting table into " + totalNumberOfRegions + " regions " + "(regions per server: "
        + numRegionsPerServer + ")");

      byte[][] splits = splitter.split(totalNumberOfRegions);

      admin.createTable(td, splits);
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running", e);
      throw new IOException(e);
    } catch (TableExistsException e) {
      LOG.warn("Table " + td.getTableName() + " already exists, continuing");
    } finally {
      admin.close();
      unmanagedConnection.close();
    }
    return totalNumberOfRegions;
  }

}
