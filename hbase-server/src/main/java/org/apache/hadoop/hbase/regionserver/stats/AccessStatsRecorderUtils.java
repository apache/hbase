/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.stats;

import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.mortbay.log.Log;

@InterfaceAudience.Private
public class AccessStatsRecorderUtils {
  
 private Configuration conf = null;
  
  private static volatile AccessStatsRecorderUtils instance;
  private static Object mutex = new Object();

  private AccessStatsRecorderUtils(Configuration configuration) {
    this.conf = configuration;
  }
  
  public static AccessStatsRecorderUtils createInstance(Configuration configuration) {
    AccessStatsRecorderUtils result = instance;
    if (result == null) {
      synchronized (mutex) {
        result = instance;
        if (result == null)
        { 
          instance = result = new AccessStatsRecorderUtils(configuration);
        }
      }
    }
    return result;
  }
  
  public static AccessStatsRecorderUtils getInstance() {
    return instance;
  }

  public int getIterationDuration() {
    int durationInMinutes = conf.getInt(AccessStatsRecorderConstants.REGION_STATS_RECORDER_IN_MINUTES_KEY, 15);
    if (durationInMinutes > 60) {
      durationInMinutes = 60;
    }
    return durationInMinutes;
  }

  public TableName getTableNameToRecordStats() {
    String[] tableNames = conf.getStrings(AccessStatsRecorderConstants.ACCESS_STATS_TABLE_KEY);

    if (tableNames != null && tableNames.length >= 1) return TableName.valueOf(tableNames[0]);

    return TableName.valueOf(AccessStatsRecorderConstants.ACCESS_STATS_TABLE_NAME_DEFAULT);
  }

  public long getNormalizedTime(long epochTime) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(epochTime);
    
    return getNormalizedTimeInternal(calendar);
  }
  
  public long getNormalizedTimeCurrent() {
    Calendar calendar = Calendar.getInstance();
    return getNormalizedTimeInternal(calendar);
  }
  
  private long getNormalizedTimeInternal(Calendar calendar) {
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    int modulo = (int) (calendar.get(Calendar.MINUTE) % getIterationDuration());
    if (modulo > 0) {
      calendar.add(Calendar.MINUTE, -modulo);
    }

    long currentTime = calendar.getTimeInMillis();
    return currentTime;
  }
  
  //supposed to be used only with test or once post cluster deployment.
  public void createTableToStoreStats() throws Exception
  {
    try(Connection connection = ConnectionFactory.createConnection(conf))
    {
      HTableDescriptor tableDescriptor = new HTableDescriptor(getTableNameToRecordStats());
  
      HColumnDescriptor columnDescriptorStats = new HColumnDescriptor(AccessStatsRecorderConstants.STATS_COLUMN_FAMILY);
      columnDescriptorStats.setTimeToLive(AccessStatsRecorderConstants.STATS_COLUMN_FAMILY_TTL);
      columnDescriptorStats.setMaxVersions(AccessStatsRecorderConstants.STATS_COLUMN_FAMILY_MAXVERSIONS);
      
      tableDescriptor.addFamily(columnDescriptorStats);
      
      HColumnDescriptor columnDescriptorUID = new HColumnDescriptor(AccessStatsRecorderConstants.UID_COLUMN_FAMILY);
      columnDescriptorUID.setMaxVersions(AccessStatsRecorderConstants.STATS_COLUMN_FAMILY_MAXVERSIONS);
      
      tableDescriptor.addFamily(columnDescriptorUID);
      
      HColumnDescriptor columnDescriptorAutoIncrement = new HColumnDescriptor(AccessStatsRecorderConstants.UID_AUTO_INCREMENT_COLUMN_FAMILY);
      columnDescriptorAutoIncrement.setMaxVersions(AccessStatsRecorderConstants.STATS_COLUMN_FAMILY_MAXVERSIONS);
      
      tableDescriptor.addFamily(columnDescriptorAutoIncrement);
      
      Admin admin = connection.getAdmin();
      admin.createTable(tableDescriptor);
      admin.close();
      
      Log.info("Table created with name - "+ tableDescriptor.getNameAsString());
    }
  }
}