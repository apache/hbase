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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class AccessStatsRecorderConstants {
  // config key to specify iteration period for recording periodic region counters
  public final static String REGION_STATS_RECORDER_IN_MINUTES_KEY =
      "hbase.region.stats.duration.minutes";

  public final static String ACCESS_STATS_TABLE_KEY = "hbase.region.stats.table";

  public final static String ACCESS_STATS_TABLE_NAME_DEFAULT = "ACCESS_STATS";

  public final static byte[] STATS_COLUMN_FAMILY = Bytes.toBytes("S");
  public final static int STATS_COLUMN_FAMILY_TTL = 30*24*60*60; // 30 days
  public final static int STATS_COLUMN_FAMILY_MAXVERSIONS = 1; // 1 version is sufficient
  
  public final static byte[] STATS_COLUMN_QUALIFIER_START_KEY = Bytes.toBytes("SK");
  public final static byte[] STATS_COLUMN_QUALIFIER_END_KEY = Bytes.toBytes("EK");

  public final static byte[] UID_COLUMN_FAMILY = Bytes.toBytes("U");;
  public final static byte[] UID_COLUMN_QUALIFIER = { (byte) 1 };

  public final static byte[] UID_AUTO_INCREMENT_COLUMN_FAMILY = Bytes.toBytes("UA");;
  
  public final static byte[] UID_AUTO_INCREMENT_ROW_KEY =
      Bytes.toBytes("ROW_FOR_AUTOINCREMENT_COLUMNS");
 
  public final static byte[] UID_AUTO_INCREMENT_COLUMN_QUALIFIER_1 = { (byte) 10 };
  public final static byte[] UID_AUTO_INCREMENT_COLUMN_QUALIFIER_2 = { (byte) 20 };
  public final static byte[] UID_AUTO_INCREMENT_COLUMN_QUALIFIER_3 = { (byte) 30 };
  public final static byte[] UID_AUTO_INCREMENT_COLUMN_QUALIFIER_4 = { (byte) 40 };
  
  public final static int MAX_UID_BYTE_ARRAY_LENGTH_SUPPORTED = 4;
  
  public final static int NUM_RETRIES_FOR_GENERATING_UID = 5;
  public final static long WAIT_TIME_BEFORE_RETRY_FOR_GENERATING_UID = 30*1000;

}