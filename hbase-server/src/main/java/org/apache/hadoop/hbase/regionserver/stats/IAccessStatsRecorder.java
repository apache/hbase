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
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.stats.AccessStats.AccessStatsGranularity;
import org.apache.hadoop.hbase.regionserver.stats.AccessStats.AccessStatsType;
import org.apache.yetus.audience.InterfaceAudience;


/*
 * Interface to store access stats.
 */
@InterfaceAudience.Private
public interface IAccessStatsRecorder extends Closeable{

  /**
   * @param table TableName for which access stats is to be read
   * @param accessStatsType Read Stats or Write Stats
   * @param accessStatsGranularity only REGION level granularity is supported currently
   * @param epochTimeTo latest time till when access stats is required
   * @param numIterations number of access stats records to be returned
   * @return List of AccessStats
   */
  public  List<AccessStats> readAccessStats(TableName table, AccessStatsType accessStatsType, AccessStatsGranularity accessStatsGranularity,
      long epochTimeTo, int numIterations);
  
  
  
  /**
   * @param listAccessStats List of AccessStats to store in DB
   */
  public  void writeAccessStats(List<AccessStats> listAccessStats) ;

}