/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.metrics;


/**
 * JMX interface for exporting RegionServer statistics.  
 */
public interface RegionServerStatisticsMBean {

  /**
   * Count of regions carried by this regionserver
   */
  public int getRegionsNum();

  /*
   * Count of requests to the regionservers since last call to metrics update
   */
  public float getRequestsRate();

  /**
   * Count of stores open on the regionserver.
   */
  public int getStoresNum();

  /**
   * Count of storefiles open on the regionserver.
   */
  public int getStoreFilesNum();

  /**
   * Sum of all the storefile index sizes in this regionserver in MB
   */
  public int getStoreFileIndexSizeMB();

  /**
   * Sum of all the memcache sizes in this regionserver in MB
   */
  public int getMemcacheSizeMB();

}
