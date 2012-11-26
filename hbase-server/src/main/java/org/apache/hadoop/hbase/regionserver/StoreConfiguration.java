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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * The class that contains shared information about various knobs of a Store/HStore object.
 * Unlike the configuration objects that merely return the XML values, the implementations
 * should return ready-to-use applicable values for corresponding calls, after all the
 * parsing/validation/adjustment for other considerations, so that we don't have to repeat
 * this logic in multiple places.
 * TODO: move methods and logic here as necessary.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface StoreConfiguration {
  /**
   * Gets the cf-specific major compaction period.
   */
  public Long getMajorCompactionPeriod();


  /**
   * Gets the Memstore flush size for the region that this store works with.
   */
  public long getMemstoreFlushSize();

  /**
   * Gets the cf-specific time-to-live for store files.
   */
  public long getStoreFileTtl();
}
