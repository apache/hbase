/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface that encapsulates optionally sending a Region's size to the master.
 */
@InterfaceAudience.Private
public interface RegionSize extends HeapSize {

  /**
   * Updates the size of the Region.
   *
   * @param newSize the new size of the Region
   * @return {@code this}
   */
  RegionSize setSize(long newSize);

  /**
   * Atomically adds the provided {@code delta} to the region size.
   *
   * @param delta The change in size in bytes of the region.
   * @return {@code this}
   */
  RegionSize incrementSize(long delta);

  /**
   * Returns the size of the region.
   *
   * @return The size in bytes.
   */
  long getSize();
}