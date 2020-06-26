/**
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

import java.util.Collection;

import org.apache.hadoop.conf.Configured;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A flush policy determines the stores that need to be flushed when flushing a region.
 */
@InterfaceAudience.Private
public abstract class FlushPolicy extends Configured {

  /**
   * The region configured for this flush policy.
   */
  protected HRegion region;

  /**
   * Upon construction, this method will be called with the region to be governed. It will be called
   * once and only once.
   */
  protected void configureForRegion(HRegion region) {
    this.region = region;
  }

  /**
   * @return the stores need to be flushed.
   */
  public abstract Collection<HStore> selectStoresToFlush();
}
