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
package org.apache.hadoop.hbase.master.http.hbck.model;

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class HbckOverlapRegions {
  private final HbckRegionDetails overlapRegion1;
  private final HbckRegionDetails overlapRegion2;

  public HbckOverlapRegions(HbckRegionDetails overlapRegion1, HbckRegionDetails overlapRegion2) {
    this.overlapRegion1 = overlapRegion1;
    this.overlapRegion2 = overlapRegion2;
  }

  public HbckRegionDetails getOverlapRegion1() {
    return overlapRegion1;
  }

  public HbckRegionDetails getOverlapRegion2() {
    return overlapRegion2;
  }
}
