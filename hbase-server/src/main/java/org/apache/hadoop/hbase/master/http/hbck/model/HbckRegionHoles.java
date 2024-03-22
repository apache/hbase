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
public class HbckRegionHoles {
  private final HbckRegionDetails region1;
  private final HbckRegionDetails region2;

  public HbckRegionHoles(HbckRegionDetails regionHole1, HbckRegionDetails regionHole2) {
    this.region1 = regionHole1;
    this.region2 = regionHole2;
  }

  public HbckRegionDetails getRegion1() {
    return region1;
  }

  public HbckRegionDetails getRegion2() {
    return region2;
  }
}
