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
package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class SystemTableIsolationCandidateGenerator extends TableIsolationCandidateGenerator {

  static SystemTableIsolationCandidateGenerator INSTANCE =
    new SystemTableIsolationCandidateGenerator();

  private boolean isolateMeta = false;

  private SystemTableIsolationCandidateGenerator() {
  }

  void setIsolateMeta(boolean isolateMeta) {
    this.isolateMeta = isolateMeta;
  }

  @Override
  boolean shouldBeIsolated(RegionInfo regionInfo) {
    if (isolateMeta) {
      return regionInfo.getTable().isSystemTable() && !regionInfo.isMetaRegion();
    } else {
      return regionInfo.getTable().isSystemTable();
    }
  }

}
