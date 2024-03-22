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

import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class HbckInconsistentRegions {
  private final String inconsistentRegionId;
  private final HbckServerName serverNameInMeta;
  private final List<HbckServerName> listOfServerName;

  public HbckInconsistentRegions(String inconsistentRegionId, HbckServerName serverNameInMeta,
    List<HbckServerName> listOfServerName) {
    this.inconsistentRegionId = inconsistentRegionId;
    this.serverNameInMeta = serverNameInMeta;
    this.listOfServerName = listOfServerName;
  }

  public String getInconsistentRegionId() {
    return inconsistentRegionId;
  }

  public HbckServerName getServerNameInMeta() {
    return serverNameInMeta;
  }

  public List<HbckServerName> getListOfServerName() {
    return listOfServerName;
  }
}
