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

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.regionserver.wal.HLog;

@InterfaceAudience.Private
class MetaLogRoller extends LogRoller {
  public MetaLogRoller(Server server, RegionServerServices services) {
    super(server, services);
  }
  @Override
  protected HLog getWAL() throws IOException {
    //The argument to getWAL below could either be HRegionInfo.FIRST_META_REGIONINFO or
    //HRegionInfo.ROOT_REGIONINFO. Both these share the same WAL.
    return services.getWAL(HRegionInfo.FIRST_META_REGIONINFO);
  }
}
