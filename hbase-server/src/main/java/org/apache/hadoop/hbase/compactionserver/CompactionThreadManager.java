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
package org.apache.hadoop.hbase.compactionserver;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class CompactionThreadManager {
  private static Logger LOG = LoggerFactory.getLogger(CompactionThreadManager.class);

  private final Configuration conf;
  private final ConcurrentMap<ServerName, AsyncRegionServerAdmin> rsAdmins =
      new ConcurrentHashMap<>();
  private final HCompactionServer server;

  public CompactionThreadManager(final Configuration conf, HCompactionServer server) {
    TraceUtil.initTracer(conf);
    this.conf = conf;
    this.server = server;
  }

  private AsyncRegionServerAdmin getRsAdmin(final ServerName sn) throws IOException {
    AsyncRegionServerAdmin admin = this.rsAdmins.get(sn);
    if (admin == null) {
      LOG.debug("New RS admin connection to {}", sn);
      admin = this.server.getAsyncClusterConnection().getRegionServerAdmin(sn);
      this.rsAdmins.put(sn, admin);
    }
    return admin;
  }

  public void requestCompaction() {
  }

}
