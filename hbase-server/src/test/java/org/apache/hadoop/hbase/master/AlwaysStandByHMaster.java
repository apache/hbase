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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

/**
 * An implementation of HMaster that always runs as a stand by and never transitions to active.
 */
public class AlwaysStandByHMaster extends HMaster {
  public AlwaysStandByHMaster(Configuration conf) throws IOException {
    super(conf);
  }

  protected ActiveMasterManager createActiveMasterManager(
      ZKWatcher zk, ServerName sn, org.apache.hadoop.hbase.Server server) {
    return new AlwaysStandByMasterManager(zk, sn, server);
  }
}
