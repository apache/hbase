/*
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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;

/**
 * A source for a replication stream has to expose this service.
 * This service allows an application to hook into the
 * regionserver and watch for new transactions.
 */
@InterfaceAudience.Private
public interface ReplicationSourceService extends ReplicationService {
  /**
   * Returns a WALObserver for the service. This is needed to 
   * observe log rolls and log archival events.
   */
  WALActionsListener getWALActionsListener();
}
