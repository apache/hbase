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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ReplicationTracker} implementation which polls the region servers list periodically from
 * master.
 * <p/>
 * FIXME: The current implementation is not enough to make the behavior always correct as the
 * {@link Admin#getRegionServers()} call can not tell if there is a change comparing to the previous
 * call. Need to introduce a new Admin method to make this class correct.
 */
@InterfaceAudience.Private
class MasterReplicationTracker extends ReplicationTrackerBase {

  private static final Logger LOG = LoggerFactory.getLogger(MasterReplicationTracker.class);

  static final String REFRESH_INTERVAL_SECONDS =
    "hbase.replication.tracker.master.refresh.interval.secs";

  // default to refresh every 5 seconds
  static final int REFRESH_INTERVAL_SECONDS_DEFAULT = 5;

  private final ChoreService choreService;

  private final ScheduledChore chore;

  private final Admin admin;

  private volatile Set<ServerName> regionServers;

  MasterReplicationTracker(ReplicationTrackerParams params) {
    try {
      this.admin = params.connection().getAdmin();
    } catch (IOException e) {
      // should not happen
      throw new AssertionError(e);
    }
    this.choreService = params.choreService();
    int refreshIntervalSecs =
      params.conf().getInt(REFRESH_INTERVAL_SECONDS, REFRESH_INTERVAL_SECONDS_DEFAULT);
    this.chore = new ScheduledChore(getClass().getSimpleName(), params.stopptable(),
      refreshIntervalSecs, 0, TimeUnit.SECONDS) {

      @Override
      protected void chore() {
        try {
          refresh();
        } catch (IOException e) {
          LOG.warn("failed to refresh region server list for replication", e);
        }
      }
    };
  }

  private Set<ServerName> reload() throws IOException {
    return Collections.unmodifiableSet(new HashSet<>(admin.getRegionServers()));

  }

  private void refresh() throws IOException {
    Set<ServerName> newRegionServers = reload();
    Set<ServerName> oldRegionServers = regionServers;
    this.regionServers = newRegionServers;
    if (!newRegionServers.equals(oldRegionServers)) {
      notifyListeners(newRegionServers);
    }
  }

  @Override
  protected Set<ServerName> internalLoadLiveRegionServersAndInitializeListeners()
    throws IOException {
    Set<ServerName> newRegionServers = reload();
    this.regionServers = newRegionServers;
    choreService.scheduleChore(chore);
    return newRegionServers;
  }
}
