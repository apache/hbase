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
package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically reloads the full ACL content of {@code hbase:acl} into the local cache, as a
 * fallback for lost or abandoned incremental updates, for updates written outside the master and
 * for the residual consistency windows of the push pipeline. Failures are logged and retried in the
 * next round: the reload is only a fallback, keeping the current cache is better than stopping.
 */
@InterfaceAudience.Private
public class AclReloadChore extends ScheduledChore {

  public static final String ACL_RELOAD_PERIOD_MSEC_KEY = "hbase.security.acl.reload.period.msec";

  private static final int DEFAULT_ACL_RELOAD_PERIOD_MSEC = 5 * 60 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(AclReloadChore.class);

  private final AclRefresher refresher;

  public AclReloadChore(AclRefresher refresher, Configuration conf, Stoppable stopper) {
    super("AclReloadChore", stopper,
      conf.getInt(ACL_RELOAD_PERIOD_MSEC_KEY, DEFAULT_ACL_RELOAD_PERIOD_MSEC));
    this.refresher = refresher;
  }

  @Override
  protected void chore() {
    try {
      refresher.reloadFromTable();
    } catch (IOException e) {
      LOG.warn("Failed to reload ACLs from {}, keeping the current cache, will retry in {} msec",
        PermissionStorage.ACL_TABLE_NAME, getPeriod(), e);
    }
  }
}
