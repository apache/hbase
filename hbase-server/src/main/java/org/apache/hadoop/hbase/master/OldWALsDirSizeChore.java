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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This chore is used to update the 'oldWALsDirSize' variable in {@link MasterWalManager} through
 * the {@link MasterWalManager#updateOldWALsDirSize()} method.
 */
@InterfaceAudience.Private
public class OldWALsDirSizeChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(OldWALsDirSizeChore.class);

  private final MasterServices master;

  public OldWALsDirSizeChore(MasterServices master) {
    super(master.getServerName() + "-OldWALsDirSizeChore", master,
      master.getConfiguration().getInt(HConstants.HBASE_OLDWAL_DIR_SIZE_UPDATER_PERIOD,
        HConstants.DEFAULT_HBASE_OLDWAL_DIR_SIZE_UPDATER_PERIOD));
    this.master = master;
  }

  @Override
  protected void chore() {
    try {
      this.master.getMasterWalManager().updateOldWALsDirSize();
    } catch (IOException e) {
      LOG.error("Got exception while trying to update the old WALs Directory size counter: "
        + e.getMessage(), e);
    }
  }
}
