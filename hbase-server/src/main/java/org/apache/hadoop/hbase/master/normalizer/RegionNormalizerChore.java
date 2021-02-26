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
package org.apache.hadoop.hbase.master.normalizer;

import java.io.IOException;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.client.NormalizeTableFilterParams;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Chore that will periodically call
 * {@link HMaster#normalizeRegions(NormalizeTableFilterParams, boolean)}.
 */
@InterfaceAudience.Private
class RegionNormalizerChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(RegionNormalizerChore.class);

  private final MasterServices master;

  public RegionNormalizerChore(MasterServices master) {
    super(master.getServerName() + "-RegionNormalizerChore", master,
      master.getConfiguration().getInt("hbase.normalizer.period", 300_000));
    this.master = master;
  }

  @Override
  protected void chore() {
    try {
      master.normalizeRegions(new NormalizeTableFilterParams.Builder().build(), false);
    } catch (IOException e) {
      LOG.error("Failed to normalize regions.", e);
    }
  }
}
