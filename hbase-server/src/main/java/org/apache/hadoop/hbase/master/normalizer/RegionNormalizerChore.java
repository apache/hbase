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
package org.apache.hadoop.hbase.master.normalizer;

import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.master.HMaster;

import java.io.IOException;

/**
 * Chore that will call {@link org.apache.hadoop.hbase.master.HMaster#normalizeRegions()}
 * when needed.
 */
@InterfaceAudience.Private
public class RegionNormalizerChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(RegionNormalizerChore.class);

  private final HMaster master;

  public RegionNormalizerChore(HMaster master) {
    super(master.getServerName() + "-RegionNormalizerChore", master,
      master.getConfiguration().getInt("hbase.normalizer.period", 300000));
    this.master = master;
  }

  @Override
  protected void chore() {
    try {
      master.normalizeRegions();
    } catch (IOException e) {
      LOG.error("Failed to normalize regions.", e);
    }
  }
}
