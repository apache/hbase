/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Factory for creating {@link SpaceQuotaSnapshotNotifier} implementations. Implementations
 * must have a no-args constructor.
 */
@InterfaceAudience.Private
public class SpaceQuotaSnapshotNotifierFactory {
  private static final SpaceQuotaSnapshotNotifierFactory INSTANCE =
      new SpaceQuotaSnapshotNotifierFactory();

  public static final String SNAPSHOT_NOTIFIER_KEY = "hbase.master.quota.snapshot.notifier.impl";
  public static final Class<? extends SpaceQuotaSnapshotNotifier> SNAPSHOT_NOTIFIER_DEFAULT =
      TableSpaceQuotaSnapshotNotifier.class;

  // Private
  private SpaceQuotaSnapshotNotifierFactory() {}

  public static SpaceQuotaSnapshotNotifierFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Instantiates the {@link SpaceQuotaSnapshotNotifier} implementation as defined in the
   * configuration provided.
   *
   * @param conf Configuration object
   * @return The SpaceQuotaSnapshotNotifier implementation
   * @throws IllegalArgumentException if the class could not be instantiated
   */
  public SpaceQuotaSnapshotNotifier create(Configuration conf) {
    Class<? extends SpaceQuotaSnapshotNotifier> clz = Objects.requireNonNull(conf)
        .getClass(SNAPSHOT_NOTIFIER_KEY, SNAPSHOT_NOTIFIER_DEFAULT,
            SpaceQuotaSnapshotNotifier.class);
    try {
      return clz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to instantiate the implementation", e);
    }
  }
}
