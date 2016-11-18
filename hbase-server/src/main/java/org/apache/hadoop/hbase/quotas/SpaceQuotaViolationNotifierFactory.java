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
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Factory for creating {@link SpaceQuotaViolationNotifier} implementations. Implementations
 * must have a no-args constructor.
 */
@InterfaceAudience.Private
public class SpaceQuotaViolationNotifierFactory {
  private static final SpaceQuotaViolationNotifierFactory INSTANCE =
      new SpaceQuotaViolationNotifierFactory();

  public static final String VIOLATION_NOTIFIER_KEY = "hbase.master.quota.violation.notifier.impl";
  public static final Class<? extends SpaceQuotaViolationNotifier> VIOLATION_NOTIFIER_DEFAULT =
      SpaceQuotaViolationNotifierForTest.class;

  // Private
  private SpaceQuotaViolationNotifierFactory() {}

  public static SpaceQuotaViolationNotifierFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Instantiates the {@link SpaceQuotaViolationNotifier} implementation as defined in the
   * configuration provided.
   *
   * @param conf Configuration object
   * @return The SpaceQuotaViolationNotifier implementation
   * @throws IllegalArgumentException if the class could not be instantiated
   */
  public SpaceQuotaViolationNotifier create(Configuration conf) {
    Class<? extends SpaceQuotaViolationNotifier> clz = Objects.requireNonNull(conf)
        .getClass(VIOLATION_NOTIFIER_KEY, VIOLATION_NOTIFIER_DEFAULT,
            SpaceQuotaViolationNotifier.class);
    try {
      return clz.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException("Failed to instantiate the implementation", e);
    }
  }
}
