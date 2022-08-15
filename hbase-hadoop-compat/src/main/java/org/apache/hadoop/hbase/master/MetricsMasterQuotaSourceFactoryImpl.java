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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Factory to create MetricsMasterQuotaSource instances when given a MetricsMasterWrapper.
 */
@InterfaceAudience.Private
public class MetricsMasterQuotaSourceFactoryImpl implements MetricsMasterQuotaSourceFactory {

  private MetricsMasterQuotaSource quotaSource;

  @Override
  public synchronized MetricsMasterQuotaSource create(MetricsMasterWrapper masterWrapper) {
    if (quotaSource == null) {
      quotaSource = new MetricsMasterQuotaSourceImpl(masterWrapper);
    }
    return quotaSource;
  }
}
