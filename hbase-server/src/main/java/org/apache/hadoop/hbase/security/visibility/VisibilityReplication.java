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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A RegionServerObserver impl that provides the custom
 * VisibilityReplicationEndpoint. This class should be configured as the
 * 'hbase.coprocessor.regionserver.classes' for the visibility tags to be
 * replicated as string.  The value for the configuration should be
 * 'org.apache.hadoop.hbase.security.visibility.VisibilityController$VisibilityReplication'.
 */
@InterfaceAudience.Private
public class VisibilityReplication implements RegionServerCoprocessor, RegionServerObserver {
  private Configuration conf;
  private VisibilityLabelService visibilityLabelService;

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    this.conf = env.getConfiguration();
    visibilityLabelService = VisibilityLabelServiceManager.getInstance()
        .getVisibilityLabelService(this.conf);
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  @Override public Optional<RegionServerObserver> getRegionServerObserver() {
    return Optional.of(this);
  }

  @Override
  public ReplicationEndpoint postCreateReplicationEndPoint(
      ObserverContext<RegionServerCoprocessorEnvironment> ctx, ReplicationEndpoint endpoint) {
    return new VisibilityReplicationEndpoint(endpoint, visibilityLabelService);
  }
}
