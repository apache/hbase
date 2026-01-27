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
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.Service;

@CoreCoprocessor
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class EndpointReadOnlyController
  implements EndpointObserver, RegionCoprocessor, ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(EndpointReadOnlyController.class);
  private volatile boolean globalReadOnlyEnabled;

  private void internalReadOnlyGuard() throws DoNotRetryIOException {
    if (this.globalReadOnlyEnabled) {
      throw new DoNotRetryIOException("Operation not allowed in Read-Only Mode");
    }
  }

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    this.globalReadOnlyEnabled =
      env.getConfiguration().getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
        HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
  }

  @Override
  public void stop(CoprocessorEnvironment env) {
  }

  @Override
  public Optional<EndpointObserver> getEndpointObserver() {
    return Optional.of(this);
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    boolean maybeUpdatedConfValue = conf.getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
      HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
    this.globalReadOnlyEnabled = maybeUpdatedConfValue;
    LOG.info("Config {} has been dynamically changed to {}.",
      HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY, this.globalReadOnlyEnabled);
  }

  @Override
  public Message preEndpointInvocation(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    Service service, String methodName, Message request) throws IOException {
    internalReadOnlyGuard();
    return EndpointObserver.super.preEndpointInvocation(ctx, service, methodName, request);
  }

}
