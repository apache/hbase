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
package org.apache.hadoop.hbase.security;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;

/**
 * Implementation of secure Hadoop policy provider for mapping
 * protocol interfaces to hbase-policy.xml entries.
 */
@InterfaceAudience.Private
public class HBasePolicyProvider extends PolicyProvider {
  protected final static Service[] services = {
      new Service("security.client.protocol.acl", ClientService.BlockingInterface.class),
      new Service("security.client.protocol.acl", AdminService.BlockingInterface.class),
      new Service("security.admin.protocol.acl", MasterService.BlockingInterface.class),
      new Service("security.masterregion.protocol.acl", RegionServerStatusService.BlockingInterface.class)
  };

  @Override
  public Service[] getServices() {
    return services;
  }

  public static void init(Configuration conf, ServiceAuthorizationManager authManager) {
    // set service-level authorization security policy
    System.setProperty("hadoop.policy.file", "hbase-policy.xml");
    if (conf.getBoolean(ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      authManager.refresh(conf, new HBasePolicyProvider());
      ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    }
  }
}
