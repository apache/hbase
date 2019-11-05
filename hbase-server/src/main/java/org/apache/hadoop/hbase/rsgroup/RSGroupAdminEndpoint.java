/**
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
package org.apache.hadoop.hbase.rsgroup;

import com.google.protobuf.Service;
import java.io.IOException;
import java.util.Collections;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * @deprecated Keep it here only for compatibility with old client, all the logics have been moved
 *             into core of HBase.
 */
@Deprecated
@CoreCoprocessor
@InterfaceAudience.Private
public class RSGroupAdminEndpoint implements MasterCoprocessor {
  // Only instance of RSGroupInfoManager. RSGroup aware load balancers ask for this instance on
  // their setup.
  private MasterServices master;
  private RSGroupAdminServiceImpl groupAdminService = new RSGroupAdminServiceImpl();

  @Override
  public void start(CoprocessorEnvironment env) throws IOException {
    if (!(env instanceof HasMasterServices)) {
      throw new IOException("Does not implement HMasterServices");
    }
    master = ((HasMasterServices) env).getMasterServices();
    groupAdminService.initialize(master);
  }

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(groupAdminService);
  }

  RSGroupInfoManager getGroupInfoManager() {
    return master.getRSGroupInfoManager();
  }

}
