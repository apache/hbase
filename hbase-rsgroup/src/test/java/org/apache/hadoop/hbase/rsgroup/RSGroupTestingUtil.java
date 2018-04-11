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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.net.Address;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class RSGroupTestingUtil {
  private static final Logger LOG = LoggerFactory.getLogger(RSGroupTestingUtil.class);

  private RSGroupTestingUtil() {
  }

  public static RSGroupInfo addRSGroup(final RSGroupAdmin rsGroupAdmin, String groupName,
      int groupRSCount) throws IOException {
    RSGroupInfo defaultInfo = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    assertTrue(defaultInfo != null);
    assertTrue(defaultInfo.getServers().size() >= groupRSCount);
    rsGroupAdmin.addRSGroup(groupName);

    Set<Address> set = new HashSet<>();
    for(Address server: defaultInfo.getServers()) {
      if(set.size() == groupRSCount) {
        break;
      }
      set.add(server);
    }
    rsGroupAdmin.moveServers(set, groupName);
    RSGroupInfo result = rsGroupAdmin.getRSGroupInfo(groupName);
    assertTrue(result.getServers().size() >= groupRSCount);
    return result;
  }

}
