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

package org.apache.hadoop.hbase.master.procedure;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class MasterProcedureUtil {
  private static final Log LOG = LogFactory.getLog(MasterProcedureUtil.class);

  private MasterProcedureUtil() {}

  public static UserInformation toProtoUserInfo(User user) {
    UserInformation.Builder userInfoPB = UserInformation.newBuilder();
    userInfoPB.setEffectiveUser(user.getName());
    if (user.getUGI().getRealUser() != null) {
      userInfoPB.setRealUser(user.getUGI().getRealUser().getUserName());
    }
    return userInfoPB.build();
  }

  public static User toUserInfo(UserInformation userInfoProto) {
    if (userInfoProto.hasEffectiveUser()) {
      String effectiveUser = userInfoProto.getEffectiveUser();
      if (userInfoProto.hasRealUser()) {
        String realUser = userInfoProto.getRealUser();
        UserGroupInformation realUserUgi = UserGroupInformation.createRemoteUser(realUser);
        return User.create(UserGroupInformation.createProxyUser(effectiveUser, realUserUgi));
      }
      return User.create(UserGroupInformation.createRemoteUser(effectiveUser));
    }
    return null;
  }
}
