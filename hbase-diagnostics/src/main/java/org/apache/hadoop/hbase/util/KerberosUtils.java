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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
/**
 * Utility class for Kerberos authentication.
 */
public class KerberosUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosUtils.class);

  /**
   * Logs in a user using Kerberos keytab and returns the UserGroupInformation (UGI) instance.
   * @param conf     the configuration object
   * @param username the username for which the keytab file and principal are configured.
   * @return the UserGroupInformation instance for the logged-in user.
   * @throws IOException If an I/O error occurs during login.
   */
  public static UserGroupInformation loginAndReturnUGI(Configuration conf, String username)
    throws IOException {
    String hostname = InetAddress.getLocalHost().getHostName();
    String keyTabFileConfKey = "hbase." + username + ".keytab.file";
    String keyTabFileLocation = conf.get(keyTabFileConfKey);
    String principalConfKey = "hbase." + username + ".kerberos.principal";
    String principal = org.apache.hadoop.security.SecurityUtil
      .getServerPrincipal(conf.get(principalConfKey), hostname);
    if (keyTabFileLocation == null || principal == null) {
      LOG.warn(
        "Principal or key tab file null for : " + principalConfKey + ", " + keyTabFileConfKey);
    }
    UserGroupInformation ugi =
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTabFileLocation);
    return ugi;
  }
}
