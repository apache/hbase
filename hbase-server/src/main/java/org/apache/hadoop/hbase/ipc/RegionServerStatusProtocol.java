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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.security.TokenInfo;
import org.apache.hadoop.hbase.security.KerberosInfo;

/**
 * Protocol that a RegionServer uses to communicate its status to the Master.
 */
@KerberosInfo(
  serverPrincipal = "hbase.master.kerberos.principal")
@TokenInfo("HBASE_AUTH_TOKEN")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface RegionServerStatusProtocol extends
    RegionServerStatusService.BlockingInterface, VersionedProtocol {
  public static final long VERSION = 1L;
}
