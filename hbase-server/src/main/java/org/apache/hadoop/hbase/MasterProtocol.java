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

package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;

/**
 * Functions implemented by all the master protocols: e.g. {@link MasterAdminProtocol}
 * and {@link MasterMonitorProtocol}. Currently, the only shared method
 * {@link #isMasterRunning(com.google.protobuf.RpcController, org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest)}
 * which is used on connection setup to check if the master has been stopped.
 */
public interface MasterProtocol extends IpcProtocol, MasterService.BlockingInterface {}