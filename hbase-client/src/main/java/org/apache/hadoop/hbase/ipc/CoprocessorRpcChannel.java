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

import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.RpcChannel;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Base interface which provides clients with an RPC connection to
 * call coprocessor endpoint {@link com.google.protobuf.Service}s.
 * Note that clients should not use this class directly, except through
 * {@link org.apache.hadoop.hbase.client.Table#coprocessorService(byte[])}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CoprocessorRpcChannel extends RpcChannel, BlockingRpcChannel {}
// This Interface is part of our public, client-facing API!!!
// This belongs in client package but it is exposed in our public API so we cannot relocate.