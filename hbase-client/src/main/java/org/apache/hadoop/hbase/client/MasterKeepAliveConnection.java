/**
 * Copyright The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.protobuf.generated.MasterProtos;

/**
 * A KeepAlive connection is not physically closed immediately after the close,
 *  but rather kept alive for a few minutes. It makes sense only if it is shared.
 *
 * <p>This interface is implemented on a stub. It allows to have a #close function in a master
 * client.
 *
 * <p>This class is intended to be used internally by HBase classes that need to make invocations
 * against the master on the MasterProtos.MasterService.BlockingInterface; but not by
 * final user code. Hence it's package protected.
 */
interface MasterKeepAliveConnection
extends MasterProtos.MasterService.BlockingInterface {
  // Do this instead of implement Closeable because closeable returning IOE is PITA.
  void close();
}
