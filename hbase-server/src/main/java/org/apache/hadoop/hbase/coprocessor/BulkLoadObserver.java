/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Coprocessors implement this interface to observe and mediate bulk load operations.
 * <br><br>
 *
 * <h3>Exception Handling</h3>
 * For all functions, exception handling is done as follows:
 * <ul>
 *   <li>Exceptions of type {@link IOException} are reported back to client.</li>
 *   <li>For any other kind of exception:
 *     <ul>
 *       <li>If the configuration {@link CoprocessorHost#ABORT_ON_ERROR_KEY} is set to true, then
 *         the server aborts.</li>
 *       <li>Otherwise, coprocessor is removed from the server and
 *         {@link org.apache.hadoop.hbase.DoNotRetryIOException} is returned to the client.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface BulkLoadObserver {
    /**
      * Called as part of SecureBulkLoadEndpoint.prepareBulkLoad() RPC call.
      * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
      * If you need to get the region or table name, get it from the
      * <code>ctx</code> as follows: <code>code>ctx.getEnvironment().getRegion()</code>. Use
      * getRegionInfo to fetch the encodedName and use getTableDescriptor() to get the tableName.
      * @param ctx the environment to interact with the framework and master
      */
    default void prePrepareBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx)
    throws IOException {}

    /**
      * Called as part of SecureBulkLoadEndpoint.cleanupBulkLoad() RPC call.
      * It can't bypass the default action, e.g., ctx.bypass() won't have effect.
      * If you need to get the region or table name, get it from the
      * <code>ctx</code> as follows: <code>code>ctx.getEnvironment().getRegion()</code>. Use
      * getRegionInfo to fetch the encodedName and use getTableDescriptor() to get the tableName.
      * @param ctx the environment to interact with the framework and master
      */
    default void preCleanupBulkLoad(ObserverContext<RegionCoprocessorEnvironment> ctx)
    throws IOException {}
}
