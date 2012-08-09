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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Defines a protocol to perform multi row transactions.
 * See {@link MultiRowMutationEndpoint} for the implementation.
 * </br>
 * See
 * {@link HRegion#mutateRowsWithLocks(java.util.Collection, java.util.Collection)}
 * for details and limitations.
 * </br>
 * Example:
 * <code><pre>
 * List<Mutation> mutations = ...;
 * Put p1 = new Put(row1);
 * Put p2 = new Put(row2);
 * ...
 * mutations.add(p1);
 * mutations.add(p2);
 * MultiRowMutationProtocol mrOp = t.coprocessorProxy(
 *   MultiRowMutationProtocol.class, row1);
 * mrOp.mutateRows(mutations);
 * </pre></code>
 */
public interface MultiRowMutationProtocol extends CoprocessorProtocol {
  public void mutateRows(List<Mutation> mutations) throws IOException;
}
