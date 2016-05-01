/**
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

import java.io.IOException;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;

/**
 * Implementations call a RegionServer and implement {@link #call(int)}.
 * Passed to a {@link RpcRetryingCaller} so we retry on fail.
 * TODO: this class is actually tied to one region, because most of the paths make use of
 *       the regioninfo part of location when building requests. The only reason it works for
 *       multi-region requests (e.g. batch) is that they happen to not use the region parts.
 *       This could be done cleaner (e.g. having a generic parameter and 2 derived classes,
 *       RegionCallable and actual RegionServerCallable with ServerName.
 * @param <T> the class that the ServerCallable handles
 */
@InterfaceAudience.Private
public abstract class RegionServerCallable<T> extends AbstractRegionServerCallable<T> implements
    RetryingCallable<T> {

  private ClientService.BlockingInterface stub;

  /**
   * @param connection Connection to use.
   * @param tableName Table name to which <code>row</code> belongs.
   * @param row The row we want in <code>tableName</code>.
   */
  public RegionServerCallable(Connection connection, TableName tableName, byte [] row) {
    super(connection, tableName, row);
  }

  void setClientByServiceName(ServerName service) throws IOException {
    this.setStub(getConnection().getClient(service));
  }

  /**
   * @return Client Rpc protobuf communication stub
   */
  protected ClientService.BlockingInterface getStub() {
    return this.stub;
  }

  /**
   * Set the client protobuf communication stub
   * @param stub to set
   */
  void setStub(final ClientService.BlockingInterface stub) {
    this.stub = stub;
  }
}
