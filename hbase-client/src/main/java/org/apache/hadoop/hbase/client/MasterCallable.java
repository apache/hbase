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

package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;

/**
 * A RetryingCallable for master operations.
 * @param <V> return type
 */
abstract class MasterCallable<V> implements RetryingCallable<V>, Closeable {
  protected HConnection connection;
  protected MasterKeepAliveConnection master;

  public MasterCallable(final HConnection connection) {
    this.connection = connection;
  }

  @Override
  public void prepare(boolean reload) throws IOException {
    this.master = this.connection.getKeepAliveMasterService();
  }

  @Override
  public void close() throws IOException {
    // The above prepare could fail but this would still be called though masterAdmin is null
    if (this.master != null) this.master.close();
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
  }

  @Override
  public String getExceptionMessageAdditionalDetail() {
    return "";
  }

  @Override
  public long sleep(long pause, int tries) {
    return ConnectionUtils.getPauseTime(pause, tries);
  }
}