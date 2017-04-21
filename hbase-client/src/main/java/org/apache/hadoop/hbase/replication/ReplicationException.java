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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.HBaseException;

/**
 * An HBase Replication exception. This exception is client facing and is thrown from all
 * replication interfaces that deal with the manipulation of replication state. This exception could
 * be thrown for a number of different reasons including loss of connection to the underlying data
 * store, loss of connection to a peer cluster or errors during deserialization of replication data.
 */
@InterfaceAudience.Public
public class ReplicationException extends HBaseException {

  private static final long serialVersionUID = -8885598603988198062L;

  public ReplicationException() {
    super();
  }

  public ReplicationException(final String message) {
    super(message);
  }

  public ReplicationException(final String message, final Throwable t) {
    super(message, t);
  }

  public ReplicationException(final Throwable t) {
    super(t);
  }
}
