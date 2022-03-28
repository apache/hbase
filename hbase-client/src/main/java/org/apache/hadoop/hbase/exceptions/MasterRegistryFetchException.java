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
package org.apache.hadoop.hbase.exceptions;

import java.util.Set;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.PrettyPrinter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Exception thrown when an master registry RPC fails in client. The exception includes the list of
 * masters to which RPC was attempted and the last exception encountered. Prior exceptions are
 * included in the logs.
 */
@InterfaceAudience.Private
public class MasterRegistryFetchException extends HBaseIOException {

  private static final long serialVersionUID = 6992134872168185171L;

  public MasterRegistryFetchException(Set<ServerName> masters, Throwable failure) {
    super(String.format("Exception making rpc to masters %s", PrettyPrinter.toString(masters)),
        failure);
  }
}
