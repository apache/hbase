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
package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Will be thrown when server received a security preamble call for asking the server principal but
 * security is not enabled for this server.
 * <p>
 * This exception will not be thrown to upper layer so mark it as IA.Private.
 */
@InterfaceAudience.Private
public class SecurityNotEnabledException extends HBaseIOException {

  private static final long serialVersionUID = -3682812966232247662L;

}
