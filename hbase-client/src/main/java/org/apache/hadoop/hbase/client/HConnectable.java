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

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * This class makes it convenient for one to execute a command in the context
 * of a {@link HConnection} instance based on the given {@link Configuration}.
 *
 * <p>
 * If you find yourself wanting to use a {@link HConnection} for a relatively
 * short duration of time, and do not want to deal with the hassle of creating
 * and cleaning up that resource, then you should consider using this
 * convenience class.
 *
 * @param <T>
 *          the return type of the {@link HConnectable#connect(HConnection)}
 *          method.
 */
@InterfaceAudience.Private
public abstract class HConnectable<T> {
  public Configuration conf;

  protected HConnectable(Configuration conf) {
    this.conf = conf;
  }

  public abstract T connect(HConnection connection) throws IOException;
}
