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
package org.apache.hadoop.hbase.master.balancer;

import java.io.Serializable;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.ServerName;

/**
 * Data structure that holds servername and 'load'.
 */
@InterfaceAudience.Private
class ServerAndLoad implements Comparable<ServerAndLoad>, Serializable {
  private static final long serialVersionUID = 2735470854607296965L;
  private final ServerName sn;
  private final int load;

  ServerAndLoad(final ServerName sn, final int load) {
    this.sn = sn;
    this.load = load;
  }

  ServerName getServerName() {
    return this.sn;
  }

  int getLoad() {
    return this.load;
  }

  @Override
  public int compareTo(ServerAndLoad other) {
    int diff = this.load - other.load;
    return diff != 0 ? diff : this.sn.compareTo(other.getServerName());
  }

  @Override
  public int hashCode() {
    int result = load;
    result = 31 * result + ((sn == null) ? 0 : sn.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ServerAndLoad) {
      ServerAndLoad sl = (ServerAndLoad) o;
      return this.compareTo(sl) == 0;
    }
    return false;
  }
}
