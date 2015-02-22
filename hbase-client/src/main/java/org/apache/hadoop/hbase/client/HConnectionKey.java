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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;

/**
 * Denotes a unique key to an {@link HConnection} instance.
 *
 * In essence, this class captures the properties in {@link Configuration}
 * that may be used in the process of establishing a connection. In light of
 * that, if any new such properties are introduced into the mix, they must be
 * added to the {@link HConnectionKey#properties} list.
 *
 */
class HConnectionKey {
  final static String[] CONNECTION_PROPERTIES = new String[] {
      HConstants.ZOOKEEPER_QUORUM, HConstants.ZOOKEEPER_ZNODE_PARENT,
      HConstants.ZOOKEEPER_CLIENT_PORT,
      HConstants.ZOOKEEPER_RECOVERABLE_WAITTIME,
      HConstants.HBASE_CLIENT_PAUSE, HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.HBASE_RPC_TIMEOUT_KEY,
      HConstants.HBASE_META_SCANNER_CACHING,
      HConstants.HBASE_CLIENT_INSTANCE_ID,
      HConstants.RPC_CODEC_CONF_KEY,
      HConstants.USE_META_REPLICAS,
      RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY};

  private Map<String, String> properties;
  private String username;

  HConnectionKey(Configuration conf) {
    Map<String, String> m = new HashMap<String, String>();
    if (conf != null) {
      for (String property : CONNECTION_PROPERTIES) {
        String value = conf.get(property);
        if (value != null) {
          m.put(property, value);
        }
      }
    }
    this.properties = Collections.unmodifiableMap(m);

    try {
      UserProvider provider = UserProvider.instantiate(conf);
      User currentUser = provider.getCurrent();
      if (currentUser != null) {
        username = currentUser.getName();
      }
    } catch (IOException ioe) {
      ConnectionManager.LOG.warn(
          "Error obtaining current user, skipping username in HConnectionKey", ioe);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    if (username != null) {
      result = username.hashCode();
    }
    for (String property : CONNECTION_PROPERTIES) {
      String value = properties.get(property);
      if (value != null) {
        result = prime * result + value.hashCode();
      }
    }

    return result;
  }


  @edu.umd.cs.findbugs.annotations.SuppressWarnings (value="ES_COMPARING_STRINGS_WITH_EQ",
      justification="Optimization")
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    HConnectionKey that = (HConnectionKey) obj;
    if (this.username != null && !this.username.equals(that.username)) {
      return false;
    } else if (this.username == null && that.username != null) {
      return false;
    }
    if (this.properties == null) {
      if (that.properties != null) {
        return false;
      }
    } else {
      if (that.properties == null) {
        return false;
      }
      for (String property : CONNECTION_PROPERTIES) {
        String thisValue = this.properties.get(property);
        String thatValue = that.properties.get(property);
        //noinspection StringEquality
        if (thisValue == thatValue) {
          continue;
        }
        if (thisValue == null || !thisValue.equals(thatValue)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return "HConnectionKey{" +
      "properties=" + properties +
      ", username='" + username + '\'' +
      '}';
  }
}
