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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection registry creator implementation for creating {@link ZKConnectionRegistry}.
 */
@InterfaceAudience.Private
public class ZKConnectionRegistryURIFactory implements ConnectionRegistryURIFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ZKConnectionRegistryURIFactory.class);

  @Override
  public ConnectionRegistry create(URI uri, Configuration conf, User user) throws IOException {
    assert getScheme().equals(uri.getScheme());
    LOG.debug("connect to hbase cluster with zk quorum='{}' and parent='{}'", uri.getAuthority(),
      uri.getPath());
    Configuration c = new Configuration(conf);
    c.set(HConstants.CLIENT_ZOOKEEPER_QUORUM, uri.getAuthority());
    c.set(HConstants.ZOOKEEPER_ZNODE_PARENT, uri.getPath());
    return new ZKConnectionRegistry(c, user);
  }

  @Override
  public String getScheme() {
    return "hbase+zk";
  }
}
