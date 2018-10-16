/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface that defines how a region server in peer cluster will get source cluster file system
 * configurations. User can configure their custom implementation implementing this interface by
 * setting the value of their custom implementation's fully qualified class name to
 * hbase.replication.source.fs.conf.provider property in RegionServer configuration. Default is
 * {@link DefaultSourceFSConfigurationProvider}
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public interface SourceFSConfigurationProvider {

  /**
   * Returns the source cluster file system configuration for the given source cluster replication
   * ID.
   * @param sinkConf sink cluster configuration
   * @param replicationClusterId unique ID which identifies the source cluster
   * @return source cluster file system configuration
   * @throws IOException for invalid directory or for a bad disk.
   */
  public Configuration getConf(Configuration sinkConf, String replicationClusterId)
      throws IOException;

}
