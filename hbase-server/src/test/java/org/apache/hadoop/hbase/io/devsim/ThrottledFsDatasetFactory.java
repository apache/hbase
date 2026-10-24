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
package org.apache.hadoop.hbase.io.devsim;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.DataStorage;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that creates {@link ThrottledFsDataset} proxies wrapping the standard
 * {@code FsDatasetImpl}. Configured via:
 *
 * <pre>
 * conf.set("dfs.datanode.fsdataset.factory", ThrottledFsDatasetFactory.class.getName());
 * </pre>
 *
 * Hadoop calls {@link #newInstance} once per DataNode during startup. Each invocation creates an
 * independent proxy with its own set of {@link EBSVolumeDevice} instances.
 */
@SuppressWarnings("rawtypes")
public class ThrottledFsDatasetFactory extends FsDatasetSpi.Factory {

  private static final Logger LOG = LoggerFactory.getLogger(ThrottledFsDatasetFactory.class);

  private static final String DEFAULT_FACTORY_CLASS =
    "org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetFactory";

  @Override
  @SuppressWarnings("unchecked")
  public FsDatasetSpi newInstance(DataNode datanode, DataStorage storage, Configuration conf)
    throws IOException {
    FsDatasetSpi.Factory defaultFactory = loadDefaultFactory();
    FsDatasetSpi<?> inner = defaultFactory.newInstance(datanode, storage, conf);
    String dnId = datanode.getDatanodeId() != null
      ? datanode.getDatanodeId().getDatanodeUuid()
      : "DN-" + System.identityHashCode(datanode);
    LOG.info("ThrottledFsDatasetFactory: creating EBS device layer for DataNode {}", dnId);
    return ThrottledFsDataset.wrap(inner, dnId, conf);
  }

  @SuppressWarnings("unchecked")
  private static FsDatasetSpi.Factory loadDefaultFactory() throws IOException {
    try {
      Class<?> clazz = Class.forName(DEFAULT_FACTORY_CLASS);
      return (FsDatasetSpi.Factory) clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IOException("Failed to load default FsDataset factory: " + DEFAULT_FACTORY_CLASS,
        e);
    }
  }
}
