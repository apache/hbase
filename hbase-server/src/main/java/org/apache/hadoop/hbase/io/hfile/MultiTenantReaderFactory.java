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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating appropriate multi-tenant HFile readers based on the reader type. This
 * handles both stream and pread access modes for multi-tenant HFiles.
 */
@InterfaceAudience.Private
public class MultiTenantReaderFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MultiTenantReaderFactory.class);

  private MultiTenantReaderFactory() {
    // Utility class, no instantiation
  }

  /**
   * Create the appropriate multi-tenant reader based on the reader type.
   * @param context   Reader context info
   * @param fileInfo  HFile info
   * @param cacheConf Cache configuration values
   * @param conf      Configuration
   * @return An appropriate multi-tenant HFile reader
   * @throws IOException If an error occurs creating the reader
   */
  public static HFile.Reader create(ReaderContext context, HFileInfo fileInfo,
    CacheConfig cacheConf, Configuration conf) throws IOException {

    if (context.getReaderType() == ReaderContext.ReaderType.STREAM) {
      LOG.debug("Creating MultiTenantStreamReader for {}", context.getFilePath());
      return new MultiTenantStreamReader(context, fileInfo, cacheConf, conf);
    } else {
      LOG.debug("Creating MultiTenantPreadReader for {}", context.getFilePath());
      return new MultiTenantPreadReader(context, fileInfo, cacheConf, conf);
    }
  }
}
