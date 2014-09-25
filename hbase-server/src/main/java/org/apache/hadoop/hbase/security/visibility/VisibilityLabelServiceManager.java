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
package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Manages singleton instance of {@link VisibilityLabelService}
 */
@InterfaceAudience.Private
public class VisibilityLabelServiceManager {

  private static final Log LOG = LogFactory.getLog(VisibilityLabelServiceManager.class);

  public static final String VISIBILITY_LABEL_SERVICE_CLASS =
      "hbase.regionserver.visibility.label.service.class";
  private static final VisibilityLabelServiceManager INSTANCE = new VisibilityLabelServiceManager();

  private volatile VisibilityLabelService visibilityLabelService = null;
  private String vlsClazzName = null;

  private VisibilityLabelServiceManager() {

  }

  public static VisibilityLabelServiceManager getInstance() {
    return INSTANCE;
  }

  /**
   * @param conf
   * @return singleton instance of {@link VisibilityLabelService}. The FQCN of the implementation
   *         class can be specified using "hbase.regionserver.visibility.label.service.class".
   * @throws IOException When VLS implementation, as specified in conf, can not be loaded.
   */
  public VisibilityLabelService getVisibilityLabelService(Configuration conf) throws IOException {
    String vlsClassName = conf.get(VISIBILITY_LABEL_SERVICE_CLASS,
        DefaultVisibilityLabelServiceImpl.class.getCanonicalName()).trim();
    if (this.visibilityLabelService != null) {
      checkForClusterLevelSingleConf(vlsClassName);
      return this.visibilityLabelService;
    }
    synchronized (this) {
      if (this.visibilityLabelService != null) {
        checkForClusterLevelSingleConf(vlsClassName);
        return this.visibilityLabelService;
      }
      this.vlsClazzName = vlsClassName;
      try {
        this.visibilityLabelService = (VisibilityLabelService) ReflectionUtils.newInstance(
            Class.forName(vlsClassName), conf);
      } catch (ClassNotFoundException e) {
        throw new IOException(e);
      }
      return this.visibilityLabelService;
    }
  }

  private void checkForClusterLevelSingleConf(String vlsClassName) {
    assert this.vlsClazzName != null;
    if (!this.vlsClazzName.equals(vlsClassName)) {
      LOG.warn("Trying to use table specific value for config "
          + "'hbase.regionserver.visibility.label.service.class' which is not supported."
          + " Will use the cluster level VisibilityLabelService class " + this.vlsClazzName);
    }
  }

  /**
   * @return singleton instance of {@link VisibilityLabelService}.
   * @throws IllegalStateException if this called before initialization of singleton instance.
   */
  public VisibilityLabelService getVisibilityLabelService() {
    // By the time this method is called, the singleton instance of visibilityLabelService should
    // have been created. And it will be created as getVisibilityLabelService(Configuration conf)
    // is called from VC#start() and that will be the 1st thing core code do with any CP.
    if (this.visibilityLabelService == null) {
      throw new IllegalStateException("VisibilityLabelService not yet instantiated");
    }
    return this.visibilityLabelService;
  }
}
