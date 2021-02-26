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
package org.apache.hadoop.metrics2.lib;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class DefaultMetricsSystemHelper {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetricsSystemHelper.class);
  private final Method removeObjectMethod;
  private final Field sourceNamesField;
  private final Field mapField;

  public DefaultMetricsSystemHelper() {
    Class<? extends DefaultMetricsSystem> clazz = DefaultMetricsSystem.INSTANCE.getClass();
    Method m;
    try {
      m = clazz.getDeclaredMethod("removeObjectName", String.class);
      m.setAccessible(true);
    } catch (NoSuchMethodException e) {
      m = null;
    }
    removeObjectMethod = m;

    Field f1, f2;
    try {
      f1 = clazz.getDeclaredField("sourceNames");
      f1.setAccessible(true);
      f2 = UniqueNames.class.getDeclaredField("map");
      f2.setAccessible(true);
    } catch (NoSuchFieldException e) {
      LOG.trace(e.toString(), e);
      f1 = null;
      f2 = null;
    }
    sourceNamesField = f1;
    mapField = f2;
  }

  public boolean removeObjectName(final String name) {
    if (removeObjectMethod != null) {
      try {
        removeObjectMethod.invoke(DefaultMetricsSystem.INSTANCE, name);
        return true;
      } catch (Exception e) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Unable to remove object name from cache: " + name, e);
        }
      }
    }
    return false;
  }

  /**
   * Unfortunately Hadoop tries to be too-clever and permanently keeps track of all names registered
   * so far as a Source, thus preventing further re-registration of the source with the same name.
   * In case of dynamic metrics tied to region-lifecycles, this becomes a problem because we would
   * like to be able to re-register and remove with the same name. Otherwise, it is resource leak.
   * This ugly code manually removes the name from the UniqueNames map.
   * TODO: May not be needed for Hadoop versions after YARN-5190.
   */
  public void removeSourceName(String name) {
    if (sourceNamesField == null || mapField == null) {
      return;
    }
    try {
      Object sourceNames = sourceNamesField.get(DefaultMetricsSystem.INSTANCE);
      HashMap map = (HashMap) mapField.get(sourceNames);
      synchronized (sourceNames) {
        map.remove(name);
      }
    } catch (Exception ex) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Received exception while trying to access Hadoop Metrics classes via " +
                        "reflection.", ex);
      }
    }
  }
}
