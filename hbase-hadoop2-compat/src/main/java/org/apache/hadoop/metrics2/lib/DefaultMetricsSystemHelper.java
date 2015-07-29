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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Method;

public class DefaultMetricsSystemHelper {

  private static final Log LOG = LogFactory.getLog(DefaultMetricsSystemHelper.class);
  private final Method removeObjectMethod;

  public DefaultMetricsSystemHelper() {
    Method m;
    try {
      Class<? extends DefaultMetricsSystem> clazz = DefaultMetricsSystem.INSTANCE.getClass();
      m = clazz.getDeclaredMethod("removeObjectName", String.class);
      m.setAccessible(true);
    } catch (NoSuchMethodException e) {
      m = null;
    }
    removeObjectMethod = m;
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
}
