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

package org.apache.hadoop.hbase;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Factory for classes supplied by hadoop compatibility modules.  Only one of each class will be
 *  created.
 */
@InterfaceAudience.Private
public class CompatibilitySingletonFactory extends CompatibilityFactory {
  public static enum SingletonStorage {
    INSTANCE;
    private final Object lock = new Object();
    private final Map<Class, Object> instances = new HashMap<>();
  }
  private static final Logger LOG = LoggerFactory.getLogger(CompatibilitySingletonFactory.class);

  /**
   * This is a static only class don't let anyone create an instance.
   */
  protected CompatibilitySingletonFactory() {  }

  /**
   * Get the singleton instance of Any classes defined by compatibiliy jar's
   *
   * @return the singleton
   */
  @SuppressWarnings("unchecked")
  public static <T> T getInstance(Class<T> klass) {
    synchronized (SingletonStorage.INSTANCE.lock) {
      T instance = (T) SingletonStorage.INSTANCE.instances.get(klass);
      if (instance == null) {
        try {
          ServiceLoader<T> loader = ServiceLoader.load(klass);
          Iterator<T> it = loader.iterator();
          instance = it.next();
          if (it.hasNext()) {
            StringBuilder msg = new StringBuilder();
            msg.append("ServiceLoader provided more than one implementation for class: ")
                .append(klass)
                .append(", using implementation: ").append(instance.getClass())
                .append(", other implementations: {");
            while (it.hasNext()) {
              msg.append(it.next()).append(" ");
            }
            msg.append("}");
            LOG.warn(msg.toString());
          }
        } catch (Exception e) {
          throw new RuntimeException(createExceptionString(klass), e);
        } catch (Error e) {
          throw new RuntimeException(createExceptionString(klass), e);
        }

        // If there was nothing returned and no exception then throw an exception.
        if (instance == null) {
          throw new RuntimeException(createExceptionString(klass));
        }
        SingletonStorage.INSTANCE.instances.put(klass, instance);
      }
      return instance;
    }

  }
}
