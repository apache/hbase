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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Class that will create many instances of classes provided by the hbase-hadoop{1|2}-compat jars.
 */
public class CompatibilityFactory {

  private static final Log LOG = LogFactory.getLog(CompatibilitySingletonFactory.class);
  public static final String EXCEPTION_START = "Could not create  ";
  public static final String EXCEPTION_END = " Is the hadoop compatibility jar on the classpath?";

  /**
   * This is a static only class don't let any instance be created.
   */
  protected CompatibilityFactory() {}

  public static synchronized <T> T getInstance(Class<T> klass) {
    T instance = null;
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
        LOG.warn(msg);
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
    return instance;
  }

  protected static String createExceptionString(Class klass) {
    return EXCEPTION_START + klass.toString() + EXCEPTION_END;
  }
}
