/**
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
package org.apache.hadoop.hbase.util;

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class ReflectionUtils {
  @SuppressWarnings("unchecked")
  public static <T> T instantiateWithCustomCtor(String className,
    Class<? >[] ctorArgTypes, Object[] ctorArgs) {
    try {
      Class<? extends T> resultType = (Class<? extends T>) Class.forName(className);
      return resultType.getDeclaredConstructor(ctorArgTypes).newInstance(ctorArgs);
    } catch (ClassNotFoundException e) {
      throw new UnsupportedOperationException(
          "Unable to find " + className, e);
    } catch (IllegalAccessException e) {
      throw new UnsupportedOperationException(
          "Unable to access specified class " + className, e);
    } catch (InstantiationException e) {
      throw new UnsupportedOperationException(
          "Unable to instantiate specified class " + className, e);
    } catch (InvocationTargetException e) {
      throw new UnsupportedOperationException(
          "Constructor threw an exception for " + className, e);
    } catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException(
          "Unable to find suitable constructor for class " + className, e);
    }
  }
}
