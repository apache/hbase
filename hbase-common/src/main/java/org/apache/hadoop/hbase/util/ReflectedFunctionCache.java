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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.reflect.ClassPath;

/**
 * Cache to hold resolved Functions generated through reflection. These can be costly to create, but
 * then are much faster than typical Method.invoke calls when executing. Upon construction, finds
 * all subclasses in the same package of the passed baseClass. For each found class, creates a
 * lambda using
 * {@link ReflectionUtils#getOneArgStaticMethodAsFunction(Class, String, Class, Class)}. These are
 * added to a hashmap for fast lookup by name later.
 * @param <I> the input argument type for the resolved functions
 * @param <R> the return type for the resolved functions
 */
@InterfaceAudience.Private
final public class ReflectedFunctionCache<I, R> {

  private static final Logger LOG = LoggerFactory.getLogger(ReflectedFunctionCache.class);

  private final Map<String, Function<I, ? extends R>> lambdasByClass;

  private ReflectedFunctionCache(Map<String, Function<I, ? extends R>> lambdasByClass) {
    this.lambdasByClass = lambdasByClass;
  }

  /**
   * Create a cache of reflected functions using the provided classloader and baseClass. Will find
   * all subclasses of the provided baseClass (in the same package), and then foreach look for a
   * static one-arg method with the methodName and argClass. The expectation is that the method
   * returns a value whose class extends the baseClass. This was primarily designed for use by our
   * Filter and Comparator parseFrom methods.
   */
  public static <I, R> ReflectedFunctionCache<I, R> create(ClassLoader classLoader,
    Class<R> baseClass, Class<I> argClass, String methodName) {
    Map<String, Function<I, ? extends R>> lambdasByClass = new HashMap<>();
    Set<? extends Class<? extends R>> classes = getSubclassesInPackage(classLoader, baseClass);
    for (Class<? extends R> clazz : classes) {
      Function<I, ? extends R> func = createFunction(clazz, methodName, argClass, clazz);
      if (func != null) {
        lambdasByClass.put(clazz.getName(), func);
      }
    }
    return new ReflectedFunctionCache<>(lambdasByClass);
  }

  /**
   * Get and execute the Function for the given className, passing the argument to the function and
   * returning the result.
   * @param className the full name of the class to lookup
   * @param argument  the argument to pass to the function, if found.
   * @return null if a function is not found for classname, otherwise the result of the function.
   */
  public R getAndCallByName(String className, I argument) {
    Function<I, ? extends R> lambda = lambdasByClass.get(className);

    // todo: if we ever make java9+ our lowest supported jdk version, we can
    // handle generating these for newly loaded classes from our DynamicClassLoader using
    // MethodHandles.privateLookupIn(). For now this is not possible, because we can't easily
    // create a privileged lookup in a non-default ClassLoader.
    if (lambda == null) {
      return null;
    }

    return lambda.apply(argument);
  }

  private static <R> Set<Class<? extends R>> getSubclassesInPackage(ClassLoader classLoader,
    Class<R> baseClass) {
    try {
      return ClassPath.from(classLoader).getAllClasses().stream()
        .filter(clazz -> clazz.getPackageName().equalsIgnoreCase(baseClass.getPackage().getName()))
        .map(ClassPath.ClassInfo::load).filter(clazz -> !Modifier.isAbstract(clazz.getModifiers()))
        .filter(baseClass::isAssignableFrom).map(clazz -> (Class<? extends R>) clazz)
        .collect(Collectors.toSet());
    } catch (IOException e) {
      LOG.debug("Failed to resolve subclasses of {}", baseClass, e);
      return Collections.emptySet();
    }
  }

  private static <I, O> Function<I, O> createFunction(Class<?> clazz, String methodName,
    Class<I> argumentClazz, Class<O> returnValueClass) {
    try {
      return ReflectionUtils.getOneArgStaticMethodAsFunction(clazz, methodName, argumentClazz,
        returnValueClass);
    } catch (Throwable e) {
      LOG.debug("Failed to create function for class={}", clazz, e);
      return null;
    }

  }
}
