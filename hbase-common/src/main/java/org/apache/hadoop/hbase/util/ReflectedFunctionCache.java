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

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache to hold resolved Functions of a specific signature, generated through reflection. These can
 * be (relatively) costly to create, but then are much faster than typical Method.invoke calls when
 * executing. The cache is built-up on demand as calls are made to new classes. The functions are
 * cached for the lifetime of the process. If a function cannot be created (security reasons, method
 * not found, etc), a fallback function is cached which always returns null. Callers to
 * {@link #getAndCallByName(String, Object)} should have handling for null return values.
 * <p>
 * An instance is created for a specified baseClass (i.e. Filter), argClass (i.e. byte[]), and
 * static methodName to call. These are used to resolve a Function which delegates to that static
 * method, if it is found.
 * @param <I> the input argument type for the resolved functions
 * @param <R> the return type for the resolved functions
 */
@InterfaceAudience.Private
public final class ReflectedFunctionCache<I, R> {

  private static final Logger LOG = LoggerFactory.getLogger(ReflectedFunctionCache.class);

  private final ConcurrentMap<String, Function<I, ? extends R>> lambdasByClass =
    new ConcurrentHashMap<>();
  private final Class<R> baseClass;
  private final Class<I> argClass;
  private final String methodName;
  private final ClassLoader classLoader;

  public ReflectedFunctionCache(Class<R> baseClass, Class<I> argClass, String staticMethodName) {
    this.classLoader = getClass().getClassLoader();
    this.baseClass = baseClass;
    this.argClass = argClass;
    this.methodName = staticMethodName;
  }

  /**
   * Get and execute the Function for the given className, passing the argument to the function and
   * returning the result.
   * @param className the full name of the class to lookup
   * @param argument  the argument to pass to the function, if found.
   * @return null if a function is not found for classname, otherwise the result of the function.
   */
  @Nullable
  public R getAndCallByName(String className, I argument) {
    // todo: if we ever make java9+ our lowest supported jdk version, we can
    // handle generating these for newly loaded classes from our DynamicClassLoader using
    // MethodHandles.privateLookupIn(). For now this is not possible, because we can't easily
    // create a privileged lookup in a non-default ClassLoader. So while this cache loads
    // over time, it will never load a custom filter from "hbase.dynamic.jars.dir".
    Function<I, ? extends R> lambda =
      ConcurrentMapUtils.computeIfAbsent(lambdasByClass, className, () -> loadFunction(className));

    return lambda.apply(argument);
  }

  private Function<I, ? extends R> loadFunction(String className) {
    long startTime = System.nanoTime();
    try {
      Class<?> clazz = Class.forName(className, false, classLoader);
      if (!baseClass.isAssignableFrom(clazz)) {
        LOG.debug("Requested class {} is not assignable to {}, skipping creation of function",
          className, baseClass.getName());
        return this::notFound;
      }
      return ReflectionUtils.getOneArgStaticMethodAsFunction(clazz, methodName, argClass,
        (Class<? extends R>) clazz);
    } catch (Throwable t) {
      LOG.debug("Failed to create function for {}", className, t);
      return this::notFound;
    } finally {
      LOG.debug("Populated cache for {} in {}ms", className,
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime));
    }
  }

  /**
   * In order to use computeIfAbsent, we can't store nulls in our cache. So we store a lambda which
   * resolves to null. The contract is that getAndCallByName returns null in this case.
   */
  private R notFound(I argument) {
    return null;
  }

}
