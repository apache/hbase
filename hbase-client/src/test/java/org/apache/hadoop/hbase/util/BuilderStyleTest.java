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

package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to check whether a given class conforms to builder-style:
 * Foo foo =
 *   new Foo()
 *     .setBar(bar)
 *     .setBaz(baz)
 */
public class BuilderStyleTest {

  /*
   * If a base class Foo declares a method setFoo() returning Foo, then the subclass should
   * re-declare the methods overriding the return class with the subclass:
   *
   * class Foo {
   *   Foo setFoo() {
   *     ..
   *     return this;
   *   }
   * }
   *
   * class Bar {
   *   Bar setFoo() {
   *     return (Bar) super.setFoo();
   *   }
   * }
   *
   */
  @SuppressWarnings("rawtypes")
  public static void assertClassesAreBuilderStyle(Class... classes) {
    for (Class clazz : classes) {
      System.out.println("Checking " + clazz);
      Method[] methods = clazz.getDeclaredMethods();
      Map<String, Set<Method>> methodsBySignature = new HashMap<>();
      for (Method method : methods) {
        if (!Modifier.isPublic(method.getModifiers())) {
          continue; // only public classes
        }
        Class<?> ret = method.getReturnType();
        if (method.getName().startsWith("set") || method.getName().startsWith("add")) {
          System.out.println("  " + clazz.getSimpleName() + "." + method.getName() + "() : "
            + ret.getSimpleName());

          // because of subclass / super class method overrides, we group the methods fitting the
          // same signatures because we get two method definitions from java reflection:
          // Mutation.setDurability() : Mutation
          //   Delete.setDurability() : Mutation
          // Delete.setDurability() : Delete
          String sig = method.getName();
          for (Class<?> param : method.getParameterTypes()) {
            sig += param.getName();
          }
          Set<Method> sigMethods = methodsBySignature.get(sig);
          if (sigMethods == null) {
            sigMethods = new HashSet<Method>();
            methodsBySignature.put(sig, sigMethods);
          }
          sigMethods.add(method);
        }
      }
      // now iterate over the methods by signatures
      for (Map.Entry<String, Set<Method>> e : methodsBySignature.entrySet()) {
        // at least one of method sigs should return the declaring class
        boolean found = false;
        for (Method m : e.getValue()) {
          found = clazz.isAssignableFrom(m.getReturnType());
          if (found) break;
        }
        String errorMsg = "All setXXX()|addXX() methods in " + clazz.getSimpleName()
            + " should return a " + clazz.getSimpleName() + " object in builder style. "
            + "Offending method:" + e.getValue().iterator().next().getName();
        assertTrue(errorMsg, found);
      }
    }
  }
}
