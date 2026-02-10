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
package org.apache.hadoop.hbase;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.stream.Stream;
import org.apache.yetus.audience.InterfaceAudience;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.params.provider.Arguments;

/**
 * The entry point class for supporting JUnit4 like Parameterized test, where we can use constructor
 * to pass parameters.
 * <p>
 * JUnit5's {@link org.junit.jupiter.params.ParameterizedClass} will create separated test classes,
 * which is different with JUnit4 and {@link org.junit.jupiter.params.ParameterizedTest} does not
 * support passing parameters through constructors.
 * <p>
 * When you want to use this provider, annotation the test class with
 * {@link HBaseParameterizedTestTemplate}, and provide a static method named "parameters" for
 * providing the arguments. The method must have no parameter, and return a Stream&lt;Arguments&gt;.
 * All the test method should be marked with {@link org.junit.jupiter.api.TestTemplate}, not
 * {@link org.junit.jupiter.api.Test} or {@link org.junit.jupiter.params.ParameterizedTest}.
 * @see HBaseParameterizedTestTemplate
 * @see HBaseParameterizedInvocationContext
 * @see HBaseParameterizedParameterResolver
 */
@InterfaceAudience.Private
public class HBaseParameterizedTemplateProvider implements TestTemplateInvocationContextProvider {

  private static final String PARAMETERS_METHOD_NAME = "parameters";

  @Override
  public boolean supportsTestTemplate(ExtensionContext context) {
    return context.getTestClass()
      .map(c -> c.isAnnotationPresent(HBaseParameterizedTestTemplate.class)).orElse(false);
  }

  @Override
  public Stream<TestTemplateInvocationContext>
    provideTestTemplateInvocationContexts(ExtensionContext context) {
    Class<?> testClass = context.getRequiredTestClass();
    // get parameters
    Method method;
    try {
      method = testClass.getDeclaredMethod(PARAMETERS_METHOD_NAME);
    } catch (NoSuchMethodException e) {
      throw new ExtensionConfigurationException(
        "Test class must declare static " + PARAMETERS_METHOD_NAME + " method");
    }

    if (!Modifier.isStatic(method.getModifiers())) {
      throw new ExtensionConfigurationException(PARAMETERS_METHOD_NAME + " method must be static");
    }
    if (method.getParameterCount() > 0) {
      throw new ExtensionConfigurationException(
        PARAMETERS_METHOD_NAME + " method must not have any parameters");
    }

    Stream<Arguments> args;
    try {
      args = (Stream<Arguments>) method.invoke(null);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new ExtensionConfigurationException("failed to invoke parameters method", e);
    }
    // get display name
    String namePattern = testClass.getAnnotation(HBaseParameterizedTestTemplate.class).name();

    return args.map(arg -> new HBaseParameterizedInvocationContext(arg, namePattern));
  }

}
