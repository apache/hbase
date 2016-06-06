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

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Triple;
import org.apache.hadoop.hbase.ClassFinder.And;
import org.apache.hadoop.hbase.ClassFinder.FileNameFilter;
import org.apache.hadoop.hbase.ClassFinder.Not;
import org.apache.hadoop.hbase.ClassTestFinder.TestClassFilter;
import org.apache.hadoop.hbase.ClassTestFinder.TestFileNameFilter;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test cases for ensuring our client visible classes have annotations
 * for {@link InterfaceAudience}.
 *
 * All classes in hbase-client and hbase-common module MUST have InterfaceAudience
 * annotations. All InterfaceAudience.Public annotated classes MUST also have InterfaceStability
 * annotations. Think twice about marking an interface InterfaceAudience.Public. Make sure that
 * it is an interface, not a class (for most cases), and clients will actually depend on it. Once
 * something is marked with Public, we cannot change the signatures within the major release. NOT
 * everything in the hbase-client module or every java public class has to be marked with
 * InterfaceAudience.Public. ONLY the ones that an hbase application will directly use (Table, Get,
 * etc, versus ProtobufUtil).
 *
 * Also note that HBase has it's own annotations in hbase-annotations module with the same names
 * as in Hadoop. You should use the HBase's classes.
 *
 * See https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/InterfaceClassification.html
 * and https://issues.apache.org/jira/browse/HBASE-10462.
 */
@Category(SmallTests.class)
public class TestInterfaceAudienceAnnotations {

  private static final String HBASE_PROTOBUF = "org.apache.hadoop.hbase.protobuf.generated";
  private static final Log LOG = LogFactory.getLog(TestInterfaceAudienceAnnotations.class);

  /** Selects classes with generated in their package name */
  class GeneratedClassFilter implements ClassFinder.ClassFilter {
    @Override
    public boolean isCandidateClass(Class<?> c) {
      return c.getPackage().getName().contains("generated");
    }
  }

  /** Selects classes with one of the {@link InterfaceAudience} annotation in their class
   * declaration.
   */
  class InterfaceAudienceAnnotatedClassFilter implements ClassFinder.ClassFilter {
    @Override
    public boolean isCandidateClass(Class<?> c) {
      if (getAnnotation(c) != null) {
        // class itself has a declared annotation.
        return true;
      }

      // If this is an internal class, look for the encapsulating class to see whether it has
      // annotation. All inner classes of private classes are considered annotated.
      return isAnnotatedPrivate(c.getEnclosingClass());
    }

    private boolean isAnnotatedPrivate(Class<?> c) {
      if (c == null) {
        return false;
      }

      Class<?> ann = getAnnotation(c);
      if (ann != null &&
        !InterfaceAudience.Public.class.equals(ann)) {
        return true;
      }

      return isAnnotatedPrivate(c.getEnclosingClass());
    }

    protected Class<?> getAnnotation(Class<?> c) {
      // we should get only declared annotations, not inherited ones
      Annotation[] anns = c.getDeclaredAnnotations();

      for (Annotation ann : anns) {
        // Hadoop clearly got it wrong for not making the annotation values (private, public, ..)
        // an enum instead we have three independent annotations!
        Class<?> type = ann.annotationType();
        if (isInterfaceAudienceClass(type)) {
          return type;
        }
      }
      return null;
    }
  }

  /** Selects classes with one of the {@link InterfaceStability} annotation in their class
   * declaration.
   */
  class InterfaceStabilityAnnotatedClassFilter implements ClassFinder.ClassFilter {
    @Override
    public boolean isCandidateClass(Class<?> c) {
      if (getAnnotation(c) != null) {
        // class itself has a declared annotation.
        return true;
      }
      return false;
    }

    protected Class<?> getAnnotation(Class<?> c) {
      // we should get only declared annotations, not inherited ones
      Annotation[] anns = c.getDeclaredAnnotations();

      for (Annotation ann : anns) {
        // Hadoop clearly got it wrong for not making the annotation values (private, public, ..)
        // an enum instead we have three independent annotations!
        Class<?> type = ann.annotationType();
        if (isInterfaceStabilityClass(type)) {
          return type;
        }
      }
      return null;
    }
  }

  /** Selects classes with one of the {@link InterfaceAudience.Public} annotation in their
   * class declaration.
   */
  class InterfaceAudiencePublicAnnotatedClassFilter extends InterfaceAudienceAnnotatedClassFilter {
    @Override
    public boolean isCandidateClass(Class<?> c) {
      return (InterfaceAudience.Public.class.equals(getAnnotation(c)));
    }
  }

  /**
   * Selects InterfaceAudience or InterfaceStability classes. Don't go meta!!!
   */
  class IsInterfaceStabilityClassFilter implements ClassFinder.ClassFilter {
    @Override
    public boolean isCandidateClass(Class<?> c) {
      return
          isInterfaceAudienceClass(c) ||
          isInterfaceStabilityClass(c);
    }
  }

  private boolean isInterfaceAudienceClass(Class<?> c) {
    return
        c.equals(InterfaceAudience.Public.class) ||
        c.equals(InterfaceAudience.Private.class) ||
        c.equals(InterfaceAudience.LimitedPrivate.class);
  }

  private boolean isInterfaceStabilityClass(Class<?> c) {
    return
        c.equals(InterfaceStability.Stable.class) ||
        c.equals(InterfaceStability.Unstable.class) ||
        c.equals(InterfaceStability.Evolving.class);
  }

  private boolean isInterfacePrivateMethod(Method m) {
    if(m.getDeclaredAnnotations().length > 0) {
      for(Annotation ann : m.getDeclaredAnnotations()) {
        if(ann.annotationType().equals(InterfaceAudience.Private.class)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean isInterfacePrivateContructor(Constructor<?> c) {
    if(c.getDeclaredAnnotations().length > 0) {
      for(Annotation ann : c.getDeclaredAnnotations()) {
        if(ann.annotationType().equals(InterfaceAudience.Private.class)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Selects classes that are declared public */
  class PublicClassFilter implements ClassFinder.ClassFilter {
    @Override
    public boolean isCandidateClass(Class<?> c) {
      int mod = c.getModifiers();
      return Modifier.isPublic(mod);
    }
  }

  /** Selects paths (jars and class dirs) only from the main code, not test classes */
  class MainCodeResourcePathFilter implements ClassFinder.ResourcePathFilter {
    @Override
    public boolean isCandidatePath(String resourcePath, boolean isJar) {
      return !resourcePath.contains("test-classes") &&
          !resourcePath.contains("tests.jar");
    }
  }

  /**
   * Selects classes that appear to be source instrumentation from Clover.
   * Clover generates instrumented code in order to calculate coverage. Part of the
   * generated source is a static inner class on each source class.
   *
   * - has an enclosing class
   * - enclosing class is not an interface
   * - name starts with "__CLR"
   */
  class CloverInstrumentationFilter implements ClassFinder.ClassFilter {
    @Override
    public boolean isCandidateClass(Class<?> clazz) {
      boolean clover = false;
      final Class<?> enclosing = clazz.getEnclosingClass();
      if (enclosing != null) {
        if (!(enclosing.isInterface())) {
          clover = clazz.getSimpleName().startsWith("__CLR");
        }
      }
      return clover;
    }
  }

  /**
   * Checks whether all the classes in client and common modules contain
   * {@link InterfaceAudience} annotations.
   */
  @Test
  public void testInterfaceAudienceAnnotation()
      throws ClassNotFoundException, IOException, LinkageError {

    // find classes that are:
    // In the main jar
    // AND are not in a hadoop-compat module
    // AND are public
    // NOT test classes
    // AND NOT generated classes
    // AND are NOT annotated with InterfaceAudience
    // AND are NOT from Clover rewriting sources
    ClassFinder classFinder = new ClassFinder(
      new And(new MainCodeResourcePathFilter(),
              new TestFileNameFilter()),
      new Not((FileNameFilter)new TestFileNameFilter()),
      new And(new PublicClassFilter(),
              new Not(new TestClassFilter()),
              new Not(new GeneratedClassFilter()),
              new Not(new IsInterfaceStabilityClassFilter()),
              new Not(new InterfaceAudienceAnnotatedClassFilter()),
              new Not(new CloverInstrumentationFilter()))
    );

    Set<Class<?>> classes = classFinder.findClasses(false);

    LOG.info("These are the classes that DO NOT have @InterfaceAudience annotation:");
    for (Class<?> clazz : classes) {
      LOG.info(clazz);
    }

    Assert.assertEquals("All classes should have @InterfaceAudience annotation",
      0, classes.size());
  }

  /**
   * Checks whether all the classes in client and common modules that are marked
   * InterfaceAudience.Public also have {@link InterfaceStability} annotations.
   */
  @Test
  public void testInterfaceStabilityAnnotation()
      throws ClassNotFoundException, IOException, LinkageError {

    // find classes that are:
    // In the main jar
    // AND are not in a hadoop-compat module
    // AND are public
    // NOT test classes
    // AND NOT generated classes
    // AND are annotated with InterfaceAudience.Public
    // AND NOT annotated with InterfaceStability
    ClassFinder classFinder = new ClassFinder(
      new And(new MainCodeResourcePathFilter(),
              new TestFileNameFilter()),
      new Not((FileNameFilter)new TestFileNameFilter()),
      new And(new PublicClassFilter(),
              new Not(new TestClassFilter()),
              new Not(new GeneratedClassFilter()),
              new InterfaceAudiencePublicAnnotatedClassFilter(),
              new Not(new IsInterfaceStabilityClassFilter()),
              new Not(new InterfaceStabilityAnnotatedClassFilter()))
    );

    Set<Class<?>> classes = classFinder.findClasses(false);

    LOG.info("These are the classes that DO NOT have @InterfaceStability annotation:");
    for (Class<?> clazz : classes) {
      LOG.info(clazz);
    }

    Assert.assertEquals("All classes that are marked with @InterfaceAudience.Public should "
        + "have @InterfaceStability annotation as well",
      0, classes.size());
  }

  @Test
  public void testProtosInReturnTypes() throws ClassNotFoundException, IOException, LinkageError {
    Set<Class<?>> classes = findPublicClasses();
    List<Pair<Class<?>, Method>> protosReturnType = new ArrayList<Pair<Class<?>, Method>>();
    for (Class<?> clazz : classes) {
      findProtoInReturnType(clazz, protosReturnType);
    }
    if (protosReturnType.size() != 0) {
      LOG.info("These are the methods that have Protos as the return type");
      for (Pair<Class<?>, Method> pair : protosReturnType) {
        LOG.info(pair.getFirst().getName() + " " + pair.getSecond().getName() + " "
            + pair.getSecond().getReturnType().getName());
      }
    }

    Assert.assertEquals("Public exposed methods should not have protos in return type", 0,
      protosReturnType.size());
  }

  private Set<Class<?>> findPublicClasses()
      throws ClassNotFoundException, IOException, LinkageError {
    ClassFinder classFinder =
        new ClassFinder(new And(new MainCodeResourcePathFilter(), new TestFileNameFilter()),
            new Not((FileNameFilter) new TestFileNameFilter()),
            new And(new PublicClassFilter(), new Not(new TestClassFilter()),
                new Not(new GeneratedClassFilter()),
                new InterfaceAudiencePublicAnnotatedClassFilter()));
    Set<Class<?>> classes = classFinder.findClasses(false);
    return classes;
  }

  @Test
  public void testProtosInParamTypes() throws ClassNotFoundException, IOException, LinkageError {
    Set<Class<?>> classes = findPublicClasses();
    List<Triple<Class<?>, Method, Class<?>>> protosParamType =
        new ArrayList<Triple<Class<?>, Method, Class<?>>>();
    for (Class<?> clazz : classes) {
      findProtoInParamType(clazz, protosParamType);
    }

    if (protosParamType.size() != 0) {
      LOG.info("These are the methods that have Protos as the param type");
      for (Triple<Class<?>, Method, Class<?>> pair : protosParamType) {
        LOG.info(pair.getFirst().getName() + " " + pair.getSecond().getName() + " "
            + pair.getThird().getName());
      }
    }

    Assert.assertEquals("Public exposed methods should not have protos in param type", 0,
      protosParamType.size());
  }

  @Test
  public void testProtosInConstructors() throws ClassNotFoundException, IOException, LinkageError {
    Set<Class<?>> classes = findPublicClasses();
    List<Class<?>> classList = new ArrayList<Class<?>>();
    for (Class<?> clazz : classes) {
      Constructor<?>[] constructors = clazz.getConstructors();
      for (Constructor<?> cons : constructors) {
        if (!isInterfacePrivateContructor(cons)) {
          Class<?>[] parameterTypes = cons.getParameterTypes();
          for (Class<?> param : parameterTypes) {
            if (param.getName().contains(HBASE_PROTOBUF)) {
              classList.add(clazz);
              break;
            }
          }
        }
      }
    }

    if (classList.size() != 0) {
      LOG.info("These are the classes that have Protos in the constructor");
      for (Class<?> clazz : classList) {
        LOG.info(clazz.getName());
      }
    }

    Assert.assertEquals("Public exposed classes should not have protos in constructors", 0,
      classList.size());
  }

  private void findProtoInReturnType(Class<?> clazz,
      List<Pair<Class<?>, Method>> protosReturnType) {
    Pair<Class<?>, Method> returnTypePair = new Pair<Class<?>, Method>();
    Method[] methods = clazz.getMethods();
    returnTypePair.setFirst(clazz);
    for (Method method : methods) {
      if (clazz.isInterface() || method.getModifiers() == Modifier.PUBLIC) {
        if (!isInterfacePrivateMethod(method)) {
          Class<?> returnType = method.getReturnType();
          if (returnType.getName().contains(HBASE_PROTOBUF)) {
            returnTypePair.setSecond(method);
            protosReturnType.add(returnTypePair);
            continue;
          }
        }
      }
    }
  }

  private void findProtoInParamType(Class<?> clazz,
      List<Triple<Class<?>, Method, Class<?>>> protosParamType) {
    Triple<Class<?>, Method, Class<?>> paramType = new Triple<Class<?>, Method, Class<?>>();
    Method[] methods = clazz.getMethods();
    paramType.setFirst(clazz);
    for (Method method : methods) {
      if (clazz.isInterface() || method.getModifiers() == Modifier.PUBLIC) {
        if (!isInterfacePrivateMethod(method)) {
          Class<?>[] parameters = method.getParameterTypes();
          for (Class<?> param : parameters) {
            if (param.getName().contains(HBASE_PROTOBUF)) {
              paramType.setSecond(method);
              paramType.setThird(param);
              protosParamType.add(paramType);
              break;
            }
          }
        }
      }
    }
  }
}
