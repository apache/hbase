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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mortbay.log.Log;

@Category(SmallTests.class)
public class TestClassFinder {
  @Rule public TestName name = new TestName();
  private static final HBaseCommonTestingUtility testUtil = new HBaseCommonTestingUtility();
  private static final String BASEPKG = "tfcpkg";
  private static final String PREFIX = "Prefix";

  // Use unique jar/class/package names in each test case with the help
  // of these global counters; we are mucking with ClassLoader in this test
  // and we don't want individual test cases to conflict via it.
  private static AtomicLong testCounter = new AtomicLong(0);
  private static AtomicLong jarCounter = new AtomicLong(0);

  private static String basePath = null;

  @BeforeClass
  public static void createTestDir() throws IOException {
    basePath = testUtil.getDataTestDir(TestClassFinder.class.getSimpleName()).toString();
    if (!basePath.endsWith("/")) {
      basePath += "/";
    }
    // Make sure we get a brand new directory.
    File testDir = new File(basePath);
    if (testDir.exists()) {
      deleteTestDir();
    }
    assertTrue(testDir.mkdirs());
    Log.info("Using new, clean directory=" + testDir);
  }

  @AfterClass
  public static void deleteTestDir() throws IOException {
    testUtil.cleanupTestDir(TestClassFinder.class.getSimpleName());
  }

  @Test
  public void testClassFinderCanFindClassesInJars() throws Exception {
    long counter = testCounter.incrementAndGet();
    FileAndPath c1 = compileTestClass(counter, "", "c1");
    FileAndPath c2 = compileTestClass(counter, ".nested", "c2");
    FileAndPath c3 = compileTestClass(counter, "", "c3");
    packageAndLoadJar(c1, c3);
    packageAndLoadJar(c2);

    ClassFinder allClassesFinder = new ClassFinder();
    Set<Class<?>> allClasses = allClassesFinder.findClasses(
        makePackageName("", counter), false);
    assertEquals(3, allClasses.size());
  }

  @Test
  public void testClassFinderHandlesConflicts() throws Exception {
    long counter = testCounter.incrementAndGet();
    FileAndPath c1 = compileTestClass(counter, "", "c1");
    FileAndPath c2 = compileTestClass(counter, "", "c2");
    packageAndLoadJar(c1, c2);
    packageAndLoadJar(c1);

    ClassFinder allClassesFinder = new ClassFinder();
    Set<Class<?>> allClasses = allClassesFinder.findClasses(
        makePackageName("", counter), false);
    assertEquals(2, allClasses.size());
  }

  @Test
  public void testClassFinderHandlesNestedPackages() throws Exception {
    final String NESTED = ".nested";
    final String CLASSNAME1 = name.getMethodName() + "1";
    final String CLASSNAME2 = name.getMethodName() + "2";
    long counter = testCounter.incrementAndGet();
    FileAndPath c1 = compileTestClass(counter, "", "c1");
    FileAndPath c2 = compileTestClass(counter, NESTED, CLASSNAME1);
    FileAndPath c3 = compileTestClass(counter, NESTED, CLASSNAME2);
    packageAndLoadJar(c1, c2);
    packageAndLoadJar(c3);

    ClassFinder allClassesFinder = new ClassFinder();
    Set<Class<?>> nestedClasses = allClassesFinder.findClasses(
        makePackageName(NESTED, counter), false);
    assertEquals(2, nestedClasses.size());
    Class<?> nestedClass1 = makeClass(NESTED, CLASSNAME1, counter);
    assertTrue(nestedClasses.contains(nestedClass1));
    Class<?> nestedClass2 = makeClass(NESTED, CLASSNAME2, counter);
    assertTrue(nestedClasses.contains(nestedClass2));
  }

  @Test
  public void testClassFinderFiltersByNameInJar() throws Exception {
    final long counter = testCounter.incrementAndGet();
    final String classNamePrefix = name.getMethodName();
    Log.info("Created jar " + createAndLoadJar("", classNamePrefix, counter));

    ClassFinder.FileNameFilter notExcNameFilter = new ClassFinder.FileNameFilter() {
      @Override
      public boolean isCandidateFile(String fileName, String absFilePath) {
        return !fileName.startsWith(PREFIX);
      }
    };
    ClassFinder incClassesFinder = new ClassFinder(null, notExcNameFilter, null);
    Set<Class<?>> incClasses = incClassesFinder.findClasses(
        makePackageName("", counter), false);
    assertEquals(1, incClasses.size());
    Class<?> incClass = makeClass("", classNamePrefix, counter);
    assertTrue(incClasses.contains(incClass));
  }

  @Test
  public void testClassFinderFiltersByClassInJar() throws Exception {
    final long counter = testCounter.incrementAndGet();
    final String classNamePrefix = name.getMethodName();
    Log.info("Created jar " + createAndLoadJar("", classNamePrefix, counter));

    final ClassFinder.ClassFilter notExcClassFilter = new ClassFinder.ClassFilter() {
      @Override
      public boolean isCandidateClass(Class<?> c) {
        return !c.getSimpleName().startsWith(PREFIX);
      }
    };
    ClassFinder incClassesFinder = new ClassFinder(null, null, notExcClassFilter);
    Set<Class<?>> incClasses = incClassesFinder.findClasses(
        makePackageName("", counter), false);
    assertEquals(1, incClasses.size());
    Class<?> incClass = makeClass("", classNamePrefix, counter);
    assertTrue(incClasses.contains(incClass));
  }

  private static String createAndLoadJar(final String packageNameSuffix,
      final String classNamePrefix, final long counter)
  throws Exception {
    FileAndPath c1 = compileTestClass(counter, packageNameSuffix, classNamePrefix);
    FileAndPath c2 = compileTestClass(counter, packageNameSuffix, PREFIX + "1");
    FileAndPath c3 = compileTestClass(counter, packageNameSuffix, PREFIX + classNamePrefix + "2");
    return packageAndLoadJar(c1, c2, c3);
  }

  @Test
  public void testClassFinderFiltersByPathInJar() throws Exception {
    final String CLASSNAME = name.getMethodName();
    long counter = testCounter.incrementAndGet();
    FileAndPath c1 = compileTestClass(counter, "", CLASSNAME);
    FileAndPath c2 = compileTestClass(counter, "", "c2");
    packageAndLoadJar(c1);
    final String excludedJar = packageAndLoadJar(c2);
    /* ResourcePathFilter will pass us the resourcePath as a path of a
     * URL from the classloader. For Windows, the ablosute path and the
     * one from the URL have different file separators.
     */
    final String excludedJarResource =
      new File(excludedJar).toURI().getRawSchemeSpecificPart();

    final ClassFinder.ResourcePathFilter notExcJarFilter =
        new ClassFinder.ResourcePathFilter() {
      @Override
      public boolean isCandidatePath(String resourcePath, boolean isJar) {
        return !isJar || !resourcePath.equals(excludedJarResource);
      }
    };
    ClassFinder incClassesFinder = new ClassFinder(notExcJarFilter, null, null);
    Set<Class<?>> incClasses = incClassesFinder.findClasses(
        makePackageName("", counter), false);
    assertEquals(1, incClasses.size());
    Class<?> incClass = makeClass("", CLASSNAME, counter);
    assertTrue(incClasses.contains(incClass));
  }

  @Test
  public void testClassFinderCanFindClassesInDirs() throws Exception {
    // Make some classes for us to find.  Class naming and packaging is kinda cryptic.
    // TODO: Fix.
    final long counter = testCounter.incrementAndGet();
    final String classNamePrefix = name.getMethodName();
    String pkgNameSuffix = name.getMethodName();
    Log.info("Created jar " + createAndLoadJar(pkgNameSuffix, classNamePrefix, counter));
    ClassFinder allClassesFinder = new ClassFinder();
    String pkgName = makePackageName(pkgNameSuffix, counter);
    Set<Class<?>> allClasses = allClassesFinder.findClasses(pkgName, false);
    assertTrue("Classes in " + pkgName, allClasses.size() > 0);
    String classNameToFind = classNamePrefix + counter;
    assertTrue(contains(allClasses, classNameToFind));
  }

  private static boolean contains(final Set<Class<?>> classes, final String simpleName) {
    for (Class<?> c: classes) {
      if (c.getSimpleName().equals(simpleName)) return true;
    }
    return false;
  }

  @Test
  public void testClassFinderFiltersByNameInDirs() throws Exception {
    // Make some classes for us to find.  Class naming and packaging is kinda cryptic.
    // TODO: Fix.
    final long counter = testCounter.incrementAndGet();
    final String classNamePrefix = name.getMethodName();
    String pkgNameSuffix = name.getMethodName();
    Log.info("Created jar " + createAndLoadJar(pkgNameSuffix, classNamePrefix, counter));
    final String classNameToFilterOut = classNamePrefix + counter;
    final ClassFinder.FileNameFilter notThisFilter = new ClassFinder.FileNameFilter() {
      @Override
      public boolean isCandidateFile(String fileName, String absFilePath) {
        return !fileName.equals(classNameToFilterOut + ".class");
      }
    };
    String pkgName = makePackageName(pkgNameSuffix, counter);
    ClassFinder allClassesFinder = new ClassFinder();
    Set<Class<?>> allClasses = allClassesFinder.findClasses(pkgName, false);
    assertTrue("Classes in " + pkgName, allClasses.size() > 0);
    ClassFinder notThisClassFinder = new ClassFinder(null, notThisFilter, null);
    Set<Class<?>> notAllClasses = notThisClassFinder.findClasses(pkgName, false);
    assertFalse(contains(notAllClasses, classNameToFilterOut));
    assertEquals(allClasses.size() - 1, notAllClasses.size());
  }

  @Test
  public void testClassFinderFiltersByClassInDirs() throws Exception {
    // Make some classes for us to find.  Class naming and packaging is kinda cryptic.
    // TODO: Fix.
    final long counter = testCounter.incrementAndGet();
    final String classNamePrefix = name.getMethodName();
    String pkgNameSuffix = name.getMethodName();
    Log.info("Created jar " + createAndLoadJar(pkgNameSuffix, classNamePrefix, counter));
    final Class<?> clazz = makeClass(pkgNameSuffix, classNamePrefix, counter);
    final ClassFinder.ClassFilter notThisFilter = new ClassFinder.ClassFilter() {
      @Override
      public boolean isCandidateClass(Class<?> c) {
        return c != clazz;
      }
    };
    String pkgName = makePackageName(pkgNameSuffix, counter);
    ClassFinder allClassesFinder = new ClassFinder();
    Set<Class<?>> allClasses = allClassesFinder.findClasses(pkgName, false);
    assertTrue("Classes in " + pkgName, allClasses.size() > 0);
    ClassFinder notThisClassFinder = new ClassFinder(null, null, notThisFilter);
    Set<Class<?>> notAllClasses = notThisClassFinder.findClasses(pkgName, false);
    assertFalse(contains(notAllClasses, clazz.getSimpleName()));
    assertEquals(allClasses.size() - 1, notAllClasses.size());
  }

  @Test
  public void testClassFinderFiltersByPathInDirs() throws Exception {
    final String hardcodedThisSubdir = "hbase-common";
    final ClassFinder.ResourcePathFilter notExcJarFilter =
        new ClassFinder.ResourcePathFilter() {
      @Override
      public boolean isCandidatePath(String resourcePath, boolean isJar) {
        return isJar || !resourcePath.contains(hardcodedThisSubdir);
      }
    };
    String thisPackage = this.getClass().getPackage().getName();
    ClassFinder notThisClassFinder = new ClassFinder(notExcJarFilter, null, null);
    Set<Class<?>> notAllClasses = notThisClassFinder.findClasses(thisPackage, false);
    assertFalse(notAllClasses.contains(this.getClass()));
  }

  @Test
  public void testClassFinderDefaultsToOwnPackage() throws Exception {
    // Correct handling of nested packages is tested elsewhere, so here we just assume
    // pkgClasses is the correct answer that we don't have to check.
    ClassFinder allClassesFinder = new ClassFinder();
    Set<Class<?>> pkgClasses = allClassesFinder.findClasses(
        ClassFinder.class.getPackage().getName(), false);
    Set<Class<?>> defaultClasses = allClassesFinder.findClasses(false);
    assertArrayEquals(pkgClasses.toArray(), defaultClasses.toArray());
  }

  private static class FileAndPath {
    String path;
    File file;
    public FileAndPath(String path, File file) {
      this.file = file;
      this.path = path;
    }
  }

  private static Class<?> makeClass(String nestedPkgSuffix,
      String className, long counter) throws ClassNotFoundException {
    return Class.forName(
        makePackageName(nestedPkgSuffix, counter) + "." + className + counter);
  }

  private static String makePackageName(String nestedSuffix, long counter) {
    return BASEPKG + counter + nestedSuffix;
  }

  /**
   * Compiles the test class with bogus code into a .class file.
   * Unfortunately it's very tedious.
   * @param counter Unique test counter.
   * @param packageNameSuffix Package name suffix (e.g. ".suffix") for nesting, or "".
   * @return The resulting .class file and the location in jar it is supposed to go to.
   */
  private static FileAndPath compileTestClass(long counter,
      String packageNameSuffix, String classNamePrefix) throws Exception {
    classNamePrefix = classNamePrefix + counter;
    String packageName = makePackageName(packageNameSuffix, counter);
    String javaPath = basePath + classNamePrefix + ".java";
    String classPath = basePath + classNamePrefix + ".class";
    PrintStream source = new PrintStream(javaPath);
    source.println("package " + packageName + ";");
    source.println("public class " + classNamePrefix
        + " { public static void main(String[] args) { } };");
    source.close();
    JavaCompiler jc = ToolProvider.getSystemJavaCompiler();
    int result = jc.run(null, null, null, javaPath);
    assertEquals(0, result);
    File classFile = new File(classPath);
    assertTrue(classFile.exists());
    return new FileAndPath(packageName.replace('.', '/') + '/', classFile);
  }

  /**
   * Makes a jar out of some class files. Unfortunately it's very tedious.
   * @param filesInJar Files created via compileTestClass.
   * @return path to the resulting jar file.
   */
  private static String packageAndLoadJar(FileAndPath... filesInJar) throws Exception {
    // First, write the bogus jar file.
    String path = basePath + "jar" + jarCounter.incrementAndGet() + ".jar";
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    FileOutputStream fos = new FileOutputStream(path);
    JarOutputStream jarOutputStream = new JarOutputStream(fos, manifest);
    // Directory entries for all packages have to be added explicitly for
    // resources to be findable via ClassLoader. Directory entries must end
    // with "/"; the initial one is expected to, also.
    Set<String> pathsInJar = new HashSet<String>();
    for (FileAndPath fileAndPath : filesInJar) {
      String pathToAdd = fileAndPath.path;
      while (pathsInJar.add(pathToAdd)) {
        int ix = pathToAdd.lastIndexOf('/', pathToAdd.length() - 2);
        if (ix < 0) {
          break;
        }
        pathToAdd = pathToAdd.substring(0, ix);
      }
    }
    for (String pathInJar : pathsInJar) {
      jarOutputStream.putNextEntry(new JarEntry(pathInJar));
      jarOutputStream.closeEntry();
    }
    for (FileAndPath fileAndPath : filesInJar) {
      File file = fileAndPath.file;
      jarOutputStream.putNextEntry(
          new JarEntry(fileAndPath.path + file.getName()));
      byte[] allBytes = new byte[(int)file.length()];
      FileInputStream fis = new FileInputStream(file);
      fis.read(allBytes);
      fis.close();
      jarOutputStream.write(allBytes);
      jarOutputStream.closeEntry();
    }
    jarOutputStream.close();
    fos.close();

    // Add the file to classpath.
    File jarFile = new File(path);
    assertTrue(jarFile.exists());
    URLClassLoader urlClassLoader = (URLClassLoader)ClassLoader.getSystemClassLoader();
    Method method = URLClassLoader.class
        .getDeclaredMethod("addURL", new Class[] { URL.class });
    method.setAccessible(true);
    method.invoke(urlClassLoader, new Object[] { jarFile.toURI().toURL() });
    return jarFile.getAbsolutePath();
  }
};
