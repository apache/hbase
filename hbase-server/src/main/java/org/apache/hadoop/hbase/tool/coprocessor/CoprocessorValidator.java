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

package org.apache.hadoop.hbase.tool.coprocessor;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.tool.PreUpgradeValidator;
import org.apache.hadoop.hbase.tool.coprocessor.CoprocessorViolation.Severity;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class CoprocessorValidator extends AbstractHBaseTool {
  private static final Logger LOG = LoggerFactory
      .getLogger(CoprocessorValidator.class);

  private CoprocessorMethods branch1;
  private CoprocessorMethods current;

  private boolean dieOnWarnings;
  private boolean scan;
  private List<String> args;

  public CoprocessorValidator() {
    branch1 = new Branch1CoprocessorMethods();
    current = new CurrentCoprocessorMethods();
  }

  /**
   * This classloader implementation calls {@link #resolveClass(Class)}
   * method for every loaded class. It means that some extra validation will
   * take place <a
   * href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-12.html#jls-12.3">
   * according to JLS</a>.
   */
  private static final class ResolverUrlClassLoader extends URLClassLoader {
    private ResolverUrlClassLoader(URL[] urls) {
      super(urls, ResolverUrlClassLoader.class.getClassLoader());
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      return loadClass(name, true);
    }
  }

  private ResolverUrlClassLoader createClassLoader(URL[] urls) {
    return AccessController.doPrivileged(new PrivilegedAction<ResolverUrlClassLoader>() {
      @Override
      public ResolverUrlClassLoader run() {
        return new ResolverUrlClassLoader(urls);
      }
    });
  }

  private void validate(ClassLoader classLoader, String className,
      List<CoprocessorViolation> violations) {
    LOG.debug("Validating class '{}'.", className);

    try {
      Class<?> clazz = classLoader.loadClass(className);

      for (Method method : clazz.getDeclaredMethods()) {
        LOG.trace("Validating method '{}'.", method);

        if (branch1.hasMethod(method) && !current.hasMethod(method)) {
          CoprocessorViolation violation = new CoprocessorViolation(Severity.WARNING,
              "Method '" + method + "' was removed from new coprocessor API, "
                  + "so it won't be called by HBase.");
          violations.add(violation);
        }
      }
    } catch (ClassNotFoundException e) {
      CoprocessorViolation violation = new CoprocessorViolation(Severity.ERROR,
          "No such class '" + className + "'.", e);
      violations.add(violation);
    } catch (RuntimeException | Error e) {
      CoprocessorViolation violation = new CoprocessorViolation(Severity.ERROR,
          "Could not validate class '" + className + "'.", e);
      violations.add(violation);
    }
  }

  public List<CoprocessorViolation> validate(ClassLoader classLoader, List<String> classNames) {
    List<CoprocessorViolation> violations = new ArrayList<>();

    for (String className : classNames) {
      validate(classLoader, className, violations);
    }

    return violations;
  }

  public List<CoprocessorViolation> validate(List<URL> urls, List<String> classNames)
      throws IOException {
    URL[] urlArray = new URL[urls.size()];
    urls.toArray(urlArray);

    try (ResolverUrlClassLoader classLoader = createClassLoader(urlArray)) {
      return validate(classLoader, classNames);
    }
  }

  @VisibleForTesting
  protected List<String> getJarClasses(Path path) throws IOException {
    try (JarFile jarFile = new JarFile(path.toFile())) {
      return jarFile.stream()
          .map(JarEntry::getName)
          .filter((name) -> name.endsWith(".class"))
          .map((name) -> name.substring(0, name.length() - 6).replace('/', '.'))
          .collect(Collectors.toList());
    }
  }

  @VisibleForTesting
  protected List<String> filterObservers(ClassLoader classLoader,
      Iterable<String> classNames) throws ClassNotFoundException {
    List<String> filteredClassNames = new ArrayList<>();

    for (String className : classNames) {
      LOG.debug("Scanning class '{}'.", className);

      Class<?> clazz = classLoader.loadClass(className);

      if (Coprocessor.class.isAssignableFrom(clazz)) {
        LOG.debug("Found coprocessor class '{}'.", className);
        filteredClassNames.add(className);
      }
    }

    return filteredClassNames;
  }

  @Override
  protected void printUsage() {
    String header = "hbase " + PreUpgradeValidator.TOOL_NAME + " " +
        PreUpgradeValidator.VALIDATE_CP_NAME + " <jar> -scan|<classes>";
    printUsage(header, "Options:", "");
  }

  @Override
  protected void addOptions() {
    addOptNoArg("e", "Treat warnings as errors.");
    addOptNoArg("scan", "Scan jar for observers.");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    scan = cmd.hasOption("scan");
    dieOnWarnings = cmd.hasOption("e");
    args = cmd.getArgList();
  }

  @Override
  protected int doWork() throws Exception {
    if (args.size() < 1) {
      System.err.println("Missing jar file.");
      printUsage();
      return EXIT_FAILURE;
    }

    String jar = args.get(0);

    if (args.size() == 1 && !scan) {
      throw new ParseException("Missing classes or -scan option.");
    } else if (args.size() > 1 && scan) {
      throw new ParseException("Can't use classes with -scan option.");
    }

    Path jarPath = Paths.get(jar);
    URL[] urls = new URL[] { jarPath.toUri().toURL() };

    List<CoprocessorViolation> violations;

    try (ResolverUrlClassLoader classLoader = createClassLoader(urls)) {
      List<String> classNames;

      if (scan) {
        List<String> jarClassNames = getJarClasses(jarPath);
        classNames = filterObservers(classLoader, jarClassNames);
      } else {
        classNames = args.subList(1, args.size());
      }

      violations = validate(classLoader, classNames);
    }

    boolean error = false;

    for (CoprocessorViolation violation : violations) {
      switch (violation.getSeverity()) {
        case WARNING:
          System.err.println("[WARNING] " + violation.getMessage());

          if (dieOnWarnings) {
            error = true;
          }

          break;
        case ERROR:
          System.err.println("[ERROR] " + violation.getMessage());
          error = true;

          break;
      }
    }

    return (error) ? EXIT_FAILURE : EXIT_SUCCESS;
  }
}
