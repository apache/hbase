/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Scanner;

/**
 * Allows adding jars dynamically and loading a specified class from the jar.
 *
 */
public class DynamicClassLoader {
  /**
   * Adds a jar to the system class loader
   *
   * @param s - the path of the jar file as a string
   * @throws IOException
   */
  public static void addFile(String s) throws IOException {
    File f = new File(s);
    if (!f.exists()) {
      throw new IOException("File does not exist, please check your path");
    }
    URL url = f.toURI().toURL();
    URLClassLoader sysloader = (URLClassLoader) ClassLoader
        .getSystemClassLoader();
    try {
      Method method = URLClassLoader.class.getDeclaredMethod("addURL",
          new Class[] { URL.class });
      method.setAccessible(true);
      method.invoke(sysloader, new Object[] { url });
    } catch (Throwable t) {
      t.printStackTrace();
      throw new IOException("Could not add URL to system classloader!");
    }
  }

  public static void load(String jarPath, String filePath) throws IOException,
      ClassNotFoundException {
    addFile(jarPath);
    ClassLoader.getSystemClassLoader().loadClass(filePath);
  }

  public static void main(String args[]) throws IOException, SecurityException,
      ClassNotFoundException, IllegalArgumentException, InstantiationException,
      IllegalAccessException, InvocationTargetException, NoSuchMethodException {
    System.out
        .println("Insert the absolute path to the jar file that will be added to the classpath:");
    Scanner s = new Scanner(System.in);
    String filePath = s.nextLine().trim();
    addFile(filePath);
    System.out
        .println("Insert the class name with path that should be loaded from the jar: (example org.mypackage.MyCompactionHook)");
    String loadClazz = s.nextLine().trim();
    ClassLoader.getSystemClassLoader().loadClass(loadClazz);
    System.out.println("Class " + loadClazz + " was successfully loaded!");
  }
}
