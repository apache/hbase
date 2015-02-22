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
package org.apache.hadoop.hbase.procedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Provides the common setup framework and runtime services for globally
 * barriered procedure invocation from HBase services.
 * @param <E> the specific procedure management extension that a concrete
 * implementation provides
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class ProcedureManagerHost<E extends ProcedureManager> {

  public static final String REGIONSERVER_PROCEDURE_CONF_KEY =
      "hbase.procedure.regionserver.classes";
  public static final String MASTER_PROCEUDRE_CONF_KEY =
      "hbase.procedure.master.classes";

  private static final Log LOG = LogFactory.getLog(ProcedureManagerHost.class);

  protected Set<E> procedures = new HashSet<E>();

  /**
   * Load system procedures. Read the class names from configuration.
   * Called by constructor.
   */
  protected void loadUserProcedures(Configuration conf, String confKey) {
    Class<?> implClass = null;

    // load default procedures from configure file
    String[] defaultProcClasses = conf.getStrings(confKey);
    if (defaultProcClasses == null || defaultProcClasses.length == 0)
      return;

    List<E> configured = new ArrayList<E>();
    for (String className : defaultProcClasses) {
      className = className.trim();
      ClassLoader cl = this.getClass().getClassLoader();
      Thread.currentThread().setContextClassLoader(cl);
      try {
        implClass = cl.loadClass(className);
        configured.add(loadInstance(implClass));
        LOG.info("User procedure " + className + " was loaded successfully.");
      } catch (ClassNotFoundException e) {
        LOG.warn("Class " + className + " cannot be found. " +
            e.getMessage());
      } catch (IOException e) {
        LOG.warn("Load procedure " + className + " failed. " +
            e.getMessage());
      }
    }

    // add entire set to the collection
    procedures.addAll(configured);
  }

  @SuppressWarnings("unchecked")
  public E loadInstance(Class<?> implClass) throws IOException {
    // create the instance
    E impl;
    Object o = null;
    try {
      o = implClass.newInstance();
      impl = (E)o;
    } catch (InstantiationException e) {
      throw new IOException(e);
    } catch (IllegalAccessException e) {
      throw new IOException(e);
    }

    return impl;
  }

  // Register a procedure manager object
  public void register(E obj) {
    procedures.add(obj);
  }

  public Set<E> getProcedureManagers() {
    Set<E> returnValue = new HashSet<E>();
    for (E e: procedures) {
      returnValue.add(e);
    }
    return returnValue;
  }

  public abstract void loadProcedures(Configuration conf);
}
