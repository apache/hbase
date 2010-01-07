/*
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * Utilities for JMX.
 */
public final class JmxHelper {
  static final Log LOG = LogFactory.getLog(JmxHelper.class);


  private JmxHelper() {
    // private constuctor for utility classes.
  }

  /**
   * Registers an MBean with the platform MBean server. if an MBean with the
   * same name exists it will be unregistered and the provided MBean would
   * replace it
   *
   * @param objectName the object name
   * @param mbean      the mbean class
   */
  public static void registerMBean(ObjectName objectName, Object mbean) {
    final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    if (mbs.isRegistered(objectName)) {
      try {
        LOG.info("unregister: "+objectName);
        mbs.unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        throw new IllegalStateException("mbean " + objectName +
          " failed unregistration", e);
      } catch (MBeanRegistrationException e) {
        throw new IllegalStateException("mbean " + objectName +
          " failed unregistration", e);
      }
    }
    try {
      LOG.info("register: " + objectName);
      mbs.registerMBean(mbean, objectName);
    } catch (InstanceAlreadyExistsException e) {
      throw new IllegalStateException("mbean " + objectName +
        " failed registration", e);
    } catch (MBeanRegistrationException e) {
      throw new IllegalStateException("mbean " + objectName +
        " failed registration", e);
    } catch (NotCompliantMBeanException e) {
      throw new IllegalStateException("mbean " + objectName +
        " failed registration", e);
    }
  }
}
