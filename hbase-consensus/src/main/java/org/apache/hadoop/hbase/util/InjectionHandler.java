/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The InjectionHandler is an object provided to a class,
 * which can perform custom actions for JUnit testing.
 * JUnit test can implement custom version of the handler.
 * For example, let's say we want to supervise FSImage object:
 *
 * <code>
 * // JUnit test code
 * class MyInjectionHandler extends InjectionHandler {
 *   protected void _processEvent(InjectionEvent event,
 *       Object... args) {
 *     if (event == InjectionEvent.MY_EVENT) {
 *       LOG.info("Handling my event for fsImage: "
 *         + args[0].toString());
 *     }
 *   }
 * }
 *
 * public void testMyEvent() {
 *   InjectionHandler ih = new MyInjectionHandler();
 *   InjectionHandler.set(ih);
 *   ...
 *
 *   InjectionHandler.clear();
 * }
 *
 * // supervised code example
 *
 * class FSImage {
 *
 *   private doSomething() {
 *     ...
 *     if (condition1 && InjectionHandler.trueCondition(MY_EVENT1) {
 *       ...
 *     }
 *     if (condition2 || condition3
 *       || InjectionHandler.falseCondition(MY_EVENT1) {
 *       ...
 *     }
 *     ...
 *     InjectionHandler.processEvent(MY_EVENT2, this)
 *     ...
 *     try {
 *       read();
 *       InjectionHandler.processEventIO(MY_EVENT3, this, object);
 *       // might throw an exception when testing
 *     catch (IOEXception) {
 *       LOG.info("Exception")
 *     }
 *     ...
 *   }
 *   ...
 * }
 * </code>
 *
 * Each unit test should use a unique event type.
 * The types can be defined by adding them to
 * InjectionEvent class.
 *
 * methods:
 *
 * // simulate actions
 * void processEvent()
 * // simulate exceptions
 * void processEventIO() throws IOException
 *
 * // simulate conditions
 * boolean trueCondition()
 * boolean falseCondition()
 *
 * The class implementing InjectionHandler must
 * override respective protected methods
 * _processEvent()
 * _processEventIO()
 * _trueCondition()
 * _falseCondition()
 */
public class InjectionHandler {

  private static final Log LOG = LogFactory.getLog(InjectionHandler.class);

  // the only handler to which everyone reports
  private static InjectionHandler handler = new InjectionHandler();

  // can not be instantiated outside, unless a testcase extends it
  protected InjectionHandler() {}

  // METHODS FOR PRODUCTION CODE

  protected void _processEvent(InjectionEvent event, Object... args) {
    // by default do nothing
  }

  protected void _processEventIO(InjectionEvent event, Object... args) throws IOException{
    // by default do nothing
  }

  protected boolean _trueCondition(InjectionEvent event, Object... args) {
    return true; // neutral in conjunction
  }

  protected boolean _falseCondition(InjectionEvent event, Object... args) {
    return false; // neutral in alternative
  }

  ////////////////////////////////////////////////////////////

  /**
   * Set to the empty/production implementation.
   */
  public static void clear() {
    handler = new InjectionHandler();
  }

  /**
   * Set custom implementation of the handler.
   */
  public static void set(InjectionHandler custom) {
    LOG.warn("WARNING: SETTING INJECTION HANDLER" +
      " - THIS SHOULD NOT BE USED IN PRODUCTION !!!");
    handler = custom;
  }

  /*
  * Static methods for reporting to the handler
  */

  public static void processEvent(InjectionEvent event, Object... args) {
    handler._processEvent(event, args);
  }

  public static void processEventIO(InjectionEvent event, Object... args)
    throws IOException {
    handler._processEventIO(event, args);
  }

  public static boolean trueCondition(InjectionEvent event, Object... args) {
    return handler._trueCondition(event, args);
  }

  public static boolean falseCondition(InjectionEvent event, Object... args) {
    return handler._falseCondition(event, args);
  }
}

