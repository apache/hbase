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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/*
* Test rules to prevent System.exit or Runtime.halt from exiting
* the JVM - instead an exception is thrown.
* */
public class SystemExitRule implements TestRule {
  final static SecurityManager securityManager = new TestSecurityManager();

  @Override
  public Statement apply(final Statement s, Description d) {
    return new Statement() {
      @Override public void evaluate() throws Throwable {

        try {
          forbidSystemExitCall();
          s.evaluate();
        } finally {
          System.setSecurityManager(null);
        }
      }

    };
  };

   // Exiting the JVM is not allowed in tests and this exception is thrown instead
   // when it is done
  public static class SystemExitInTestException extends SecurityException {
  }

  private static void forbidSystemExitCall() {
    System.setSecurityManager(securityManager);
  }
}
