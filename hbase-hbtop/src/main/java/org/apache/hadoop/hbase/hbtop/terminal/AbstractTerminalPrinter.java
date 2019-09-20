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
package org.apache.hadoop.hbase.hbtop.terminal;

public abstract class AbstractTerminalPrinter implements TerminalPrinter {

  @Override
  public TerminalPrinter print(Object value) {
    print(value.toString());
    return this;
  }

  @Override
  public TerminalPrinter print(char value) {
    print(Character.toString(value));
    return this;
  }

  @Override
  public TerminalPrinter print(short value) {
    print(Short.toString(value));
    return this;
  }

  @Override
  public TerminalPrinter print(int value) {
    print(Integer.toString(value));
    return this;
  }

  @Override
  public TerminalPrinter print(long value) {
    print(Long.toString(value));
    return this;
  }

  @Override
  public TerminalPrinter print(float value) {
    print(Float.toString(value));
    return this;
  }

  @Override
  public TerminalPrinter print(double value) {
    print(Double.toString(value));
    return this;
  }

  @Override
  public TerminalPrinter printFormat(String format, Object... args) {
    print(String.format(format, args));
    return this;
  }
}
