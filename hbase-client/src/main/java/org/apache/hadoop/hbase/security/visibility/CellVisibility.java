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
package org.apache.hadoop.hbase.security.visibility;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This contains a visibility expression which can be associated with a cell. When it is set with a
 * Mutation, all the cells in that mutation will get associated with this expression. A visibility
 * expression can contain visibility labels combined with logical
 * operators AND(&amp;), OR(|) and NOT(!)
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CellVisibility {

  private String expression;

  public CellVisibility(String expression) {
    this.expression = expression;
  }

  /**
   * @return The visibility expression
   */
  public String getExpression() {
    return this.expression;
  }

  @Override
  public String toString() {
    return this.expression;
  }

  /**
   * Helps in quoting authentication Strings. Use this if unicode characters to
   * be used in expression or special characters like '(', ')',
   * '"','\','&amp;','|','!'
   */
  public static String quote(String auth) {
    return quote(Bytes.toBytes(auth));
  }

  /**
   * Helps in quoting authentication Strings. Use this if unicode characters to
   * be used in expression or special characters like '(', ')',
   * '"','\','&amp;','|','!'
   */
  public static String quote(byte[] auth) {
    int escapeChars = 0;

    for (int i = 0; i < auth.length; i++)
      if (auth[i] == '"' || auth[i] == '\\')
        escapeChars++;

    byte[] escapedAuth = new byte[auth.length + escapeChars + 2];
    int index = 1;
    for (int i = 0; i < auth.length; i++) {
      if (auth[i] == '"' || auth[i] == '\\') {
        escapedAuth[index++] = '\\';
      }
      escapedAuth[index++] = auth[i];
    }

    escapedAuth[0] = '"';
    escapedAuth[escapedAuth.length - 1] = '"';

    return Bytes.toString(escapedAuth);
  }
}
