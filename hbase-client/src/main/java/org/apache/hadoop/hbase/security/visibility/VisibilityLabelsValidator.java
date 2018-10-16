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
package org.apache.hadoop.hbase.security.visibility;

import java.util.regex.Pattern;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A simple validator that validates the labels passed
 */
@InterfaceAudience.Private
public class VisibilityLabelsValidator {
  private static final boolean[] validAuthChars = new boolean[256];

  public static final String regex = "[A-Za-z_\\-\\:\\/\\.0-9]+";
  public static final Pattern pattern = Pattern.compile(regex);

  static {
    for (int i = 0; i < 256; i++) {
      validAuthChars[i] = false;
    }

    for (int i = 'a'; i <= 'z'; i++) {
      validAuthChars[i] = true;
    }

    for (int i = 'A'; i <= 'Z'; i++) {
      validAuthChars[i] = true;
    }

    for (int i = '0'; i <= '9'; i++) {
      validAuthChars[i] = true;
    }

    validAuthChars['_'] = true;
    validAuthChars['-'] = true;
    validAuthChars[':'] = true;
    validAuthChars['.'] = true;
    validAuthChars['/'] = true;
  }
  
  static final boolean isValidAuthChar(byte b) {
    return validAuthChars[0xff & b];
  }

  public static final boolean isValidLabel(byte[] label) {
    for (int i = 0; i < label.length; i++) {
      if (!isValidAuthChar(label[i])) {
        return false;
      }
    }
    return true;
  }
}
