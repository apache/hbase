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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.master.cleaner.BaseHFileCleanerDelegate;

/**
 * HFile archive cleaner that just tells you if it has been run already or not (and allows resets) -
 * always attempts to delete the passed file.
 * <p>
 * Just a helper class for testing to make sure the cleaner has been run.
 */
public class CheckedArchivingHFileCleaner extends BaseHFileCleanerDelegate {

  private static boolean checked;

  @Override
  public boolean isFileDeletable(Path file) {
    checked = true;
    return true;
  }

  public static boolean getChecked() {
    return checked;
  }

  public static void resetCheck() {
    checked = false;
  }
}
