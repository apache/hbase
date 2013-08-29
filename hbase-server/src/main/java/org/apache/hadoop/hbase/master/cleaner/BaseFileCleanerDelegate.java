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
package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.BaseConfigurable;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

/**
 * Base class for file cleaners which allows subclasses to implement a simple
 * isFileDeletable method (which used to be the FileCleanerDelegate contract).
 */
public abstract class BaseFileCleanerDelegate extends BaseConfigurable
implements FileCleanerDelegate {

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    return Iterables.filter(files, new Predicate<FileStatus>() {
      @Override
      public boolean apply(FileStatus file) {
        return isFileDeletable(file);
      }});
  }

  /**
   * Should the master delete the file or keep it?
   * @param fStat file status of the file to check
   * @return <tt>true</tt> if the file is deletable, <tt>false</tt> if not
   */
  protected abstract boolean isFileDeletable(FileStatus fStat);

}
