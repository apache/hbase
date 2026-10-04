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
package org.apache.hadoop.hbase.backup.mapreduce;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpConstants;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class InputFileListingGenerator {

  public static Path createInputFileListing(DistCp distCp, Job job, Configuration conf,
    Path fileListingPath, int levelsToPreserve) throws IOException {
    long totalBytesExpected = 0;
    int totalRecords = 0;
    try (SequenceFile.Writer writer = getWriter(fileListingPath, conf)) {
      List<Path> srcFiles = getSourceFiles(distCp);
      if (srcFiles.size() == 0) {
        return fileListingPath;
      }
      totalRecords = srcFiles.size();
      FileSystem fs = srcFiles.get(0).getFileSystem(conf);
      for (Path path : srcFiles) {
        FileStatus fst = fs.getFileStatus(path);
        totalBytesExpected += fst.getLen();
        Text key = getKey(path, levelsToPreserve);
        writer.append(key, new CopyListingFileStatus(fst));
      }
      writer.close();

      // update jobs configuration

      Configuration cfg = job.getConfiguration();
      cfg.setLong(DistCpConstants.CONF_LABEL_TOTAL_BYTES_TO_BE_COPIED, totalBytesExpected);
      cfg.set(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH, fileListingPath.toString());
      cfg.setLong(DistCpConstants.CONF_LABEL_TOTAL_NUMBER_OF_RECORDS, totalRecords);
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
      | IllegalAccessException | NoSuchMethodException | ClassNotFoundException
      | InvocationTargetException e) {
      throw new IOException(e);
    }
    return fileListingPath;
  }

  @SuppressWarnings("unchecked")
  public static List<Path> getSourcePaths(DistCp distCp, Field fieldInputOptions)
    throws IOException {
    Object options;
    try {
      options = fieldInputOptions.get(distCp);
      if (options instanceof DistCpOptions) {
        return ((DistCpOptions) options).getSourcePaths();
      } else {
        // Hadoop 3
        Class<?> classContext = Class.forName("org.apache.hadoop.tools.DistCpContext");
        Method methodGetSourcePaths = classContext.getDeclaredMethod("getSourcePaths");
        methodGetSourcePaths.setAccessible(true);

        return (List<Path>) methodGetSourcePaths.invoke(options);
      }
    } catch (IllegalArgumentException | IllegalAccessException | ClassNotFoundException
      | NoSuchMethodException | SecurityException | InvocationTargetException e) {
      throw new IOException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Path> getSourceFiles(DistCp distCp) throws NoSuchFieldException,
    SecurityException, IllegalArgumentException, IllegalAccessException, NoSuchMethodException,
    ClassNotFoundException, InvocationTargetException, IOException {
    Field options = null;
    try {
      options = DistCp.class.getDeclaredField("inputOptions");
    } catch (NoSuchFieldException | SecurityException e) {
      options = DistCp.class.getDeclaredField("context");
    }
    options.setAccessible(true);
    return getSourcePaths(distCp, options);
  }

  private static SequenceFile.Writer getWriter(Path pathToListFile, Configuration conf)
    throws IOException {
    FileSystem fs = pathToListFile.getFileSystem(conf);
    fs.delete(pathToListFile, false);
    return SequenceFile.createWriter(conf, SequenceFile.Writer.file(pathToListFile),
      SequenceFile.Writer.keyClass(Text.class),
      SequenceFile.Writer.valueClass(CopyListingFileStatus.class),
      SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
  }

  private static Text getKey(Path path, int level) {
    int count = 0;
    String relPath = "";
    while (count++ < level) {
      relPath = Path.SEPARATOR + path.getName() + relPath;
      path = path.getParent();
    }
    return new Text(relPath);
  }

}
