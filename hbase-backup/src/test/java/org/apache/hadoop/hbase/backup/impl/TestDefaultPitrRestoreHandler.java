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
package org.apache.hadoop.hbase.backup.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.backup.BackupInfo;
import org.apache.hadoop.hbase.backup.BackupInfo.BackupState;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.mockito.MockedConstruction;

@Tag(SmallTests.TAG)
public class TestDefaultPitrRestoreHandler
  extends TestAbstractPitrRestoreHandlerGetBackupMetadata {

  @Override
  protected List<PitrBackupMetadata> callGetBackupMetadataWithBackupStates(BackupState... states)
    throws IOException {
    List<BackupInfo> allInfos = new ArrayList<>();
    for (int i = 0; i < states.length; i++) {
      BackupInfo info = getBackupInfo(states[i]);
      info.setBackupId("backup_" + i);
      allInfos.add(info);
    }

    try (
      MockedConstruction<BackupSystemTable> mockedConstruction =
        mockConstruction(BackupSystemTable.class, (mockSysTable, context) -> {
          when(mockSysTable.getBackupHistory(any())).thenAnswer(invocation -> {
            Predicate<BackupInfo> combined = Arrays.stream(invocation.getArguments())
              .map(arg -> (Predicate<BackupInfo>) arg).reduce(Predicate::and).orElse(x -> true);
            return allInfos.stream().filter(combined).collect(Collectors.toList());
          });
        })) {
      DefaultPitrRestoreHandler handler = new DefaultPitrRestoreHandler(mockConn, request);
      return handler.getBackupMetadata(request);
    }
  }
}
