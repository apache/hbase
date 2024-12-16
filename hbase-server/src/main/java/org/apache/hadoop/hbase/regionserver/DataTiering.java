package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.io.hfile.HFileInfo;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface DataTiering {
    long getTimestamp(HStoreFile hFile);
    long getTimestamp(HFileInfo hFileInfo);

}
