package org.apache.hadoop.hbase.regionserver.compactions;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;

@InterfaceAudience.Private
public class ExploringCompactionPolicy extends RatioBasedCompactionPolicy {

  public ExploringCompactionPolicy(Configuration conf,
                                   StoreConfigInformation storeConfigInfo) {
    super(conf, storeConfigInfo);
  }

  @Override
  ArrayList<StoreFile> applyCompactionPolicy(ArrayList<StoreFile> candidates,
                                             boolean mayUseOffPeak) throws IOException {
    // Start off choosing nothing.
    List<StoreFile> bestSelection = new ArrayList<StoreFile>(0);
    long bestSize = 0;

    // Consider every starting place.
    for (int start = 0; start < candidates.size(); start++) {
      // Consider every different sub list permutation in between start and end with min files.
      for(int currentEnd = start + comConf.getMinFilesToCompact() - 1;
          currentEnd < candidates.size(); currentEnd++) {
        List<StoreFile> potentialMatchFiles = candidates.subList(start, currentEnd+1);

        // Sanity checks
        if (potentialMatchFiles.size() < comConf.getMinFilesToCompact()) continue;
        if (potentialMatchFiles.size() > comConf.getMaxFilesToCompact()) continue;
        if (!filesInRatio(potentialMatchFiles, mayUseOffPeak)) continue;

        // Compute the total size of files that will
        // have to be read if this set of files is compacted.
        long size = 0;

        for (StoreFile s:potentialMatchFiles) {
          size += s.getReader().length();
        }

        // Keep if this gets rid of more files.  Or the same number of files for less io.
        if (potentialMatchFiles.size() > bestSelection.size() ||
            (potentialMatchFiles.size() == bestSelection.size() && size < bestSize)) {
          bestSelection = potentialMatchFiles;
          bestSize = size;
        }
      }
    }

    return new ArrayList<StoreFile>(bestSelection);
  }

  /**
   * Check that all files satisfy the r
   * @param files
   * @return
   */
  private boolean filesInRatio(List<StoreFile> files, boolean isOffPeak) {
    if (files.size() < 2) {
      return  true;
    }
    double currentRatio = isOffPeak ?
                          comConf.getCompactionRatioOffPeak() : comConf.getCompactionRatio();

    long totalFileSize = 0;
    for (int i = 0; i < files.size(); i++) {
      totalFileSize += files.get(i).getReader().length();
    }
    for (int i = 0; i < files.size(); i++) {
      long singleFileSize = files.get(i).getReader().length();
      long sumAllOtherFilesize = totalFileSize - singleFileSize;

      if (( singleFileSize >  sumAllOtherFilesize * currentRatio)
          && (sumAllOtherFilesize >= comConf.getMinCompactSize())){
        return false;
      }
    }

    return true;

  }
}
