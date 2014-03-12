package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.hadoop.hbase.HRegionInfo;

public interface RegionPlacementPolicy {

/**
 * Get the assignment plan for the new regions
 * @param regions
 * @param servers
 * @return the favored assignment plan for the regions.
 * @throws IOException
 */
  public AssignmentPlan getNewAssignmentPlan(HRegionInfo[] regions,
      AssignmentDomain domain) throws IOException;
  
  /**
   * Get the favored assignment plan for all the regions
   * @return the favored assignment plan for all the regions
   * @throws IOException
   */
  public AssignmentPlan getNewAssignmentPlan()
  throws IOException;
  
  /**
   * Get the existing assignment plan for all the regions
   * @return the existing favored assignment plan for all the regions
   * @throws IOException
   */
  public AssignmentPlan getExistingAssignmentPlan()
  throws IOException;
  
  /**
   * Update the favored assignment plan
   * @param plan
   * @throws IOException
   */
  public void updateAssignmentPlan(AssignmentPlan plan)
  throws IOException;
}
