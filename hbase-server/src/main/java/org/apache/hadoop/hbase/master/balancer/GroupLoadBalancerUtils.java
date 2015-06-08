package org.apache.hadoop.hbase.master.balancer;

public class GroupLoadBalancerUtils {

  private static final String NAME_DELIMITER = ",";

  public static String getServerNameWithoutStartCode(String serverName) {
    String[] serverNameArray = serverName.split(NAME_DELIMITER);
    return serverNameArray[0] + NAME_DELIMITER + serverNameArray[1];
  }

  public static String getTableNameFromRegionName(String regionName) {
    return regionName.split(NAME_DELIMITER)[0];
  }

}
