/**
 * Copyright 2012 The Apache Software Foundation
 *
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

import java.text.DecimalFormat;
import java.util.Calendar;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

/**
 * Control knobs for default compaction algorithm
 */
public class TierCompactionConfiguration extends CompactionConfiguration {

  private CompactionTier[] compactionTier;
  private boolean recentFirstOrder;
  private boolean tierBoundaryFixed = false;
  TierCompactionConfiguration(Configuration conf, Store store) {
    super(conf, store);

    String strPrefix = "hbase.hstore.compaction.";
    String strSchema = "tbl." + store.getHRegion().getTableDesc().getNameAsString()
                       + "cf." + store.getFamily().getNameAsString() + ".";
    String strDefault = "Default.";
    String strAttribute;
    // If value not set for family, use default family (by passing null).
    // If default value not set, use 1 tier.

    strAttribute = "NumCompactionTiers";
    compactionTier = new CompactionTier[
      conf.getInt(strPrefix + strSchema  + strAttribute,
      conf.getInt(strPrefix + strDefault + strAttribute,
      1))];

    strAttribute = "IsRecentFirstOrder";
    recentFirstOrder =
      conf.getBoolean(strPrefix + strSchema  + strAttribute,
      conf.getBoolean(strPrefix + strDefault + strAttribute,
      true));

    strAttribute = "MinCompactSize";
    minCompactSize =
      conf.getLong(strPrefix + strSchema  + strAttribute,
      conf.getLong(strPrefix + strDefault + strAttribute,
      0));

    strAttribute = "MaxCompactSize";
    maxCompactSize =
      conf.getLong(strPrefix + strSchema  + strAttribute,
      conf.getLong(strPrefix + strDefault + strAttribute,
      Long.MAX_VALUE));

    strAttribute = "ShouldExcludeBulk";
    shouldExcludeBulk =
      conf.getBoolean(strPrefix + strSchema  + strAttribute,
      conf.getBoolean(strPrefix + strDefault + strAttribute,
      shouldExcludeBulk));

    strAttribute = "ShouldDeleteExpired";
    shouldDeleteExpired =
      conf.getBoolean(strPrefix + strSchema  + strAttribute,
      conf.getBoolean(strPrefix + strDefault + strAttribute,
      shouldDeleteExpired));

    strAttribute = "ThrottlePoint";
    throttlePoint =
      conf.getLong(strPrefix + strSchema  + strAttribute,
      conf.getLong(strPrefix + strDefault + strAttribute,
      throttlePoint));

    strAttribute = "MajorCompactionPeriod";
    majorCompactionPeriod =
      conf.getLong(strPrefix + strSchema  + strAttribute,
      conf.getLong(strPrefix + strDefault + strAttribute,
      majorCompactionPeriod));

    strAttribute = "MajorCompactionJitter";
    majorCompactionJitter =
      conf.getFloat(
          strPrefix + strSchema + strAttribute,
          conf.getFloat(
              strPrefix + strDefault + strAttribute,
              majorCompactionJitter
          )
      );
    
    // This allows us to compact files in hourly, daily boundary and etc
    strAttribute = "IsTierBoundaryFixed";
    tierBoundaryFixed = conf.getBoolean(strPrefix + strSchema + strAttribute,
        conf.getBoolean(strPrefix + strDefault + strAttribute, false));
    for (int i = 0; i < compactionTier.length; i++) {
      compactionTier[i] = new CompactionTier(i);
    }
  }
  /**
   * @return Number of compaction Tiers
   */
  int getNumCompactionTiers() {
    return compactionTier.length;
  }

  /**
   * @return The i-th tier from most recent
   */
  CompactionTier getCompactionTier(int i) {
    return compactionTier[i];
  }

  /**
   * @return Whether the tiers will be checked for compaction from newest to oldest
   */
  boolean isRecentFirstOrder() {
    return recentFirstOrder;
  }

  /**
   * @return Whether the tiers will be compacted based on fixed boundary
   */
  boolean isTierBoundaryFixed() {
    return tierBoundaryFixed;
  }
  /**
   * Parameters for each tier
   */
  class CompactionTier {

    private long maxAgeInDisk;
    private long maxSize;
    private double tierCompactionRatio;
    private int tierMinFilesToCompact;
    private int tierMaxFilesToCompact;
    private int endingIndexForTier;
    private CronTierBoundary cronTierBoundary;
    
    CompactionTier(int tier) {
      String strPrefix = "hbase.hstore.compaction.";
      String strSchema = "tbl." + store.getHRegion().getTableDesc().getNameAsString()
                         + "cf." + store.getFamily().getNameAsString() + ".";
      String strDefault = "Default.";
      String strDefTier = "";
      String strTier = "Tier." + String.valueOf(tier) + ".";
      String strAttribute;

      /**
       * Use value set for current family, current tier
       * If not set, use value set for current family, default tier
       * if not set, use value set for Default family, current tier
       * If not set, use value set for Default family, default tier
       * Else just use a default value
       */

      strAttribute = "MaxAgeInDisk";
      maxAgeInDisk =
        conf.getLong(strPrefix + strSchema  + strTier + strAttribute,
        conf.getLong(strPrefix + strDefault + strTier + strAttribute,
        Long.MAX_VALUE));

      strAttribute = "MaxSize";
      maxSize =
        conf.getLong(strPrefix + strSchema  + strTier + strAttribute,
        conf.getLong(strPrefix + strDefault + strTier + strAttribute,
        Long.MAX_VALUE));

      strAttribute = "CompactionRatio";
      tierCompactionRatio = (double)
        conf.getFloat(strPrefix + strSchema  + strTier  + strAttribute,
        conf.getFloat(strPrefix + strSchema  + strDefTier + strAttribute,
        conf.getFloat(strPrefix + strDefault + strTier  + strAttribute,
        conf.getFloat(strPrefix + strDefault + strDefTier + strAttribute,
        (float) compactionRatio))));

      strAttribute = "MinFilesToCompact";
      tierMinFilesToCompact =
        conf.getInt(strPrefix + strSchema  + strTier  + strAttribute,
        conf.getInt(strPrefix + strSchema  + strDefTier + strAttribute,
        conf.getInt(strPrefix + strDefault + strTier  + strAttribute,
        conf.getInt(strPrefix + strDefault + strDefTier + strAttribute,
        minFilesToCompact))));

      strAttribute = "MaxFilesToCompact";
      tierMaxFilesToCompact =
        conf.getInt(strPrefix + strSchema  + strTier  + strAttribute,
        conf.getInt(strPrefix + strSchema  + strDefTier + strAttribute,
        conf.getInt(strPrefix + strDefault + strTier  + strAttribute,
        conf.getInt(strPrefix + strDefault + strDefTier + strAttribute,
        maxFilesToCompact))));

      strAttribute = "EndingIndexForTier";
      endingIndexForTier =
        conf.getInt(strPrefix + strSchema  + strTier + strAttribute,
        conf.getInt(strPrefix + strDefault + strTier + strAttribute,
        tier));

      //make sure this value is not incorrectly set
      if (endingIndexForTier < 0 || endingIndexForTier > tier) {
        LOG.error("EndingIndexForTier improperly set. Using default value.");
        endingIndexForTier = tier;
      }
      
      strAttribute = "Boundary";
        String cronExpression = conf.get(strPrefix + strSchema + strTier + strAttribute);
        if (cronExpression == null) {
          cronExpression = conf.get(strPrefix + strSchema + strDefTier + strAttribute);
          if (cronExpression == null) {
            cronExpression = conf.get(strPrefix + strDefault + strTier + strAttribute);
            if (cronExpression == null) {
              cronExpression = conf.get(strPrefix + strDefault + strDefTier + strAttribute);
          }
        }
      }
      if (cronExpression != null) {
        LOG.info("Tier " + tier + " has cron expression " + cronExpression);
        cronTierBoundary = new CronTierBoundary(cronExpression);
      } else {
        LOG.info("Tier " + tier + " has an empty cron expression");
        cronTierBoundary = new CronTierBoundary();
      }
    }

    /**
     * 
     * @return tierBoundary
     */
    Calendar getTierBoundary(Calendar prevTierBoundary) {
      return cronTierBoundary.getTierBoundary(prevTierBoundary);
    }

    /**
     * @return Upper bound on storeFile's minFlushTime to be included in this tier
     */
    long getMaxAgeInDisk() {
      return maxAgeInDisk;
    }

    /**
     * @return Upper bound on storeFile's size to be included in this tier
     */
    long getMaxSize() {
      return maxSize;
    }

    /**
     * @return Compaction ratio for selections of this tier
     */
    double getCompactionRatio() {
      return tierCompactionRatio;
    }

    /**
     * @return lower bound on number of files in selections of this tier
     */
    int getMinFilesToCompact() {
      return tierMinFilesToCompact;
    }

    /**
     * @return upper bound on number of files in selections of this tier
     */
    int getMaxFilesToCompact() {
      return tierMaxFilesToCompact;
    }

    /**
     * @return the newest tier which will also be included in selections of this tier
     *  by default it is the index of this tier, must be between 0 and this tier
     */
    int getEndingIndexForTier() {
      return endingIndexForTier;
    }

    String getDescription() {
      String ageString = "INF";
      String sizeString = "INF";
      if (getMaxAgeInDisk() < Long.MAX_VALUE) {
        ageString = StringUtils.formatTime(getMaxAgeInDisk());
      }
      if (getMaxSize() < Long.MAX_VALUE) {
        ageString = StringUtils.humanReadableInt(getMaxSize());
      }
      String ret = "Has files upto age " + ageString
          + " and upto size " + sizeString + ". "
          + "Compaction ratio: " + (new DecimalFormat("#.##")).format(getCompactionRatio()) + ", "
          + "Compaction Selection with at least " + getMinFilesToCompact() + " and "
          + "at most " + getMaxFilesToCompact() + " files possible, "
          + "Selections in this tier includes files up to tier " + getEndingIndexForTier();
      return ret;
    }

  }

  /*
   * * * * * * [num]                        Additional chars are allowed
   * _ _ _ _ _ _ 
   * | | | | | |___year (empty, 1970-2099)  empty, * or an explicit value
   * | | | | |_____day_of_week (0-6)        , - * / L # 
   * | | | |_______month (1-12)             , - * / ?
   * | | |_________day_of_month (1-31)      , - * / ? L 
   * | |___________hour_of_day (0-23)       , - * /
   * |_____________min (0-59)               , - * /
   *
   * Examples: 
   * "0 0 * * 5#3" :     3rd Friday of every month midnight
   * "0 0 1 * *" :       first day of every month midnight
   * "0 0 * * 5L" :      last Friday per month midnight
   * "0 0 1-7 * *":      every first 7 days per month
   * "0 0 * * 3,4":      every Wednesday, Thursday 
   * "0 0 8 7 * 2012" :  July 8th, 2012, midnight
   * Currently we only support only one "L" tagged value at day_of_month and
   * month fields since we can always make separate expressions to specify last
   * two or more days. 
   */

  public class CronTierBoundary {
    private static final int NUMBER_OF_FIELDS = 5;
    private static final int MINUTE = 0;
    private static final int HOUR_OF_DAY = 1;
    private static final int DAY_OF_MONTH = 2;
    private static final int MONTH = 3;
    private static final int DAY_OF_WEEK = 4;
    private static final int YEAR = 5;
    private int numFields = 0;
    private final String[] fieldNames = { "MINUTE", "HOUR_OF_DAY",
        "DAY_OF_MONTH", "MONTH", "DAY_OF_WEEK" };
    private final int[] calIdx = { Calendar.MINUTE, Calendar.HOUR_OF_DAY,
        Calendar.DAY_OF_MONTH, Calendar.MONTH, Calendar.DAY_OF_WEEK,
        Calendar.YEAR };

    private TimeField[] fields;

    public CronTierBoundary() {
    }

    public CronTierBoundary(String inputString) {
      if (inputString.equals("")) {
        return;
      }
      String[] cronPhrases = inputString.split(" ");
      if (cronPhrases.length < NUMBER_OF_FIELDS
          || cronPhrases.length > (NUMBER_OF_FIELDS + 1)) {
        throw new IllegalArgumentException("Wrong CronExpression "
            + inputString);
      } else {
        numFields = cronPhrases.length;
        fields = new TimeField[numFields];
        for (int i = 0; i < numFields; i++) {
          fields[i] = new TimeField(cronPhrases[i], i);
        }
      }
    }

    /**
     * 
     * the main API used to find the next legitimate boundary
     * 
     * @param from when we find the available boundary
     * @return the next legitimate boundary
     */
    public Calendar getTierBoundary(Calendar currCal) {

      if (numFields == 0) {
        // default case: the boundary is 0, in this case 1970/1/1
        // clone() is much more efficient than getInstance()
        Calendar cal = (Calendar) currCal.clone();
        cal.setTimeInMillis(0);
        return cal;
      }
      return getNextAvail(currCal);
    }

    /**
     * 
     * the main function to find the next legitimate boundary
     * @param from when we find the available boundary
     * @return the next legitimate boundary
     */
    private Calendar getNextAvail(Calendar present) {
      // clone() is much more efficient than getInstance()
      Calendar nextCal = (Calendar) present.clone();
      
      int val, nextVal;
      // This will guarantee the time go backward at least at minute level
      boolean carry = true;
      // minute and hour are simple cases, only need to consider borrowing
      // from day or higher unit
      for (int i = 0; i < DAY_OF_MONTH; i++) {
        val = nextCal.get(calIdx[i]);
        nextVal = fields[i].getNext(val, nextCal, carry);
        nextCal.set(calIdx[i], nextVal);
        carry = (nextVal == val) ? carry : (val < nextVal);
      }

      // check whether the year is a single value
      // we don't deal with the multiple values for year although it could be
      // implemented. For simplicity, we just consider the three cases:
      // empty, or an explicit value
      if (numFields > NUMBER_OF_FIELDS) {
        if (fields[YEAR].getSize() != 1) {
          throw new IllegalArgumentException("Cron Expression inValid: "
              + " need either * or a particular year can be specified");
        }
        nextCal.set(Calendar.YEAR, fields[YEAR].values.first());
      }

      // special character "#" at DAY_OF_WEEK
      if (fields[DAY_OF_WEEK].hashId > 0) {
        int weekID = nextCal.get(Calendar.WEEK_OF_MONTH);
        int weekday = nextCal.get(Calendar.DAY_OF_WEEK);
        int weekDayDesired = fields[DAY_OF_WEEK].getNext(
            nextCal.get(Calendar.DAY_OF_WEEK), nextCal, false);
        // if in the current month the desired weekday is ahead
        // please roll back in month
        if (weekID < fields[DAY_OF_WEEK].hashId
            || (weekID == fields[DAY_OF_WEEK].hashId && weekday < weekDayDesired)) {
          carry = true;
        }
        // update month and year
        updateMonthYear(fields, nextCal, carry);
        // set the desired week and weekday
        nextCal.set(Calendar.DAY_OF_WEEK, weekDayDesired);
        val = nextCal.get(Calendar.MONTH);
        nextCal.set(Calendar.WEEK_OF_MONTH, fields[DAY_OF_WEEK].hashId);
        // when the first day_of_week in month greater than the desired one
        // it falls back to the previous month, to compensate it, we add it
        // back to the current month 
        if (nextCal.get(Calendar.MONTH) < val) {
          nextCal.add(Calendar.DAY_OF_MONTH, 7);
        }
      } else if (fields[DAY_OF_WEEK].isLast) {
        // special character "L" at DAY_OF_WEEK
        int dayOfMonth = nextCal.get(Calendar.DAY_OF_MONTH);
        int daysInMonth = nextCal.getActualMaximum(Calendar.DAY_OF_MONTH);
        nextCal.set(Calendar.DAY_OF_MONTH, daysInMonth);
        int dayOfWeekLast = nextCal.get(Calendar.DAY_OF_WEEK);
        int dayOfWeekDesired = fields[DAY_OF_WEEK].getLastValue();
        // If the desired day_of_week is the future compared with day_of_week
        // for the end of the current month, we need roll back in month
        if ((dayOfWeekLast - dayOfWeekDesired + 7) < (daysInMonth - dayOfMonth)
            || (carry && (dayOfWeekLast - dayOfWeekDesired + 7)
                         == (daysInMonth - dayOfMonth))) {
          updateMonthYear(fields, nextCal, true);
          nextCal.set(Calendar.DAY_OF_MONTH,
              nextCal.getActualMaximum(Calendar.DAY_OF_MONTH));
          dayOfWeekLast = nextCal.get(Calendar.DAY_OF_WEEK);
        }
        // move to the desired day_of_week
        nextCal.add(Calendar.DAY_OF_MONTH,
            -((dayOfWeekLast - dayOfWeekDesired + 7) % 7));

      } else if (fields[DAY_OF_MONTH].isOffset) {
        // special character "?" with DAY_OF_MONTH
        nextCal.add(calIdx[DAY_OF_MONTH], -fields[DAY_OF_MONTH].step);
        updateMonthYear(fields, nextCal, false);

      } else if (fields[DAY_OF_MONTH].isLast) {
        // special character "L" with DAY_OF_MONTH
        val = fields[DAY_OF_MONTH].getLastValue();
        int days = nextCal.getActualMaximum(Calendar.DAY_OF_MONTH);
        if (days - val + 1 > nextCal.get(DAY_OF_MONTH)) {
          carry = true;
        } 
        updateMonthYear(fields, nextCal, carry);
        days = nextCal.getActualMaximum(Calendar.DAY_OF_MONTH);
        nextCal.set(Calendar.DAY_OF_MONTH, days - val + 1);
      } else {
        // This process all the rest cases, such as comma, *, hyphen
        do {
          nextVal = 0;
          int oldNextVal = 0;
          for (int i = DAY_OF_MONTH; i <= MONTH; i++) {
            val = nextCal.get(calIdx[i]);
            oldNextVal = nextVal;
            nextVal = fields[i].getNext(val, nextCal, carry);
            carry = (nextVal == val) ? carry : (nextVal > val);
          }
          nextCal.set(calIdx[MONTH], nextVal);
          while (oldNextVal > nextCal.getActualMaximum(calIdx[DAY_OF_MONTH])) {
            oldNextVal = fields[DAY_OF_MONTH]
                .getNext(oldNextVal, nextCal, true);
          }
          nextCal.set(calIdx[DAY_OF_MONTH], oldNextVal);
          // If day_of_week is satisfied, then we've found what we want
          if (fields[DAY_OF_WEEK].values.contains(nextCal
              .get(Calendar.DAY_OF_WEEK))) {
            break;
          }
          carry = true;

        } while (true);
        // when we have roll over in month
        if (carry && numFields == NUMBER_OF_FIELDS) {
          nextCal.set(Calendar.YEAR, nextCal.get(Calendar.YEAR) - 1);
          carry = false;
        }
      }
      return nextCal;
    }

    // Helper function to update month/year
    private void updateMonthYear(TimeField[] fields, Calendar nextCal,
        boolean carry) {
      int val = nextCal.get(Calendar.MONTH);
      int nextVal = fields[MONTH].getNext(val, nextCal, carry);
      nextCal.add(Calendar.MONTH, nextVal - val);
    }
  
    // this is used for debugging 
    public String displayCronConfigure() {
      StringBuilder str = new StringBuilder();
      for (int i = 0; i < numFields - 1; i++) {
        str.append(fields[i].toString() + "\n");
      }
      str.append(fields[numFields - 1].toString());
      return str.toString();
    }

    class TimeField {
      private int field = 0;
      private int step = 1;
      private int start = 0;
      private int end = 0;
      private int hashId = -1;
      private int lastValue;
      SortedSet<Integer> values;
      
      private boolean isOffset = false;   // the indicator for ?/stepSize
      private boolean isLast;             // the indicator whether it starts from the 
                                          // end of the month or the end of the week 

      /*
       * convert the cron expression to java Calendar:
       * 
       * Java calendar defines the following: 
       *   month: Jan-Dec (0 - 11)
       *   day_of_week: Sun-Sat (1-7)
       * 
       * This is different from what is defined our cron expression, which has
       *   month: Jan-Dec (1 - 12) 
       *   day_of_week: Sun-Sat (0 - 6) 
       * therefore, we are going to convert our value range to java 
       * calendar accordingly
       */
      TimeField(String phrase, int idx) {
        field = idx;
        switch (idx) {
        case MINUTE:
          start = 0;
          end = 59;
          break;
        case HOUR_OF_DAY:
          start = 0;
          end = 23;
          break;
        case DAY_OF_MONTH:
          start = 1;
          end = 31;
          break;
        case MONTH:
          start = 1;
          end = 12;
          break;
        case DAY_OF_WEEK:
          start = 0;
          end = 6;
          break;
        case YEAR:
          start = 1969;
          end = Integer.MAX_VALUE;
          break;
        default:
          throw new IllegalArgumentException("Illegal Field Index number["
              + (DAY_OF_WEEK + 1) + "]: " + idx);
        }
        values = new TreeSet<Integer>();
        if (phrase != null) {
          parseField(phrase);
        }
      }

      // parse each field
      public void parseField(String phrase) {
        for (String word : phrase.split(",")) {
          String[] cronStep = word.split("/");
          if (cronStep.length > 2) {
            throw new IllegalArgumentException("multiple division \"/\": "
                + phrase);
          } else if (cronStep.length == 2) {
            try {
              step = Integer.valueOf(cronStep[1]);
            } catch (Exception e) {
              throw new IllegalArgumentException("Illegal denominator: "
                  + cronStep[1]);
            }
          } // end of step

          /* range */
          String startStr = null, endStr = null;
          if (!cronStep[0].equals("*") && !cronStep[0].equals("?")) {
            String[] cronRange = cronStep[0].split("-");
            if (cronRange.length > 2) {
              throw new IllegalArgumentException("multiple range \"-\": "
                  + cronStep[0]);
            } else if (cronRange.length == 2) {
              startStr = cronRange[0];
              endStr = cronRange[1];
            } else {
              try {
                startStr = cronRange[0];
                endStr = startStr;
              } catch (Exception e) {
                throw new IllegalArgumentException("Illegal range value");
              }
            }

            if (field == DAY_OF_WEEK) {
              String[] cronHash = cronStep[0].split("#");
              if (cronHash.length > 2) {
                throw new IllegalArgumentException("multiple hash \"#\": "
                    + cronStep[0]);
              } else if (cronHash.length == 2) {
                hashId = Integer.valueOf(cronHash[1]);
                startStr = cronHash[0];
                endStr = startStr;
              }
            }

            if ((field == DAY_OF_WEEK || field == DAY_OF_MONTH)
                && startStr != null && startStr.endsWith("L")) {
              isLast = true;
              if (endStr != null && !endStr.endsWith("L")) {
                throw new IllegalArgumentException("Illegal arguments: "
                    + startStr + ", " + endStr);
              }
              start = Integer.valueOf(startStr.split("L")[0]);
              end = Integer.valueOf(endStr.split("L")[0]);
            } else {
              if (startStr != null) {
                start = Integer.valueOf(startStr);
                end = Integer.valueOf(endStr);
              }
            }
          } // end of range
          if (cronStep[0].equals("?")) {
            isOffset = true;
          }
          // In Java calendar, Jan-Dec map to 0-11
          if (field == MONTH) {
            start--;
            end--;
          }
          // In java calendar, Sun-Sat map to 1-7
          if (field == DAY_OF_WEEK) {
            start++;
            end++;
          }
          if (isLast) {
            if (start != end) {
              throw new IllegalArgumentException(
                  "only one value can be specified here");
            }
            lastValue = start;
          } else {
            for (int i = start; i <= end; i += step) {
              values.add(i);
            }
          }
        } // end of phrase loop
      }

      /**
       * 
       * @return the value tagged by "L"
       */
      public int getLastValue() {
        return lastValue;
      }
      /**
       * 
       * @return the size of valid values at a given field
       */
      public int getSize() {
        return values != null ? values.size() : 1;
      }

      /**
       * 
       * To seek the next valid value before the present one
       * 
       * @param present the present value at field i
       * @param cal the valid Calendar so far
       * @param carry the carry bit
       * @return the next valid value before the present one
       */
      public int getNext(int present, Calendar cal, boolean carry) {
        SortedSet<Integer> fvalues = values;
        // reconstruct the value's treeset in order to 
        // accommodate the case: ?/step
        if (field == MONTH && isOffset) {
          fvalues = new TreeSet<Integer>();
          int val = cal.get(Calendar.MONTH);
          carry = true;
          for (int i = 0; i < 12 / step; i++) {
            fvalues.add((val - i * step + 12) % 12);
          }
        }
        if (!carry && fvalues.contains(present)) {
          return present;
        } else {
          // backward
          if (fvalues.size() == 1) {
            return fvalues.first();
          }
          if (fvalues.headSet(present).size() > 0)
            return fvalues.headSet(present).last();
          else
            return fvalues.last();
        }
      }

      public String toString() {
        StringBuilder sb = new StringBuilder(fieldNames[field] + ":\t");
        for (Integer val : values) {
          if (values.last() == val) {
            sb.append(val);
          } else {
            sb.append(val + ", ");
          }
        }
        
        if (isOffset) {
          sb.append("from now go back :" + step);
        }
        
        if (isLast && (field == DAY_OF_WEEK || field == DAY_OF_MONTH)) {
          sb.append(lastValue + "L");
        }
        return sb.toString();
      }
    }
  }
}
