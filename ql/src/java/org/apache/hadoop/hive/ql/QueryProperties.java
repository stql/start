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

package org.apache.hadoop.hive.ql;

/**
 *
 * QueryProperties.
 *
 * A structure to contain features of a query that are determined
 * during parsing and may be useful for categorizing a query type
 *
 * These inlucde whether the query contains:
 * a join clause, a group by clause, an order by clause, a sort by
 * clause, a group by clause following a join clause, and whether
 * the query uses a script for mapping/reducing
 */
public class QueryProperties {

  boolean hasJoin = false;
  boolean hasGroupBy = false;
  boolean hasOrderBy = false;
  boolean hasSortBy = false;
  boolean hasJoinFollowedByGroupBy = false;
  boolean hasPTF = false;
  boolean hasWindowing = false;

  // does the query have a using clause
  boolean usesScript = false;

  boolean hasDistributeBy = false;
  boolean hasClusterBy = false;
  boolean mapJoinRemoved = false;
  boolean hasMapGroupBy = false;

  //Added by Qiang
  boolean hasProject = false;
  boolean hasVProject = false;

  boolean hasCoalesce = false;
  boolean hasMultipleTrack = false;
  boolean hasClosest = false;
  boolean hasExclusiveJoin = false;
  boolean hasIntersectJoin = false;
  boolean hasDiscretize = false;

  boolean hasPairLocationComp = false;


  public boolean hasJoin() {
    return hasJoin;
  }

  public void setHasJoin(boolean hasJoin) {
    this.hasJoin = hasJoin;
  }

  public boolean hasProject() {
    return hasProject;
  }

  public void setHasProject(boolean hasProject) {
    this.hasProject = hasProject;
  }

  public boolean hasVProject() {
    return hasVProject;
  }

  public void setHasVProject(boolean hasVProject) {
    this.hasVProject = hasVProject;
  }

  public boolean hasGroupBy() {
    return hasGroupBy;
  }

  public void setHasGroupBy(boolean hasGroupBy) {
    this.hasGroupBy = hasGroupBy;
  }

  public boolean hasOrderBy() {
    return hasOrderBy;
  }

  public void setHasOrderBy(boolean hasOrderBy) {
    this.hasOrderBy = hasOrderBy;
  }

  public boolean hasSortBy() {
    return hasSortBy;
  }

  public void setHasSortBy(boolean hasSortBy) {
    this.hasSortBy = hasSortBy;
  }

  public boolean hasJoinFollowedByGroupBy() {
    return hasJoinFollowedByGroupBy;
  }

  public void setHasJoinFollowedByGroupBy(boolean hasJoinFollowedByGroupBy) {
    this.hasJoinFollowedByGroupBy = hasJoinFollowedByGroupBy;
  }

  public boolean usesScript() {
    return usesScript;
  }

  public void setUsesScript(boolean usesScript) {
    this.usesScript = usesScript;
  }

  public boolean hasDistributeBy() {
    return hasDistributeBy;
  }

  public void setHasDistributeBy(boolean hasDistributeBy) {
    this.hasDistributeBy = hasDistributeBy;
  }

  public boolean hasClusterBy() {
    return hasClusterBy;
  }

  public void setHasClusterBy(boolean hasClusterBy) {
    this.hasClusterBy = hasClusterBy;
  }

  public boolean hasPTF() {
    return hasPTF;
  }

  public void setHasPTF(boolean hasPTF) {
    this.hasPTF = hasPTF;
  }

  public boolean hasWindowing() {
    return hasWindowing;
  }

  public void setHasWindowing(boolean hasWindowing) {
    this.hasWindowing = hasWindowing;
  }

  public boolean isMapJoinRemoved() {
    return mapJoinRemoved;
  }

  public void setMapJoinRemoved(boolean mapJoinRemoved) {
    this.mapJoinRemoved = mapJoinRemoved;
  }

  public boolean isHasMapGroupBy() {
    return hasMapGroupBy;
  }

  public void setHasMapGroupBy(boolean hasMapGroupBy) {
    this.hasMapGroupBy = hasMapGroupBy;
  }



  public void setHasCoalesce(boolean hasCoalesce) {
    this.hasCoalesce = hasCoalesce;
  }

  public boolean hasCoalesce() {
    return hasCoalesce;
  }

  public void setHasMultipleTrack(boolean hasMultipleTrack) {
    this.hasMultipleTrack = hasMultipleTrack;
  }

  public boolean hasMultipleTrack() {
    return hasMultipleTrack;
  }

  public void setHasClosest(boolean hasClosest) {
    this.hasClosest = hasClosest;
  }

  public boolean hasClosest() {
    return hasClosest;
  }

  public void setHasExclusiveJoin(boolean hasExclusiveJoin) {
    this.hasExclusiveJoin = hasExclusiveJoin;
  }

  public boolean hasExclusiveJoin() {
    return hasExclusiveJoin;
  }

  public void setHasIntersectJoin(boolean hasIntersectJoin) {
    this.hasIntersectJoin = hasIntersectJoin;
  }

  public boolean hasIntersectJoin() {
    return hasIntersectJoin;
  }

  public void setHasDiscretize(boolean hasDiscretize) {
    this.hasDiscretize = hasDiscretize;
  }

  public boolean hasDiscretize() {
    return hasDiscretize;
  }

  public void setHasPairLocationComp(boolean hasPairLocationComp) {
    this.hasPairLocationComp = hasPairLocationComp;
  }

  public boolean hasPairLocationComp() {
    return hasPairLocationComp;
  }

}
