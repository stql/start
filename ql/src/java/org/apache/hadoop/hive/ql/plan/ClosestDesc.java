
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

package org.apache.hadoop.hive.ql.plan;

import java.util.List;


/**
 * ClosestDesc.
 *
 */
@Explain(displayName = "Closest Operator")
public class ClosestDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  private int chrIndex;
  private int startIndex;
  private int endIndex;
  private String givenChr;
  private int givenStart;
  private int givenEnd;
  private List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList;
  private List<java.lang.String> outputColumnNames;

  public ClosestDesc() {
  }

  public ClosestDesc(
    final int[] chrStartEndIndices,
    final String givenChr,
    final int givenStart,
    final int givenEnd,
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
    List<java.lang.String> outputColumnNames) {
    this.chrIndex = chrStartEndIndices[0];
    this.startIndex = chrStartEndIndices[1];
    this.endIndex = chrStartEndIndices[2];
    this.givenChr = givenChr;
    this.givenStart = givenStart;
    this.givenEnd = givenEnd;
    this.colList = colList;
    this.outputColumnNames = outputColumnNames;
  }

  @Explain(displayName = "chrIndex")
  public int getChrIndex() {
    return chrIndex;
  }

  public void setChrDesc(
      final int chrIndex) {
    this.chrIndex = chrIndex;
  }

  @Explain(displayName = "startIndex")
  public int getStartIndex() {
    return startIndex;
  }

  public void setStartIndex(
      final int startIndex) {
    this.startIndex = startIndex;
  }

  @Explain(displayName = "endIndex")
  public int getEndIndex() {
    return endIndex;
  }

  public void setEndIndex(
      final int endIndex) {
    this.endIndex = endIndex;
  }

  @Explain(displayName = "the given interval's chr")
  public String getGivenChr() {
    return givenChr;
  }

  public void setGivenChr(final String givenChr) {
    this.givenChr = givenChr;
  }

  @Explain(displayName = "the given interval's start")
  public int getGivenStart() {
    return givenStart;
  }

  public void setGivenStart(final int givenStart) {
    this.givenStart = givenStart;
  }

  @Explain(displayName = "the given interval's end")
  public int getGivenEnd() {
    return givenEnd;
  }

  public void setGivenEnd(final int givenEnd) {
    this.givenEnd = givenEnd;
  }

  @Explain(displayName = "expressions")
  public List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> getColList() {
    return colList;
  }

  public void setColList(
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList) {
    this.colList = colList;
  }

  @Explain(displayName = "outputColumnNames")
  public List<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
    List<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

}
