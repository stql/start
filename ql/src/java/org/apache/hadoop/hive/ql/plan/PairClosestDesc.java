
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
 * PairClosestDesc.
 *
 */
@Explain(displayName = "PairClosest Operator")
public class PairClosestDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  private int leftChrIndex;
  private int leftStartIndex;
  private int leftEndIndex;
  private int rightChrIndex;
  private int rightStartIndex;
  private int rightEndIndex;

  private List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> leftDesc;
  private List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> rightDesc;

  private List<java.lang.String> outputColumnNames;

  public PairClosestDesc() {
  }

  public PairClosestDesc(
    final int[] leftChrStartEndIndices,
    final int[] rightChrStartEndIndices,
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> leftDesc,
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> rightDesc,
    List<java.lang.String> outputColumnNames) {
    this.leftChrIndex = leftChrStartEndIndices[0];
    this.leftStartIndex = leftChrStartEndIndices[1];
    this.leftEndIndex = leftChrStartEndIndices[2];
    this.rightChrIndex = rightChrStartEndIndices[0];
    this.rightStartIndex = rightChrStartEndIndices[1];
    this.rightEndIndex = rightChrStartEndIndices[2];
    this.leftDesc = leftDesc;
    this.rightDesc = rightDesc;
    this.outputColumnNames = outputColumnNames;
  }

  @Explain(displayName = "leftChrIndex")
  public int getLeftChrIndex() {
    return leftChrIndex;
  }

  public void setLeftChrIndex(
      final int leftChrIndex) {
    this.leftChrIndex = leftChrIndex;
  }

  @Explain(displayName = "leftStartIndex")
  public int getLeftStartIndex() {
    return leftStartIndex;
  }

  public void setLeftStartIndex(
      final int leftStartIndex) {
    this.leftStartIndex = leftStartIndex;
  }

  @Explain(displayName = "leftEndIndex")
  public int getLeftEndIndex() {
    return leftEndIndex;
  }

  public void setLeftEndIndex(
      final int leftEndIndex) {
    this.leftEndIndex = leftEndIndex;
  }

  @Explain(displayName = "rightChrIndex")
  public int getRightChrIndex() {
    return rightChrIndex;
  }

  public void setRightChrIndex(
      final int rightChrIndex) {
    this.rightChrIndex = rightChrIndex;
  }

  @Explain(displayName = "RightStartIndex")
  public int getRightStartIndex() {
    return rightStartIndex;
  }

  public void setRightStartIndex(
      final int rightStartIndex) {
    this.rightStartIndex = rightStartIndex;
  }

  @Explain(displayName = "rightEndIndex")
  public int getRightEndIndex() {
    return rightEndIndex;
  }

  public void setRightEndIndex(
      final int rightEndIndex) {
    this.rightEndIndex = rightEndIndex;
  }

  @Explain(displayName = "leftIntervalDesc")
  public List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> getLeftDesc() {
    return leftDesc;
  }

  public void setLeftDesc(
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> leftDesc) {
    this.leftDesc = leftDesc;
  }

  @Explain(displayName = "rightIntervalDesc")
  public List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> getRightDesc() {
    return rightDesc;
  }

  public void setRightDesc(
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> rightDesc) {
    this.rightDesc = rightDesc;
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
/*
package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.List;


/**
 * SelectDesc.
 *
 */
/*@Explain(displayName = "PairClosest Operator")
public class PairClosestDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;
  private List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList;
  private List<java.lang.String> outputColumnNames;
  private boolean selectStar;
  private boolean selStarNoCompute;

  public PairClosestDesc() {
  }

  public PairClosestDesc(final boolean selStarNoCompute) {
    this.selStarNoCompute = selStarNoCompute;
  }

  public PairClosestDesc(
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
    final List<java.lang.String> outputColumnNames) {
    this(colList, outputColumnNames, false);
  }

  public PairClosestDesc(
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
    List<java.lang.String> outputColumnNames,
    final boolean selectStar) {
    this.colList = colList;
    this.selectStar = selectStar;
    this.outputColumnNames = outputColumnNames;
  }

  public PairClosestDesc(
    final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
    final boolean selectStar, final boolean selStarNoCompute) {
    this.colList = colList;
    this.selectStar = selectStar;
    this.selStarNoCompute = selStarNoCompute;
  }

  @Override
  public Object clone() {
    SelectDesc ret = new SelectDesc();
    ret.setColList(getColList() == null ? null : new ArrayList<ExprNodeDesc>(getColList()));
    ret.setOutputColumnNames(getOutputColumnNames() == null ? null :
      new ArrayList<String>(getOutputColumnNames()));
    ret.setSelectStar(selectStar);
    ret.setSelStarNoCompute(selStarNoCompute);
    return ret;
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

  @Explain(displayName = "SELECT * ")
  public String explainNoCompute() {
    if (isSelStarNoCompute()) {
      return "(no compute)";
    } else {
      return null;
    }
  }

  /**
   * @return the selectStar
   */
/*  public boolean isSelectStar() {
    return selectStar;
  }

  /**
   * @param selectStar
   *          the selectStar to set
   */
/*  public void setSelectStar(boolean selectStar) {
    this.selectStar = selectStar;
  }

  /**
   * @return the selStarNoCompute
   */
/*  public boolean isSelStarNoCompute() {
    return selStarNoCompute;
  }

  /**
   * @param selStarNoCompute
   *          the selStarNoCompute to set
   */
/*  public void setSelStarNoCompute(boolean selStarNoCompute) {
    this.selStarNoCompute = selStarNoCompute;
  }
}
*/