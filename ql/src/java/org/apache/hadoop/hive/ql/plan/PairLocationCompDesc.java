package org.apache.hadoop.hive.ql.plan;

import java.util.HashMap;
import java.util.List;

/**
 * PairLocationCompDesc.
 *
 */
@Explain(displayName = "PairLocationComp Operator")
public class PairLocationCompDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  private HashMap<Byte, List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc>> exprMap;
  private List<java.lang.String> outputColumnNames;
  private String locationComp;

  private int leftChrIndex =  0, leftStartIndex =  0, leftEndIndex = 0;
  private int rightChrIndex = 0,  rightStartIndex = 0, rightEndIndex = 0;

  public PairLocationCompDesc() {
  }

  public PairLocationCompDesc(
    final HashMap<Byte, List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc>> exprMap,
    List<java.lang.String> outputColumnNames,
    final String locationComp,
    int leftChrIndex, int leftStartIndex, int leftEndIndex,
    int rightChrIndex, int rightStartIndex, int rightEndIndex) {
    this.exprMap = exprMap;
    this.outputColumnNames = outputColumnNames;
    this.locationComp = locationComp;
    this.leftChrIndex = leftChrIndex;
    this.leftStartIndex = leftStartIndex;
    this.leftEndIndex = leftEndIndex;
    this.rightChrIndex = rightChrIndex;
    this.rightStartIndex = rightStartIndex;
    this.rightEndIndex = rightEndIndex;
  }

  @Explain(displayName = "expressionsMap")
  public HashMap<Byte, List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc>> getExprMap() {
    return exprMap;
  }

  public void setExprMap(
    final HashMap<Byte, List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc>> exprMap) {
    this.exprMap = exprMap;
  }

  @Explain(displayName = "outputColumnNames")
  public List<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
    List<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  @Explain(displayName = "locationComparator")
  public String getLocationComparotor() {
    return locationComp;
  }

  public void setLocationComparator(
    String locationComp) {
    this.locationComp = locationComp;
  }

  @Explain(displayName = "leftChrIndex")
  public int getLeftChrIndex() {
    return leftChrIndex;
  }

  public void setLeftChrIndex(
    int leftChrIndex) {
    this.leftChrIndex = leftChrIndex;
  }

  @Explain(displayName = "leftStartIndex")
  public int getLeftStartIndex() {
    return leftStartIndex;
  }

  public void setLeftStartIndex(
    int leftStartIndex) {
    this.leftStartIndex = leftStartIndex;
  }

  @Explain(displayName = "leftEndIndex")
  public int getLeftEndIndex() {
    return leftEndIndex;
  }

  public void setLeftEndIndex(
    int leftEndIndex) {
    this.leftEndIndex = leftEndIndex;
  }

  @Explain(displayName = "rightChrIndex")
  public int getRightChrIndex() {
    return rightChrIndex;
  }

  public void setRightChrIndex(
    int rightChrIndex) {
    this.rightChrIndex = rightChrIndex;
  }

  @Explain(displayName = "rightStartIndex")
  public int getRightStartIndex() {
    return rightStartIndex;
  }

  public void setRightStartIndex(
    int rightStartIndex) {
    this.rightStartIndex = rightStartIndex;
  }

  @Explain(displayName = "rightEndIndex")
  public int getRightEndIndex() {
    return rightEndIndex;
  }

  public void setRightEndIndex(
    int rightEndIndex) {
    this.rightEndIndex = rightEndIndex;
  }

}

