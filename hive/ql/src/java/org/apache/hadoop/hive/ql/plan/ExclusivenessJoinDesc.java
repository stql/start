
package org.apache.hadoop.hive.ql.plan;

import java.util.HashMap;
import java.util.List;

/**
 * ExclusivenessJoinDesc.
 *
 */
@Explain(displayName = "ExclusivenessJoin Operator")
public class ExclusivenessJoinDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  private HashMap<Byte, List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc>> exprMap;
  private List<java.lang.String> outputColumnNames;
  private String agg;

  public ExclusivenessJoinDesc() {
  }

  public ExclusivenessJoinDesc(
    final HashMap<Byte, List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc>> exprMap,
    List<java.lang.String> outputColumnNames,
    String agg) {
    this.exprMap = exprMap;
    this.outputColumnNames = outputColumnNames;
    this.agg = agg;
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

  @Explain(displayName = "aggregation")
  public String getAggregation() {
    return agg;
  }

  public void setAggregation(
    String agg) {
    this.agg = agg;
  }

}

