
package org.apache.hadoop.hive.ql.plan;

import java.util.HashMap;
import java.util.List;

/**
 * ProjectDesc.
 *
 */
@Explain(displayName = "Project Operator")
public class ProjectDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  private HashMap<Byte, List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc>> exprMap;
  private List<java.lang.String> outputColumnNames;
  private String vd;
  private String vm;

  public ProjectDesc() {
  }

  public ProjectDesc(
    final HashMap<Byte, List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc>> exprMap,
    List<java.lang.String> outputColumnNames,
    final String vd, final String vm) {
    this.exprMap = exprMap;
    this.outputColumnNames = outputColumnNames;
    this.vd = vd;
    this.vm = vm;
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

  @Explain(displayName = "value derivation")
  public String getVd() {
    return vd;
  }

  public void setVd (final String vd) {
    this.vd = vd;
  }

  @Explain(displayName = "value mode")
  public String getVm() {
    return vm;
  }

  public void setVm (final String vm) {
    this.vm = vm;
  }

}
