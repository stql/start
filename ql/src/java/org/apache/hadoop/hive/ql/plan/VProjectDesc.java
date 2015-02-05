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

//Added by Qiang
package org.apache.hadoop.hive.ql.plan;

import java.util.List;


/**
 * VProjectDesc.
 *
 */
@Explain(displayName = "VProject Operator")
public class VProjectDesc extends AbstractOperatorDesc {
  private static final long serialVersionUID = 1L;

  private String vd;
  private String vm;
  private int binLength;
  private List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList;
  private List<java.lang.String> outputColumnNames;


  public VProjectDesc() {
  }

  public VProjectDesc(
      final List<org.apache.hadoop.hive.ql.plan.ExprNodeDesc> colList,
      List<java.lang.String> outputColumnNames,
      final int binLength, final String vd, final String vm) {
      this.vd = vd;
      this.vm = vm;
      this.binLength = binLength;
      this.colList = colList;
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

  @Explain(displayName = "bin length")
  public int getBinLength() {
    return binLength;
  }

  public void setBinLength (final int binLength) {
    this.binLength = binLength;
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
