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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PairClosestDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * PairClosest operator implementation.
 **/

public class PairClosestOperator extends Operator<PairClosestDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;

  private transient ExprNodeEvaluator[] leftEval;

  private transient ExprNodeEvaluator[] rightEval;

  private transient PrimitiveObjectInspector[] propObjectInspectors;

  private transient ArrayList<ObjectInspector> currentPropObjectInspectors;

  private transient int leftChrIndex;

  private transient int leftStartIndex;

  private transient int leftEndIndex;

  private transient int rightChrIndex;

  private transient int rightStartIndex;

  private transient int rightEndIndex;

  class Interval {
    String chr;
    int start, end;

    Interval (String chr, int start, int end) {
      this.chr = chr;
      this.start = start;
      this.end = end;
    }

    boolean isBefore (Interval I) {
      return end < I.start;
    }

    boolean isAfter (Interval I) {
      return start > I.end;
    }

    int distance (Interval I) {
      if (this.isBefore(I)) {
        return I.start - end;
      } else if (this.isAfter(I)) {
        return start - I.end;
      } else {
        return 0;
      }
    }

  }

  class KeyWrapper {
    Object[] fieldList;
    int hashCode;

    public KeyWrapper () {
    }

    public KeyWrapper (Object[] fields, int hashCode) {
      this.fieldList = fields;
      this.hashCode = hashCode;
    }

    public void copyFields(Object[] fields) {
      fieldList = new Object [fields.length];
      for (int i = 0; i < fields.length; i ++) {
        fieldList[i] = fields[i];
      }
    }

    public KeyWrapper copyKeyWrapper () {
      return new KeyWrapper (fieldList, hashCode);
    }

    public void setHashCode() {
      hashCode = Arrays.hashCode(fieldList);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    @Override
    public boolean equals(Object otherKW) {
      Object[] otherFieldList = ((KeyWrapper)(otherKW)).fieldList;
      if (otherFieldList.length != fieldList.length) {
        return false;
      }
      int i = 0;
      for (; i < fieldList.length; i ++) {
        if (fieldList[i] != null && otherFieldList[i] == null) {
          break;
        }
        else if (fieldList[i] == null && otherFieldList[i] != null) {
          break;
        }
        else if (fieldList[i] != null && otherFieldList[i] != null) {
          if (!fieldList[i].equals(otherFieldList[i])) {
            break;
          }
        }
      }
      if (i == fieldList.length) {
        return true;
      } else {
        return false;
      }
    }

  }

  private transient LinkedHashMap<KeyWrapper, ArrayList<Object[]>> bufferedMap;

  private transient HashMap<KeyWrapper, Integer> minDistance;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    List<ExprNodeDesc> leftColList = conf.getLeftDesc();
    List<ExprNodeDesc> rightColList = conf.getRightDesc();

    leftEval = new ExprNodeEvaluator[leftColList.size()];
    rightEval = new ExprNodeEvaluator[rightColList.size()];

    propObjectInspectors = new PrimitiveObjectInspector[leftColList.size() + rightColList.size()];
    currentPropObjectInspectors = new ArrayList<ObjectInspector> (leftColList.size() + rightColList.size());

    for (int i = 0; i < rightColList.size(); i ++) {
      rightEval[i] = ExprNodeEvaluatorFactory.get(rightColList.get(i));
      if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEEXPREVALUATIONCACHE)) {
        rightEval[i] = ExprNodeEvaluatorFactory.toCachedEval(rightEval[i]);
      }
      propObjectInspectors[i] = (PrimitiveObjectInspector) rightEval[i].initialize(inputObjInspectors[0]);
      currentPropObjectInspectors.add(ObjectInspectorUtils
          .getStandardObjectInspector(propObjectInspectors[i],
              ObjectInspectorCopyOption.WRITABLE));
    }

    for (int i = 0; i < leftColList.size(); i++) {
      leftEval[i] = ExprNodeEvaluatorFactory.get(leftColList.get(i));
      if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEEXPREVALUATIONCACHE)) {
        leftEval[i] = ExprNodeEvaluatorFactory.toCachedEval(leftEval[i]);
      }
      propObjectInspectors[i + rightEval.length] = (PrimitiveObjectInspector) leftEval[i].initialize(inputObjInspectors[0]);
      currentPropObjectInspectors.add(ObjectInspectorUtils
          .getStandardObjectInspector(propObjectInspectors[i + rightEval.length],
              ObjectInspectorCopyOption.WRITABLE));
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf
        .getOutputColumnNames(), currentPropObjectInspectors);

    leftChrIndex = conf.getLeftChrIndex();

    leftStartIndex = conf.getLeftStartIndex();

    leftEndIndex = conf.getLeftEndIndex();

    rightChrIndex = conf.getRightChrIndex();

    rightStartIndex = conf.getRightStartIndex();

    rightEndIndex = conf.getRightEndIndex();

    minDistance = new HashMap<KeyWrapper, Integer>();

    bufferedMap = new LinkedHashMap<KeyWrapper, ArrayList<Object[]>>();

    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    Object[] leftInterval = new Object [leftEval.length];
    Object[] rightInterval = new Object [rightEval.length];

    for (int i = 0; i < rightEval.length; i++) {
      try {
        rightInterval[i] = ObjectInspectorUtils.copyToStandardObject(rightEval[i].evaluate(row),
            propObjectInspectors[i], ObjectInspectorCopyOption.WRITABLE);
      } catch (HiveException e) {
        throw e;
      } catch (RuntimeException e) {
        throw new HiveException("Error evaluating "
            + conf.getRightDesc().get(i).getExprString(), e);
      }
    }

    for (int i = 0; i < leftEval.length; i++) {
      try {
        leftInterval[i] = ObjectInspectorUtils.copyToStandardObject(leftEval[i].evaluate(row),
            propObjectInspectors[i + rightEval.length], ObjectInspectorCopyOption.WRITABLE);
      } catch (HiveException e) {
        throw e;
      } catch (RuntimeException e) {
        throw new HiveException("Error evaluating "
            + conf.getLeftDesc().get(i).getExprString(), e);
      }
    }

    String rightChr = ((Text)(rightInterval[rightChrIndex])).toString();
    String leftChr = ((Text)(leftInterval[leftChrIndex])).toString();

    if (rightChr.equals(leftChr)) {
      int leftStart = ((IntWritable)(leftInterval[leftStartIndex])).get();
      int leftEnd = ((IntWritable)(leftInterval[leftEndIndex])).get();
      int rightStart = ((IntWritable)(rightInterval[rightStartIndex])).get();
      int rightEnd = ((IntWritable)(rightInterval[rightEndIndex])).get();
      int dis = (new Interval(rightChr, rightStart, rightEnd)).distance(new Interval(leftChr, leftStart, leftEnd));

      KeyWrapper right = new KeyWrapper ();
      right.copyFields(rightInterval);
      right.setHashCode();

      Integer minDis = minDistance.get(right);
      if (minDis == null || dis < minDis.intValue()) {
        KeyWrapper key = right.copyKeyWrapper();
        minDistance.put(key, new Integer(dis));
        ArrayList<Object[]> leftList = new ArrayList<Object[]>();
        bufferedMap.put(key, leftList);
        leftList.add(leftInterval);
      }
      else if (dis == minDis.intValue()) {
        ArrayList<Object[]> leftList = bufferedMap.get(right);
        leftList.add(leftInterval);
      }

    }

  }

  private void forwardBuffered() throws HiveException {
    for (Map.Entry<KeyWrapper, ArrayList<Object[]>> map : bufferedMap.entrySet()) {
      Object[] rightInterval = map.getKey().fieldList;
      ArrayList<Object[]> leftIntervals = map.getValue();
      for (int i = 0; i < leftIntervals.size(); i ++) {
        Object[] output = new Object [rightEval.length + leftEval.length];
        System.arraycopy(rightInterval, 0, output, 0, rightEval.length);
        System.arraycopy(leftIntervals.get(i), 0, output, rightEval.length, leftEval.length);
        forward(output, outputObjInspector);
      }
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      forwardBuffered();
    }
  }

  /**
   * @return the name of the operator
   */
  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "PAIRCLOSEST";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.PAIRCLOSEST;
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
package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PairClosestDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * Select operator implementation.
 */
/*public class PairClosestOperator extends Operator<PairClosestDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;
  protected transient ExprNodeEvaluator[] eval;

  transient Object[] output;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    // Just forward the row as is
    if (conf.isSelStarNoCompute()) {
      initializeChildren(hconf);
      return;
    }

    List<ExprNodeDesc> colList = conf.getColList();
    eval = new ExprNodeEvaluator[colList.size()];
    for (int i = 0; i < colList.size(); i++) {
      assert (colList.get(i) != null);
      eval[i] = ExprNodeEvaluatorFactory.get(colList.get(i));
      if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEEXPREVALUATIONCACHE)) {
        eval[i] = ExprNodeEvaluatorFactory.toCachedEval(eval[i]);
      }
    }

    output = new Object[eval.length];
    LOG.info("SELECT "
        + ((StructObjectInspector) inputObjInspectors[0]).getTypeName());
    outputObjInspector = initEvaluatorsAndReturnStruct(eval, conf
        .getOutputColumnNames(), inputObjInspectors[0]);
    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    // Just forward the row as is
    if (conf.isSelStarNoCompute()) {
      forward(row, inputObjInspectors[tag]);
      return;
    }

    for (int i = 0; i < eval.length; i++) {
      try {
        output[i] = eval[i].evaluate(row);
      } catch (HiveException e) {
        throw e;
      } catch (RuntimeException e) {
        throw new HiveException("Error evaluating "
            + conf.getColList().get(i).getExprString(), e);
      }
    }
    forward(output, outputObjInspector);
  }

  /**
   * @return the name of the operator
   */
/*  @Override
  public String getName() {
    return getOperatorName();
  }

  static public String getOperatorName() {
    return "PAIRCLOSEST";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.PAIRCLOSEST;
  }

  @Override
  public boolean supportSkewJoinOptimization() {
    return true;
  }

  @Override
  public boolean columnNamesRowResolvedCanBeObtained() {
    return true;
  }

  @Override
  public boolean supportAutomaticSortMergeJoin() {
    return true;
  }

  @Override
  public boolean supportUnionRemoveOptimization() {
    return true;
  }
}
*/