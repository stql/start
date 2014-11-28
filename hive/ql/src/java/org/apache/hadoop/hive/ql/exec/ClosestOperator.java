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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ClosestDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Closest operator implementation.
 **/
public class ClosestOperator extends Operator<ClosestDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;

  private transient ExprNodeEvaluator[] eval;

  private transient PrimitiveObjectInspector[] propObjectInspectors;

  private transient ArrayList<ObjectInspector> currentPropObjectInspectors;

  private transient int chrIndex;

  private transient int startIndex;

  private transient int endIndex;

  private transient ArrayList<Object[]> bufferedCandidates;

  private transient int minDistance;

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

  private transient Interval givenInterval;


  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    List<ExprNodeDesc> colList = conf.getColList();

    eval = new ExprNodeEvaluator[colList.size()];

    propObjectInspectors = new PrimitiveObjectInspector[colList.size()];
    currentPropObjectInspectors = new ArrayList<ObjectInspector> (colList.size());

    for (int i = 0; i < colList.size(); i++) {
      eval[i] = ExprNodeEvaluatorFactory.get(colList.get(i));
      if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEEXPREVALUATIONCACHE)) {
        eval[i] = ExprNodeEvaluatorFactory.toCachedEval(eval[i]);
      }
      propObjectInspectors[i] = (PrimitiveObjectInspector) eval[i].initialize(inputObjInspectors[0]);
      currentPropObjectInspectors.add(ObjectInspectorUtils
          .getStandardObjectInspector(propObjectInspectors[i],
              ObjectInspectorCopyOption.WRITABLE));
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf
        .getOutputColumnNames(), currentPropObjectInspectors);

    chrIndex = conf.getChrIndex();

    startIndex = conf.getStartIndex();

    endIndex = conf.getEndIndex();

    minDistance = Integer.MAX_VALUE;

    bufferedCandidates = new ArrayList<Object[]>();

    givenInterval = new Interval(conf.getGivenChr(), conf.getGivenStart(), conf.getGivenEnd());

    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    Object[] obj = new Object [eval.length];
    for (int i = 0; i < eval.length; i++) {
      try {
        obj[i] = ObjectInspectorUtils.copyToStandardObject(eval[i].evaluate(row),
            propObjectInspectors[i], ObjectInspectorCopyOption.WRITABLE);
      } catch (HiveException e) {
        throw e;
      } catch (RuntimeException e) {
        throw new HiveException("Error evaluating "
            + conf.getColList().get(i).getExprString(), e);
      }
    }

    String chr = ((Text)(obj[chrIndex])).toString();
    if (givenInterval.chr.equals(chr)) {
      int start = ((IntWritable)(obj[startIndex])).get();
      int end = ((IntWritable)(obj[endIndex])).get();
      int dis = givenInterval.distance(new Interval(chr, start, end));
      if (dis < minDistance) {
        bufferedCandidates.clear();
        bufferedCandidates.add(obj);
        minDistance = dis;
      }
      else if (dis == minDistance) {
        bufferedCandidates.add(obj);
      }
    }

  }

  private void forwardBuffered() throws HiveException {
    for (int i = 0; i < bufferedCandidates.size(); i ++) {
      forward(bufferedCandidates.get(i), outputObjInspector);
    }
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      if (!bufferedCandidates.isEmpty()) {
        forwardBuffered();
      }
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
    return "CLOSEST";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.CLOSEST;
  }

}
