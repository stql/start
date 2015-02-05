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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DiscretizeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Discretize operator implementation.
 */
public class DiscretizeOperator extends Operator<DiscretizeDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;

  protected transient ExprNodeEvaluator[] eval;

  protected transient PrimitiveObjectInspector[] propObjectInspectors;

  protected transient ArrayList<ObjectInspector> currentPropObjectInspectors;

  transient List<String> fieldNames;

  transient Object[] obj;

  transient Object[] output;

  protected transient String bufferedChr;

  class Interval {
    int start, end;
    float value;

    Interval (int start, int end) {
      this.start = start;
      this.end = end;
    }

    Interval (int start, int end, float value) {
      this.start = start;
      this.end = end;
      this.value = value;
    }

    int getStart () {
      return start;
    }

    void setStart (int start) {
      this.start = start;
    }

    int getEnd () {
      return end;
    }

    void setEnd (int end) {
      this.end = end;
    }

    float getValue () {
      return value;
    }

    void setValue (float value) {
      this.value = value;
    }
  }

  class IntervalStartComparator implements Comparator<Interval> {
    public int compare(Interval interval1, Interval interval2) {
      if (interval1.getStart() < interval2.getStart()) {
        return -1;
      } else if (interval1.getStart() == interval2.getStart()) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  class PosIndexPair {
    int pos, index;

    public PosIndexPair(int pos, int index) {
      this.pos = pos;
      this.index = index;
    }

    int getPos() {
      return pos;
    }

    int getIndex() {
      return index;
    }
  }

  class PosIndexPairComparator implements Comparator<PosIndexPair> {
    public int compare(PosIndexPair pip1, PosIndexPair pip2) {
      if (pip1.getPos() < pip2.getPos()) {
        return -1;
      } else if (pip1.getPos() == pip2.getPos()) {
        return 0;
      } else {
        return 1;
      }
    }
  }

  protected transient ArrayList<Interval> bufferedList;


  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    List<ExprNodeDesc> colList = conf.getColList();

    eval = new ExprNodeEvaluator[colList.size()];

    propObjectInspectors = new PrimitiveObjectInspector[colList.size()];
    currentPropObjectInspectors = new ArrayList<ObjectInspector> (colList.size());

    for (int i = 0; i < colList.size(); i++) {
      assert (colList.get(i) != null);
      eval[i] = ExprNodeEvaluatorFactory.get(colList.get(i));
      if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEEXPREVALUATIONCACHE)) {
        eval[i] = ExprNodeEvaluatorFactory.toCachedEval(eval[i]);
      }

      propObjectInspectors[i] = (PrimitiveObjectInspector) eval[i].initialize(inputObjInspectors[0]);
      currentPropObjectInspectors.add(ObjectInspectorUtils
          .getStandardObjectInspector(propObjectInspectors[i],
              ObjectInspectorCopyOption.WRITABLE));

    }

    obj = new Object[eval.length];
    output = new Object [eval.length];

    fieldNames = conf.getOutputColumnNames();

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames, currentPropObjectInspectors);

    bufferedList = new ArrayList<Interval> ();

    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {

    String chr = null;
    int start = 0, end = 0;
    float value = 0;

    for (int i = 0; i < eval.length; i++) {
      try {
        obj[i] = eval[i].evaluate(row);
        switch (i) {
          case 0:
            chr = PrimitiveObjectInspectorUtils.getString(obj[i], propObjectInspectors[i]);
            break;
          case 1:
            start = PrimitiveObjectInspectorUtils.getInt(obj[i], propObjectInspectors[i]);
            break;
          case 2:
            end = PrimitiveObjectInspectorUtils.getInt(obj[i], propObjectInspectors[i]);
            break;
          case 3:
            value = PrimitiveObjectInspectorUtils.getFloat(obj[i], propObjectInspectors[i]);
            break;
        }
      } catch (HiveException e) {
        throw e;
      } catch (RuntimeException e) {
        throw new HiveException("Error evaluating "
            + conf.getColList().get(i).getExprString(), e);
      }
    }
    bufferedList.add(new Interval(start, end, value));

    if (!chr.equals(bufferedChr)) {
      bufferedChr = chr;
    }

  }

  protected void genAndForwardObject() throws HiveException {
    Collections.sort(bufferedList, new IntervalStartComparator ());
    String vd = conf.getVd();
    int pCursor, qCursor = 0;
    boolean[] significant = new boolean [bufferedList.size()];
    Arrays.fill(significant, false);
    ArrayList<PosIndexPair> pipList = new ArrayList<PosIndexPair> ();
    while (qCursor < bufferedList.size()) {
      pCursor = qCursor;
      qCursor ++;
      int start = bufferedList.get(pCursor).getStart();
      int end = bufferedList.get(pCursor).getEnd();
      while (qCursor < bufferedList.size() && end >= bufferedList.get(qCursor).getStart()) {
        end = bufferedList.get(qCursor).getEnd() > end ? bufferedList.get(qCursor).getEnd() : end;
        qCursor ++;
      }
      if(qCursor - pCursor == 1) {
        output[0] = new Text(bufferedChr);
        output[1] = new IntWritable(start);
        output[2] = new IntWritable(end);
        if (vd != null) {
          output[3] = new FloatWritable(bufferedList.get(pCursor).getValue());
        }
        forward(output, outputObjInspector);
      } else {
//        float value = 0;
        for (int i = pCursor; i < qCursor; i ++) {
          pipList.add(new PosIndexPair (bufferedList.get(i).getStart() - 1, i));
          pipList.add(new PosIndexPair (bufferedList.get(i).getEnd(), i));
        }
        Collections.sort(pipList, new PosIndexPairComparator());
        int prePos = pipList.get(0).getPos();
        significant[pipList.get(0).getIndex()] = !significant[pipList.get(0).getIndex()];
        pipList.remove(0);
        Iterator<PosIndexPair> pipIter = pipList.iterator();
        while (pipIter.hasNext()) {
          PosIndexPair pip = pipIter.next();
          if (pip.getPos() != prePos) {
            output[0] = new Text(bufferedChr);
            output[1] = new IntWritable(prePos + 1);
            output[2] = new IntWritable(pip.getPos());
            if (vd != null) {
              String vm = conf.getVm();
              float segValue = 0;
              if (vd.equals("max")) {
                segValue = Float.MIN_VALUE;
              } else if (vd.equals("min")) {
                segValue = Float.MAX_VALUE;
              } else if (vd.equals("product")) {
                segValue = 1;
              }
              int counter = 0;
              for (int cur = pCursor; cur < qCursor; cur ++) {
                if (significant[cur]) {
                  counter ++;
                  float v = bufferedList.get(cur).getValue();
                  if (vm.equals("total")) {
                    v = v * (pip.getPos() - prePos)
                        / (bufferedList.get(cur).getEnd() - bufferedList.get(cur).getStart() + 1);
                  }
                  if (vd.equals("sum") || vd.equals("avg")) {
                    segValue += v;
                  } else if (vd.equals("product")) {
                    segValue *= v;
                  } else if (vd.equals("max")) {
                    segValue = segValue > v ? segValue : v;
                  } else if (vd.equals("min")) {
                    segValue = segValue < v ? segValue : v;
                  } // *** five vd
                }
              }
              if (counter > 0 && vd.equals("avg")) {
                segValue = segValue / counter;
              }
              output[3] = new FloatWritable(segValue);
            }
            forward(output, outputObjInspector);
          }
          significant[pip.getIndex()] = !significant[pip.getIndex()];
          prePos = pip.getPos();
        }
        pipList.clear();
      }
    }
  }

  @Override
  public void startGroup() throws HiveException {
    bufferedList.clear();
    super.startGroup();
  }

  @Override
  public void endGroup() throws HiveException {
    genAndForwardObject();
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      bufferedList.clear();
      bufferedList = null;
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
    return "DISCRETIZE";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.DISCRETIZE;
  }

}
