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
import org.apache.hadoop.hive.ql.plan.CoalesceDesc;
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
 * Coalesce operator implementation.
 */
public class CoalesceOperator extends Operator<CoalesceDesc> implements
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
//      if (bufferedList != null && !bufferedList.isEmpty()) {
//        genAndForwardObject();
//      }
      bufferedChr = chr;
//      bufferedList.clear();
    }

/*    if (conf.getMode() == CoalesceDesc.Mode.PARTIAL) {
      addIntoPartialBufferedList(start, end);
    }
    else if (conf.getMode() == CoalesceDesc.Mode.COMPLETE) {
      addIntoCompleteBufferedList(start, end);
    } */

  }

  private void addIntoPartialBufferedList(int start, int end) {
    if (bufferedList.isEmpty() || bufferedList.get(bufferedList.size() - 1).end < start - 1) {
      bufferedList.add(new Interval(start, end));
    }
    else {
      if (bufferedList.get(bufferedList.size() - 1).end < end) {
        bufferedList.get(bufferedList.size() - 1).end = end;
      }
    }
  }

  private void addIntoCompleteBufferedList(int start, int end) {
    if (bufferedList.isEmpty() || bufferedList.get(bufferedList.size() - 1).end < start - 1) {
      bufferedList.add(new Interval(start, end));
    }
    else {
      int low = 0, high = bufferedList.size() - 1;
      int pos = 0;
      while (low <= high) {
        pos = (low + high) / 2;
        if (bufferedList.get(pos).start == start) {
          break;
        } else if (bufferedList.get(pos).start < start) {
          low = pos + 1;
        } else if (bufferedList.get(pos).start > start) {
          high = pos - 1;
        }
      }
      if (low > high) {
        pos = low;
        if (pos == 0 || bufferedList.get(pos - 1).end < start - 1) {
          bufferedList.add(pos, new Interval(start, end));
        } else {
          if (bufferedList.get(pos - 1).end < end) {
            bufferedList.get(pos - 1).end = end;
            pos = pos - 1;
          }
        }
      }
      else {
        if (bufferedList.get(pos).end < end) {
          bufferedList.get(pos).end = end;
        }
      }
      while (pos < bufferedList.size() - 1 && bufferedList.get(pos).end + 1 >= bufferedList.get(pos + 1).start) {
        if (bufferedList.get(pos).end < bufferedList.get(pos + 1).end) {
          bufferedList.get(pos).end = bufferedList.get(pos + 1).end;
        }
        bufferedList.remove(pos + 1);
      }
    }
  }

  protected void forwardBuffered() throws HiveException {
    output[0] = new Text(bufferedChr);
    for (int i = 0; i < bufferedList.size(); i ++) {
      output[1] = new IntWritable(bufferedList.get(i).start);
      output[2] = new IntWritable(bufferedList.get(i).end);
      forward(output, outputObjInspector);
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
      while (qCursor < bufferedList.size() && end + 1 >= bufferedList.get(qCursor).getStart()) {
        end = bufferedList.get(qCursor).getEnd() > end ? bufferedList.get(qCursor).getEnd() : end;
        qCursor ++;
      }
      output[0] = new Text(bufferedChr);
      output[1] = new IntWritable(start);
      output[2] = new IntWritable(end);

      if (vd != null && qCursor - pCursor == 1) {
        output[3] = new FloatWritable(bufferedList.get(pCursor).getValue());
      } else if (vd != null && qCursor - pCursor > 1) {
        String vm = conf.getVm();
        float value = 0;
        if (vm.equals("each")) {
          int maxEnd = 0;
          for (int i = pCursor; i < qCursor; i ++) {
            if (bufferedList.get(i).start > maxEnd) {
              if (i + 1 == qCursor || bufferedList.get(i).end < bufferedList.get(i + 1).start) {
                value += bufferedList.get(i).value * (bufferedList.get(i).end - bufferedList.get(i).start + 1);
              } else {
                pipList.add(new PosIndexPair (bufferedList.get(i).getStart() - 1, i));
                pipList.add(new PosIndexPair (bufferedList.get(i).getEnd(), i));
              }
            } else {
              pipList.add(new PosIndexPair (bufferedList.get(i).getStart() - 1, i));
              pipList.add(new PosIndexPair (bufferedList.get(i).getEnd(), i));
              if (i + 1 == qCursor || bufferedList.get(i).end < bufferedList.get(i + 1).start) {
                Collections.sort(pipList, new PosIndexPairComparator());
                int prePos = pipList.get(0).getPos();
                significant[pipList.get(0).getIndex()] = !significant[pipList.get(0).getIndex()];
                pipList.remove(0);
                Iterator<PosIndexPair> pipIter = pipList.iterator();
                while (pipIter.hasNext()) {
                  PosIndexPair pip = pipIter.next();
                  if (pip.getPos() != prePos) {
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
                        if (vd.equals("sum") || vd.equals("avg")) {
                          segValue += bufferedList.get(cur).getValue();
                        } else if (vd.equals("product")) {
                          segValue *= bufferedList.get(cur).getValue();
                        } else if (vd.equals("max")) {
                          segValue = segValue > bufferedList.get(cur).getValue() ? segValue : bufferedList.get(cur).getValue();
                        } else if (vd.equals("min")) {
                          segValue = segValue < bufferedList.get(cur).getValue() ? segValue : bufferedList.get(cur).getValue();
                        } // *** five vd
                      }
                    }
                    if (counter > 0 && vd.equals("avg")) {
                      segValue = segValue / counter;
                    }
                    value += segValue * (pip.getPos() - prePos);
                  }
                  significant[pip.getIndex()] = !significant[pip.getIndex()];
                  prePos = pip.getPos();
                }
                pipList.clear();
              }
            }
            maxEnd = bufferedList.get(i).end > maxEnd ?  bufferedList.get(i).end : maxEnd;
          }
          output[3] = new FloatWritable(value / (end - start + 1));
        } else {
          if (vd.equals("max")) {
            value = Float.MIN_VALUE;
          } else if (vd.equals("min")) {
            value = Float.MAX_VALUE;
          } else if (vd.equals("product")) {
            value = 1;
          }
          for (int i = pCursor; i < qCursor; i ++) {
            float v = bufferedList.get(i).getValue();
            if (vd.equals("sum") || vd.equals("avg")) {
              value += v;
            } else if (vd.equals("product")) {
              value *= v;
            } else if (vd.equals("max")) {
              value = value > v ? value : v;
            } else if (vd.equals("min")) {
              value = value < v ? value : v;
            } // *** five vd
          }
          if (vd.equals("avg")) {
            value = value / (qCursor - pCursor);
          }
          output[3] = new FloatWritable(value);
        }

      }
      forward(output, outputObjInspector);
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
    return "COALESCE";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.COALESCE;
  }

}
