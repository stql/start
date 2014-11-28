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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.VProjectDesc;
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
 * VProject operator implementation.
 */
public class VProjectOperator extends Operator<VProjectDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;

  protected transient ExprNodeEvaluator[] eval;

  protected transient PrimitiveObjectInspector[] propObjectInspectors;

  protected transient ArrayList<ObjectInspector> currentPropObjectInspectors;

  transient List<String> fieldNames;

  transient Object[] obj;
  transient Object[] output;

  protected transient String bufferedChr;
  protected transient float [] binValues;
  protected transient int [] binCoverCounts;
  protected transient int binLength;
  protected transient String vd;
  protected transient String vm;

  private static Map<String, Integer> chrLength;
  static {
    chrLength = new HashMap<String, Integer>();
    chrLength.put("chr1", new Integer(249250621));
    chrLength.put("chr2", new Integer(243199373));
    chrLength.put("chr3", new Integer(198022430));
    chrLength.put("chr4", new Integer(191154276));
    chrLength.put("chr5", new Integer(180915260));
    chrLength.put("chr6", new Integer(171115067));
    chrLength.put("chr7", new Integer(159138663));
    chrLength.put("chr8", new Integer(146364022));
    chrLength.put("chr9", new Integer(141213431));
    chrLength.put("chr10", new Integer(135534747));
    chrLength.put("chr11", new Integer(135006516));
    chrLength.put("chr12", new Integer(133851895));
    chrLength.put("chr13", new Integer(115169878));
    chrLength.put("chr14", new Integer(107349540));
    chrLength.put("chr15", new Integer(102531392));
    chrLength.put("chr16", new Integer(90354753));
    chrLength.put("chr17", new Integer(81195210));
    chrLength.put("chr18", new Integer(78077248));
    chrLength.put("chr19", new Integer(59128983));
    chrLength.put("chr20", new Integer(63025520));
    chrLength.put("chr21", new Integer(48129895));
    chrLength.put("chr22", new Integer(51304566));
    chrLength.put("chrX", new Integer(155270560));
    chrLength.put("chrY", new Integer(59373566));
    chrLength.put("chrM", new Integer(16571));
    chrLength.put("chrT", new Integer(99));
  }

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
    output = new Object[eval.length];

    binLength = conf.getBinLength();
    vd = conf.getVd();
    vm = conf.getVm();

    fieldNames = conf.getOutputColumnNames();

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames, currentPropObjectInspectors);

    bufferedList = new ArrayList<Interval> ();

    initializeChildren(hconf);
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
//    if (globalCnt % 100000 == 0) {
//      System.out.println("Now we have processed " + globalCnt + " rows");
//    }
//    globalCnt++;

    String chr = null;
    int start = 0;
    int end = 0;
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

    if (!chr.equals(bufferedChr)) {
      bufferedChr = chr;
      initializeNewChr();
    }

    if (eval.length > 3) {
      if ("total".equals(vm) || ("each".equals(vm) && "sum".equals(vd))) {
        computeBinValues(start, end, value);
      }

      if ("each".equals(vm) && !"sum".equals(vd)) {
        bufferedList.add(new Interval(start, end, value));
      }
    }

  }

  protected void initializeNewChr() {
    int length = chrLength.get(bufferedChr);
    int binsNum = (length % binLength == 0) ? (length / binLength) : (length / binLength + 1);
    binValues = new float [binsNum];
    if ("each".equals(vm)) {
      Arrays.fill(binValues, 0);
    } else if ("total".equals(vm)) {
      if (vd.equals("sum") || vd.equals("avg")) {
        Arrays.fill(binValues, 0);
      } else if (vd.equals("product")) {
        Arrays.fill(binValues, 1);
      } else if (vd.equals("max")) {
        Arrays.fill(binValues, Float.MIN_VALUE);
      } else if (vd.equals("min")) {
        Arrays.fill(binValues, Float.MAX_VALUE);
      }
      binCoverCounts = new int [binsNum];
      Arrays.fill(binCoverCounts, 0);
    }
  }

  protected void computeBinValues(int start, int end, float value) {
    int totalLength = end - start + 1;
    int binIndex = (start - 1) / binLength;
    if (end <= (binIndex + 1) * binLength) {
      if (vm.equals("total")) {
        computeTotalBinValues(binIndex, value);
      } else if (vm.equals("each")) {
        binValues[binIndex] = binValues[binIndex] + value * totalLength;
      }
    } else {
      int fracLength = binLength * (binIndex + 1) - start + 1;
      if (vm.equals("total")) {
        computeTotalBinValues(binIndex, value * fracLength / totalLength);
      } else if (vm.equals("each")) {
        binValues[binIndex] = binValues[binIndex] + value * fracLength;
      }
      binIndex ++;
      while (end > (binIndex + 1) * binLength) {
        fracLength = binLength;
        if (vm.equals("total")) {
          computeTotalBinValues(binIndex, value * fracLength / totalLength);
        } else if (vm.equals("each")) {
          binValues[binIndex] = binValues[binIndex] + value * fracLength;
        }
        binIndex ++;
      }
      fracLength = end - binIndex * binLength;
      if (vm.equals("total")) {
        computeTotalBinValues(binIndex, value * fracLength / totalLength);
      } else if (vm.equals("each")) {
        binValues[binIndex] = binValues[binIndex] + value * fracLength;
      }
    }
  }

  protected void computeTotalBinValues(int binIndex, float v) {
    float value = processDifferentVd(binValues[binIndex], v);
    binValues[binIndex] = value;
    binCoverCounts[binIndex] ++;
  }

  protected void computeEachBinValues() {
    Collections.sort(bufferedList, new IntervalStartComparator ());
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
        computeBinValues(start, end, bufferedList.get(pCursor).getValue());
      } else {
        for (int i = pCursor; i < qCursor; i ++) {
          pipList.add(new PosIndexPair (bufferedList.get(i).getStart() - 1, i));
          pipList.add(new PosIndexPair (bufferedList.get(i).getEnd(), i));
        }
        Collections.sort(pipList, new PosIndexPairComparator());
        int prePos = pipList.get(0).getPos();
        significant[pipList.get(0).getIndex()] = !significant[pipList.get(0).getIndex()];
        pipList.remove(0);
        Iterator<PosIndexPair> pipIter = pipList.iterator();
        float segValue = 0;
        int counter = 0;
        while (pipIter.hasNext()) {
          PosIndexPair pip = pipIter.next();
          if (pip.getPos() != prePos) {
            if (vd.equals("sum") || vd.equals("avg")) {
              segValue = 0;
            }
            if (vd.equals("max")) {
              segValue = Float.MIN_VALUE;
            } else if (vd.equals("min")) {
              segValue = Float.MAX_VALUE;
            } else if (vd.equals("product")) {
              segValue = 1;
            }
            counter = 0;
            for (int cur = pCursor; cur < qCursor; cur ++) {
              if (significant[cur]) {
                segValue = processDifferentVd(segValue, bufferedList.get(cur).getValue());
                counter ++;
              }
            }
            if (vd.equals("avg")) {
              segValue /= counter;
            }
            computeBinValues(prePos + 1, pip.getPos(), segValue);
          }
          significant[pip.getIndex()] = !significant[pip.getIndex()];
          prePos = pip.getPos();
        }
        pipList.clear();
      }
    }
  }

  protected float processDifferentVd(float oldValue, float v) {
    float value = 0;
    if (vd.equals("sum") || vd.equals("avg")) {
      value = oldValue + v;
    } else if (vd.equals("product")) {
      value = oldValue * v;
    } else if (vd.equals("max")) {
      value = oldValue > v ? oldValue : v;
    } else if (vd.equals("min")) {
      value = oldValue < v ? oldValue : v;
    }
    return value;
  }

  protected void forwardBufferedWithValues() throws HiveException {
    output[0] = new Text(bufferedChr);
    int binsNum = binValues.length;
    for (int i = 0; i < binsNum - 1; i ++) {
      output[1] = new IntWritable(i * binLength + 1);
      output[2] = new IntWritable((i + 1) * binLength);
      output[3] = new FloatWritable(binValues[i]);
      forward(output, outputObjInspector);
    }
    output[1] = new IntWritable(binLength * (binsNum - 1) + 1);
    output[2] = new IntWritable(chrLength.get(bufferedChr));
    output[3] = new FloatWritable(binValues[binsNum - 1]);
    forward(output, outputObjInspector);
    }

  protected void forwardBuffered() throws HiveException {
    output[0] = new Text(bufferedChr);
    int binsNum = binValues.length;
    for (int i = 0; i < binsNum - 1; i ++) {
      output[1] = new IntWritable(i * binLength + 1);
      output[2] = new IntWritable((i + 1) * binLength);
      forward(output, outputObjInspector);
    }
    output[1] = new IntWritable(binLength * (binsNum - 1) + 1);
    output[2] = new IntWritable(chrLength.get(bufferedChr));
    forward(output, outputObjInspector);
    }

  @Override
  public void startGroup() throws HiveException {
    bufferedList.clear();
    super.startGroup();
  }

  @Override
  public void endGroup() throws HiveException {
    if ("total".equals(vm)) {
      if (vd.equals("avg")) {
        for (int i = 0; i < binValues.length; i ++) {
          if (binCoverCounts[i] > 0) {
            binValues[i] = binValues[i] / binCoverCounts[i];
          }
        }
      } else if (vd.equals("max") || vd.equals("min") || vd.equals("product")) {
        for (int i = 0; i < binValues.length; i ++) {
          if (binCoverCounts[i] == 0) {
            binValues[i] = 0;
          }
        }
      }
      forwardBufferedWithValues();
    }
    else if ("each".equals(vm)) {
      if (!vd.equals("sum")) {
        computeEachBinValues();
      }
      float value = 0;
      for (int i = 0; i < binValues.length - 1; i ++) {
        binValues[i] = binValues[i] / binLength;
      }
      binValues[binValues.length - 1] = binValues[binValues.length - 1]
          / (chrLength.get(bufferedChr) - binLength * (binValues.length - 1));
      forwardBufferedWithValues();
    } else {
      forwardBuffered();
    }
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
    return "VPJ";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.VPROJECT;
  }

}
