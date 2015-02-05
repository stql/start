
package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.IntersectJoinDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * IntersectJoin operator implementation.
 **/
public class IntersectJoinOperator extends Operator<IntersectJoinDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;

  private transient int numAliases;
  private transient List<ExprNodeEvaluator>[] joinValues;
  private transient List<ObjectInspector>[] joinValuesObjectInspectors;
  private transient List<ObjectInspector>[] joinValuesStandardObjectInspectors;
  private transient Object[] forwardCache;

  class Interval {
    ArrayList<Object> metadata;
    int start, end;
    float value;

    public Interval (int start, int end) {
      this.start = start;
      this.end = end;
      metadata = null;
    }

    public Interval (int start, int end, float value) {
      this.start = start;
      this.end = end;
      this.value = value;
      metadata = null;
    }

    void setStart (int start) {
      this.start = start;
    }

    void setEnd (int end) {
      this.end = end;
    }

    void setValue (float value) {
      this.value = value;
    }

    void setFields (ArrayList<Object> metadata) {
      this.metadata = metadata;
    }

    ArrayList<Object> getMeta () {
      return metadata;
    }

    int getStart () {
      return start;
    }

    int getEnd () {
      return end;
    }

    float getValue () {
      return value;
    }

    boolean isBefore (Interval I) {
      return end < I.getStart();
    }

    boolean isAfter (Interval I) {
      return start > I.getEnd();
    }

    boolean overlapsWith (Interval I) {
      return (start <= I.getEnd() && end >= I.getStart());
    }
  }

  /*
  class Interval {
    int start, end;
    float value = 0;
    String meta = null;

    Interval (int start, int end) {
      this.start = start;
      this.end = end;
    }

    Interval (int start, int end, float value) {
      this.start = start;
      this.end = end;
      this.value = value;
    }

    int getStart() {
      return start;
    }

    int getEnd() {
      return end;
    }

    float getValue() {
      return value;
    }

    String getMeta() {
      return meta;
    }

    void setMeta(String meta) {
      this.meta = meta;
    }

    void setStart(int start) {
      this.start = start;
    }

    void setEnd(int end) {
      this.end = end;
    }

    void setValue(float value) {
      this.value = value;
    }

    boolean isBefore (Interval I) {
      return end < I.start;
    }

    boolean isAfter (Interval I) {
      return start > I.end;
    }

    boolean overlapsWith (Interval I) {
      return (start <= I.end && end >= I.start);
    }

  }
*/
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


  private transient ArrayList<Interval>[] storage;
//  int leftStartIndex, leftEndIndex, leftValueIndex;
  int chrIndex =  -1, startIndex =  -1, endIndex = -1, valueIndex = -1;
  boolean metadata = false;
  private transient String curChr = null;


  @SuppressWarnings("unchecked")
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    storage = new ArrayList [conf.getNumSources()];
    for (int i = 0; i < storage.length; i ++) {
      storage[i] = new ArrayList<Interval> ();
    }

    numAliases = conf.getExprMap().size();
    joinValues = new List[conf.getNumSources()];

    populateJoinValues(joinValues, conf.getExprMap());
    joinValuesObjectInspectors = getObjectInspectorsFromEvaluators(joinValues, inputObjInspectors);
    joinValuesStandardObjectInspectors = getStandardObjectInspectors(joinValuesObjectInspectors);

/*    if (conf.getAggregation() == null) {
      forwardCache = new Object[3];
    } else {
      forwardCache = new Object[4];
    } */


    List<String> outCols = conf.getOutputColumnNames();
    forwardCache = new Object[outCols.size()];
    for (int index = 0; index < outCols.size(); index ++) {
      if (outCols.get(index).equals("chr")) {
        chrIndex = index;
      }
      else if (outCols.get(index).equals("chrstart")) {
        startIndex = index;
      }
      else if (outCols.get(index).equals("chrend")) {
        endIndex = index;
      }
      else if (outCols.get(index).equals("value")) {
        valueIndex = index;
      }
      else {
        metadata = true;
      }
    }
//    leftStartIndex = outStartIndex > outChrIndex ? outStartIndex - 1 : outStartIndex;
  //  leftEndIndex = outEndIndex > outChrIndex ? outEndIndex - 1 : outEndIndex;
//    leftValueIndex = outValueIndex > outChrIndex ? outValueIndex  - 1: outValueIndex;



 /*   outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf
        .getOutputColumnNames(), joinValuesStandardObjectInspectors[conf.getNumSources() - 1]); */

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf
        .getOutputColumnNames(), joinValuesStandardObjectInspectors[0]);

    initializeChildren(hconf);
  }

  private void populateJoinValues(List<ExprNodeEvaluator>[] outMap,
      HashMap<Byte, List<ExprNodeDesc>> inputMap) throws HiveException {
    for (Entry<Byte, List<ExprNodeDesc>> e : inputMap.entrySet()) {
      Byte key = e.getKey();
      List<ExprNodeEvaluator> valueFields = new ArrayList<ExprNodeEvaluator>();
      for (ExprNodeDesc expr : e.getValue()) {
        valueFields.add(ExprNodeEvaluatorFactory.get(expr));
      }
      outMap[key] = valueFields;
    }
  }

  private List<ObjectInspector>[] getObjectInspectorsFromEvaluators(List<ExprNodeEvaluator>[] exprEntries,
      ObjectInspector[] inputObjInspectors) throws HiveException {
    List<ObjectInspector>[] result = new List[exprEntries.length];
    for (byte alias = 0; alias < exprEntries.length; alias++) {
      List<ExprNodeEvaluator> exprList = exprEntries[alias];
      List<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>();
      for (int i = 0; i < exprList.size(); i++) {
        fieldOIList.add(exprList.get(i).initialize(inputObjInspectors[alias]));
      }
      result[alias] = fieldOIList;
    }
    return result;
  }

  private List<ObjectInspector>[] getStandardObjectInspectors(
      List<ObjectInspector>[] aliasToObjectInspectors) {
    List<ObjectInspector>[] result = new List[aliasToObjectInspectors.length];
    for (byte alias = 0; alias < aliasToObjectInspectors.length; alias++) {
      List<ObjectInspector> oiList = aliasToObjectInspectors[alias];
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>(
          oiList.size());
      for (int i = 0; i < oiList.size(); i++) {
        fieldOIList.add(ObjectInspectorUtils.getStandardObjectInspector(oiList
            .get(i), ObjectInspectorCopyOption.WRITABLE));
      }
      result[alias] = fieldOIList;
    }
    return result;
  }

  @Override
  public void processOp(Object row, int tag) throws HiveException {
    ArrayList<Object> nr = computeValue(row, joinValues[tag],
        joinValuesObjectInspectors[tag]);
    String chr = tag == 0 ? ((Text)nr.get(chrIndex)).toString() : ((Text)nr.get(0)).toString();
    int start = tag == 0 ? ((IntWritable)nr.get(startIndex)).get() : ((IntWritable)nr.get(1)).get();
    int end = tag == 0 ? ((IntWritable)nr.get(endIndex)).get() : ((IntWritable)nr.get(2)).get();
    if (curChr == null || !curChr.equals(chr)) {
      curChr = chr;
    }
    Interval interval;
    if (valueIndex >= 0) {
      float value = tag == 0 ? ((FloatWritable)nr.get(valueIndex)).get() : ((FloatWritable)nr.get(3)).get();
      interval = new Interval(start, end, value);
    }
    else {
      interval = new Interval(start, end);
    }

    if (tag == 0 && metadata) {
      nr.set(chrIndex, null);
      nr.set(startIndex, null);
      nr.set(endIndex, null);
      if (valueIndex >= 0) {
        nr.set(valueIndex, null);
      }
      interval.setFields(nr);
    }

    store(interval, tag);

/*    ArrayList<Object> nr = computeValue(row, joinValues[tag],
        joinValuesObjectInspectors[tag]);
    String chr = ((Text)nr.get(0)).toString();
    if (curChr == null || !curChr.equals(chr)) {
      curChr = chr;
    }


    assert chr == curChr;
    int start = ((IntWritable)nr.get(1)).get();
    int end = ((IntWritable)nr.get(2)).get();
    if (conf.getAggregation() == null) {
      store(new Interval(start, end), tag);
    } else {
      float value = ((FloatWritable)nr.get(3)).get();
      store(new Interval(start, end, value), tag);
    }
*/
  }


  private ArrayList<Object> computeValue(Object row, List<ExprNodeEvaluator> valueFields,
      List<ObjectInspector> valueFieldsOI) throws HiveException {
    ArrayList<Object> nr = new ArrayList<Object> ();
    for (int i = 0; i < valueFields.size(); i++) {
      nr.add(ObjectInspectorUtils.copyToStandardObject(valueFields.get(i)
          .evaluate(row), valueFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE));
    }
    return nr;
  }

/*  private ArrayList<Object> computeValue1(Object row, List<ExprNodeEvaluator> valueFields,
      List<ObjectInspector> valueFieldsOI) throws HiveException {
    ArrayList<Object> nr = new ArrayList<Object>(3);
    for (int i = 0; i < valueFields.size(); i++) {
      nr.add(ObjectInspectorUtils.copyToStandardObject(valueFields.get(i)
          .evaluate(row), valueFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE));
    }
    return nr;
  }*/

  private void store(Interval interval, int tag) {
    ArrayList<Interval> list = storage[tag];
    list.add(interval);
/*    if (list.isEmpty() || list.get(list.size() - 1).start <= interval.start) {
      list.add(interval);
    }
    else {
      int low = 0, high = list.size() - 1;
      int pos = 0;
      while (low <= high) {
        pos = (low + high) / 2;
        if (list.get(pos).start == interval.start) {
          break;
        } else if (list.get(pos).start < interval.start) {
          low = pos + 1;
        } else if (list.get(pos).start > interval.start) {
          high = pos - 1;
        }
      }
      if (low > high){
        list.add(low, interval);
      }
      else {
        list.add(pos + 1, interval);
      }
    }*/
  }

  private void genAndForwardObject() throws HiveException {
    ArrayList<Interval> intermediate = storage[0];
    Collections.sort(intermediate, new IntervalStartComparator());
    String vd = conf.getVd();
    for (byte alias = 1; alias < conf.getNumSources(); alias ++) {
      ArrayList<Interval> left = intermediate;
      ArrayList<Interval> right = storage[alias];
      Collections.sort(right, new IntervalStartComparator());
      intermediate = new ArrayList<Interval>();
      int leftPos = 0, rightPos = 0;
      LinkedList<Interval> mainStream = new LinkedList<Interval> ();
      LinkedList<Interval> auxiStream = new LinkedList<Interval> ();
      while (leftPos < left.size() || rightPos < right.size()) {
        int leftStart = leftPos == left.size() ? Integer.MAX_VALUE : left.get(leftPos).getStart();
        int rightStart = rightPos == right.size() ? Integer.MAX_VALUE : right.get(rightPos).getStart();
        if (leftStart <= rightStart) {
          Interval interval = left.get(leftPos ++);
          while (!auxiStream.isEmpty()) {
            if (auxiStream.peek().isBefore(interval)) {
              auxiStream.remove();
            } else {
              break;
            }
          }
          for (int index = 0; index < auxiStream.size(); index ++) {
            if (auxiStream.get(index).overlapsWith(interval)) {
              int maxStart = auxiStream.get(index).getStart() > interval.getStart() ? auxiStream.get(index).getStart() : interval.getStart();
              int minEnd = auxiStream.get(index).getEnd() < interval.getEnd() ? auxiStream.get(index).getEnd() : interval.getEnd();
              if (alias == conf.getNumSources() - 1) {
                forwardCache[chrIndex] = new Text(curChr);
                forwardCache[startIndex] = new IntWritable(maxStart);
                forwardCache[endIndex] = new IntWritable(minEnd);
                if (vd != null) {
                  String vm = conf.getVm();
                  float value1 = interval.getValue();
                  float value2 = auxiStream.get(index).getValue();
                  if (vm.equals("total")) {
                    value1 = value1 * (minEnd - maxStart + 1) / (interval.getEnd() - interval.getStart() + 1);
                    value2 = value2 * (minEnd - maxStart + 1) / (auxiStream.get(index).getEnd() - auxiStream.get(index).getStart() + 1);
                  }
                  if (vd.equals("sum")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 + value2);
                  } else if (vd.equals("avg")) {
                    forwardCache[valueIndex] = new FloatWritable((value1 + value2) / 2);
                  } else if (vd.equals("max")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 > value2 ? value1 : value2);
                  } else if (vd.equals("min")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 < value2 ? value1 : value2);
                  } else if (vd.equals("diff")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 - value2);
                  } else if (vd.equals("product")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 * value2);
                  } else if (vd.equals("quotient")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 / value2);
                  } else if (vd.equals("left")) {
                    forwardCache[valueIndex] = new FloatWritable(value1);
                  } else if (vd.equals("right")) {
                    forwardCache[valueIndex] = new FloatWritable(value2);
                  } // *** nine vd
                }
                if (interval.getMeta() != null) {
                  for (int i = 0; i < forwardCache.length; i ++) {
                    if (interval.getMeta().get(i) != null) {
                      forwardCache[i] = interval.getMeta().get(i);
                    }
                  }
                }
                forward(forwardCache, outputObjInspector);
              }
            /*  else {
                if (vd != null) {
                  if (vd.equals("sum")) {
                    intermediate.add(new Interval(maxStart, minEnd, interval.value + auxiStream.get(index).value));
                  } else if (vd.equals("difference")) {
                    intermediate.add(new Interval(maxStart, minEnd, interval.value - auxiStream.get(index).value));
                  } else if (vd.equals("division")) {
                    intermediate.add(new Interval(maxStart, minEnd, interval.value / auxiStream.get(index).value));
                  }
                }
                else {
                  intermediate.add(new Interval(maxStart, minEnd));
                }
              } */
            }
          }
          mainStream.add(interval);
        }
        else {
          Interval interval = right.get(rightPos ++);
          while (!mainStream.isEmpty()) {
            if (mainStream.peek().isBefore(interval)) {
              mainStream.remove();
            } else {
              break;
            }
          }
          for (int index = 0; index < mainStream.size(); index ++) {
            if (mainStream.get(index).overlapsWith(interval)) {
              int maxStart = mainStream.get(index).getStart() > interval.getStart() ? mainStream.get(index).getStart() : interval.getStart();
              int minEnd = mainStream.get(index).getEnd() < interval.getEnd() ? mainStream.get(index).getEnd() : interval.getEnd();
              if (alias == conf.getNumSources() - 1) {
                forwardCache[chrIndex] = new Text(curChr);
                forwardCache[startIndex] = new IntWritable(maxStart);
                forwardCache[endIndex] = new IntWritable(minEnd);
                if (vd != null) {
                  String vm = conf.getVm();
                  float value2 = interval.getValue();
                  float value1 = mainStream.get(index).getValue();
                  if (vm.equals("total")) {
                    value2 = value2 * (minEnd - maxStart + 1) / (interval.getEnd() - interval.getStart() + 1);
                    value1 = value1 * (minEnd - maxStart + 1) / (mainStream.get(index).getEnd() - mainStream.get(index).getStart() + 1);
                  }
                  if (vd.equals("sum")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 + value2);
                  } else if (vd.equals("avg")) {
                    forwardCache[valueIndex] = new FloatWritable((value1 + value2) / 2);
                  } else if (vd.equals("max")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 > value2 ? value1 : value2);
                  } else if (vd.equals("min")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 < value2 ? value1 : value2);
                  } else if (vd.equals("diff")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 - value2);
                  } else if (vd.equals("product")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 * value2);
                  } else if (vd.equals("quotient")) {
                    forwardCache[valueIndex] = new FloatWritable(value1 / value2);
                  } else if (vd.equals("left")) {
                    forwardCache[valueIndex] = new FloatWritable(value1);
                  } else if (vd.equals("right")) {
                    forwardCache[valueIndex] = new FloatWritable(value2);
                  } // *** nine vd
                }
                if (mainStream.get(index).getMeta() != null) {
                  for (int i = 0; i < forwardCache.length; i ++) {
                    if (mainStream.get(index).getMeta().get(i) != null) {
                      forwardCache[i] = mainStream.get(index).getMeta().get(i);
                    }
                  }
                }
                forward(forwardCache, outputObjInspector);
              }
            /*  else {
                if (vd != null) {
                  if (vd.equals("sum")) {
                    intermediate.add(new Interval(maxStart, minEnd, mainStream.get(index).value + interval.value));
                  } else if (vd.equals("difference")) {
                    intermediate.add(new Interval(maxStart, minEnd, mainStream.get(index).value - interval.value));
                  } else if (vd.equals("division")) {
                    intermediate.add(new Interval(maxStart, minEnd, mainStream.get(index).value / interval.value));
                  }
                }
                else {
                  intermediate.add(new Interval(maxStart, minEnd));
                }
              } */
            }
          }
          auxiStream.add(interval);
        }
      }
    }
  }

  @Override
  public void startGroup() throws HiveException {
    for (ArrayList<Interval> list : storage) {
      list.clear();
    }
    super.startGroup();
  }

  @Override
  public void endGroup() throws HiveException {
    genAndForwardObject();
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      for (ArrayList<Interval> list : storage) {
        if (list != null) {
          list.clear();
        }
      }
      Arrays.fill(storage, null);
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
    return "INTERSECTJOIN";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.INTERSECTJOIN;
  }
}
