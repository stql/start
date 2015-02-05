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
import org.apache.hadoop.hive.ql.plan.ExclusivenessJoinDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * ExclusivenessJoin operator implementation.
 **/
public class ExclusivenessJoinOperator extends Operator<ExclusivenessJoinDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;

  private transient int numAliases;
  private transient List<ExprNodeEvaluator>[] joinValues;
  private transient List<ObjectInspector>[] joinValuesObjectInspectors;
  private transient List<ObjectInspector>[] joinValuesStandardObjectInspectors;
  private transient Object[] forwardCache;

  class Interval {
    int start, end;
    float value = 0;

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
  private transient String curChr;


  @SuppressWarnings("unchecked")
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    storage = new ArrayList [2];
    for (int i = 0; i < storage.length; i ++) {
      storage[i] = new ArrayList<Interval> ();
    }

    numAliases = conf.getExprMap().size();
    joinValues = new List[2];

    populateJoinValues(joinValues, conf.getExprMap());
    joinValuesObjectInspectors = getObjectInspectorsFromEvaluators(joinValues, inputObjInspectors);
    joinValuesStandardObjectInspectors = getStandardObjectInspectors(joinValuesObjectInspectors);

    if (conf.getAggregation() == null) {
      forwardCache = new Object[3];
    } else {
      forwardCache = new Object[4];
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf
        .getOutputColumnNames(), joinValuesStandardObjectInspectors[1]);

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
    List<ObjectInspector>[] result = new List[2];
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
    List<ObjectInspector>[] result = new List[2];
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

  }

  private ArrayList<Object> computeValue(Object row, List<ExprNodeEvaluator> valueFields,
      List<ObjectInspector> valueFieldsOI) throws HiveException {
    ArrayList<Object> nr = new ArrayList<Object>(3);
    for (int i = 0; i < valueFields.size(); i++) {
      nr.add(ObjectInspectorUtils.copyToStandardObject(valueFields.get(i)
          .evaluate(row), valueFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE));
    }
    return nr;
  }

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
    ArrayList<Interval> left = storage[0];
    ArrayList<Interval> right = storage[1];

    Collections.sort(left, new IntervalStartComparator());
    Collections.sort(right, new IntervalStartComparator());

    String agg = conf.getAggregation();

    int leftPos = 0, rightPos = 0;
    LinkedList<Interval> mainStream = new LinkedList<Interval> ();
    LinkedList<Interval> auxiStream = new LinkedList<Interval> ();
    while (leftPos < left.size() || rightPos < right.size()) {
      int leftStart = leftPos == left.size() ? Integer.MAX_VALUE : left.get(leftPos).start;
      int rightStart = rightPos == right.size() ? Integer.MAX_VALUE : right.get(rightPos).start;
      if (leftStart <= rightStart) {
        Interval interval = left.get(leftPos ++);
        while (!auxiStream.isEmpty()) {
          if (auxiStream.peek().isBefore(interval)) {
            auxiStream.remove();
          } else {
            break;
          }
        }
        int maxAuxiEnd = 0;
        for (int index = 0; index < auxiStream.size(); index ++) {
          if (auxiStream.get(index).overlapsWith(interval)) {
            if (auxiStream.get(index).end > maxAuxiEnd) {
              maxAuxiEnd = auxiStream.get(index).end;
            }
          }
        }
        if (maxAuxiEnd < interval.end) {
          if (maxAuxiEnd >= interval.start) {
            interval.setStart(maxAuxiEnd + 1);
          }
          mainStream.add(interval);
        }
      }
      else {
        Interval interval = right.get(rightPos ++);
        while (!mainStream.isEmpty()) {
          if (mainStream.peek().isBefore(interval)) {
            forwardCache[0] = new Text(curChr);
            forwardCache[1] = new IntWritable(mainStream.peek().start);
            forwardCache[2] = new IntWritable(mainStream.peek().end);
            if (agg != null) {
              if (agg.equals("left")) {
                forwardCache[3] = new FloatWritable(mainStream.peek().value);
              }
            }
            forward(forwardCache, outputObjInspector);
            mainStream.remove();
          } else {
            break;
          }
        }
        for (int index = 0; index < mainStream.size(); index ++) {
          if (mainStream.get(index).overlapsWith(interval)) {
            if (interval.end < mainStream.get(index).end && interval.end >= mainStream.get(index).start) {
              if (interval.start > mainStream.get(index).start) {
                forwardCache[0] = new Text(curChr);
                forwardCache[1] = new IntWritable(mainStream.get(index).start);
                forwardCache[2] = new IntWritable(interval.start - 1);
                if (agg != null) {
                  if (agg.equals("left")) {
                    forwardCache[3] = new FloatWritable(mainStream.get(index).value);
                  }
                }
                forward(forwardCache, outputObjInspector);
              }
              mainStream.get(index).setStart(interval.end + 1);
            }
            else if (interval.end >= mainStream.get(index).end) {
              if (interval.start > mainStream.get(index).start) {
                forwardCache[0] = new Text(curChr);
                forwardCache[1] = new IntWritable(mainStream.get(index).start);
                forwardCache[2] = new IntWritable(interval.start - 1);
                if (agg != null) {
                  if (agg.equals("left")) {
                    forwardCache[3] = new FloatWritable(mainStream.get(index).value);
                  }
                }
                forward(forwardCache, outputObjInspector);
              }
              mainStream.remove(index);
              index --;
            }
          }
          else if (mainStream.get(index).isBefore(interval)) {
            forwardCache[0] = new Text(curChr);
            forwardCache[1] = new IntWritable(mainStream.get(index).start);
            forwardCache[2] = new IntWritable(mainStream.get(index).end);
            if (agg != null) {
              if (agg.equals("left")) {
                forwardCache[3] = new FloatWritable(mainStream.get(index).value);
              }
            }
            forward(forwardCache, outputObjInspector);
            mainStream.remove(index);
            index --;
          }
        }
        auxiStream.add(interval);
      }
    }
    while (!mainStream.isEmpty()) {
      forwardCache[0] = new Text(curChr);
      forwardCache[1] = new IntWritable(mainStream.peek().start);
      forwardCache[2] = new IntWritable(mainStream.peek().end);
      if (agg != null) {
        if (agg.equals("left")) {
          forwardCache[3] = new FloatWritable(mainStream.peek().value);
        }
      }
      forward(forwardCache, outputObjInspector);
      mainStream.remove();
    }
  }

  /*
  private void genAndForwardObject() throws HiveException {
    ArrayList<Interval> left = storage[0];
    ArrayList<Interval> right = storage[1];
    int leftPos = 0, rightPos = 0;
    LinkedList<Interval> mainStream = new LinkedList<Interval> ();
    LinkedList<Interval> auxiStream = new LinkedList<Interval> ();
    while (leftPos < left.size() || rightPos < right.size()) {
      int leftStart = leftPos == left.size() ? Integer.MAX_VALUE : left.get(leftPos).start;
      int rightStart = rightPos == right.size() ? Integer.MAX_VALUE : right.get(rightPos).start;
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
            forwardCache[0] = new Text(curChr);
            forwardCache[1] = new IntWritable(interval.start);
            forwardCache[2] = new IntWritable(interval.end);
            forward(forwardCache, outputObjInspector);
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
            forwardCache[0] = new Text(curChr);
            forwardCache[1] = new IntWritable(mainStream.get(index).start);
            forwardCache[2] = new IntWritable(mainStream.get(index).end);
            forward(forwardCache, outputObjInspector);
          }
        }
        auxiStream.add(interval);
      }
    }
  }*/


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
    return "EXCLUSIVENESSJOIN";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.EXCLUSIVENESSJOIN;
  }
}
