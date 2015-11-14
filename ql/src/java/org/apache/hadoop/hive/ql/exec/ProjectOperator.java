package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ProjectDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Project operator implementation.
 **/
public class ProjectOperator extends Operator<ProjectDesc> implements
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

    public Interval(int start, int end) {
      this.start = start;
      this.end = end;
      metadata = null;
    }

    public Interval(int start, int end, float value) {
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

    void setFields(ArrayList<Object> metadata) {
      this.metadata = metadata;
    }

    ArrayList<Object> getMeta() {
      return metadata;
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

    boolean isBefore(Interval I) {
      return end < I.getStart();
    }

    boolean isAfter(Interval I) {
      return start > I.getEnd();
    }

    boolean overlapsWith(Interval I) {
      return (start <= I.getEnd() && end >= I.getStart());
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


  private transient ArrayList<Interval>[] storage;
  private transient String curChr = null;
  int chrIndex = -1, startIndex = -1, endIndex = -1, valueIndex = -1;
  boolean metadata = false;


  @SuppressWarnings("unchecked")
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    storage = new ArrayList[2];
    for (int i = 0; i < storage.length; i++) {
      storage[i] = new ArrayList<Interval>();
    }

    numAliases = conf.getExprMap().size();
    joinValues = new List[2];

    populateJoinValues(joinValues, conf.getExprMap());
    joinValuesObjectInspectors = getObjectInspectorsFromEvaluators(joinValues, inputObjInspectors);
    joinValuesStandardObjectInspectors = getStandardObjectInspectors(joinValuesObjectInspectors);

    // forwardCache = new Object[4];

    List<String> outCols = conf.getOutputColumnNames();
    forwardCache = new Object[outCols.size()];
    for (int index = 0; index < outCols.size(); index++) {
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

    if (valueIndex >= 0) {
      joinValuesStandardObjectInspectors[1].add(PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(PrimitiveCategory.FLOAT));
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

  private List<ObjectInspector>[] getObjectInspectorsFromEvaluators(
      List<ExprNodeEvaluator>[] exprEntries,
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
    String chr = tag == 1 ? ((Text) nr.get(chrIndex)).toString() : ((Text) nr.get(0)).toString();
    int start = tag == 1 ? ((IntWritable) nr.get(startIndex)).get() : ((IntWritable) nr.get(1))
        .get();
    int end = tag == 1 ? ((IntWritable) nr.get(endIndex)).get() : ((IntWritable) nr.get(2)).get();
    if (curChr == null || !curChr.equals(chr)) {
      curChr = chr;
    }
    Interval interval;
    float value = 0;
    if (tag == 0 && valueIndex >= 0) {
      Object o = nr.get(3);
      if (o instanceof FloatWritable) {
        value = ((FloatWritable) o).get();
      } else if (o instanceof DoubleWritable) {
        value = (float) ((DoubleWritable) o).get();
      }
      interval = new Interval(start, end, value);
    } else {
      interval = new Interval(start, end);
    }

/*
    if (valueIndex >= 0) {
      float value = tag == 1 ? ((FloatWritable) nr.get(valueIndex)).get() : ((FloatWritable) nr
          .get(3)).get();
      interval = new Interval(start, end, value);
    }
    else {
      interval = new Interval(start, end);
    }*/

    if (tag == 1 && metadata) {
      nr.set(chrIndex, null);
      nr.set(startIndex, null);
      nr.set(endIndex, null);
//      if (valueIndex >= 0) {
//        nr.set(valueIndex, null);
//      }
      interval.setFields(nr);
    }

    store(interval, tag);

  }

  private ArrayList<Object> computeValue(Object row, List<ExprNodeEvaluator> valueFields,
      List<ObjectInspector> valueFieldsOI) throws HiveException {
    ArrayList<Object> nr = new ArrayList<Object>(valueFields.size());
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
    /*
     * ArrayList<Interval> list = storage[tag];
     * if (list.isEmpty() || list.get(list.size() - 1).start <= interval.start) {
     * list.add(interval);
     * }
     * else {
     * int low = 0, high = list.size() - 1;
     * int pos = 0;
     * while (low <= high) {
     * pos = (low + high) / 2;
     * if (list.get(pos).start == interval.start) {
     * break;
     * } else if (list.get(pos).start < interval.start) {
     * low = pos + 1;
     * } else if (list.get(pos).start > interval.start) {
     * high = pos - 1;
     * }
     * }
     * if (low > high){
     * list.add(low, interval);
     * }
     * else {
     * list.add(pos + 1, interval);
     * }
     * }
     */
  }

  private void genAndForwardObject() throws HiveException {
    ArrayList<Interval> main = storage[1];
    ArrayList<Interval> auxi = storage[0];
    Collections.sort(main, new IntervalStartComparator());
    Collections.sort(auxi, new IntervalStartComparator());
    String vd = conf.getVd();
    Iterator<Interval> mainIter = main.iterator();
    int auxiSize = auxi.size();
    boolean[] significant = new boolean [auxiSize];
    Arrays.fill(significant, false);
    ArrayList<PosIndexPair> pipList = new ArrayList<PosIndexPair> ();
    Interval interval;
    int pCursor = 0, qCursor;
    while(mainIter.hasNext()) {
      interval = mainIter.next();
      forwardCache[chrIndex] = new Text(curChr);
      forwardCache[startIndex] = new IntWritable(interval.getStart());
      forwardCache[endIndex] = new IntWritable(interval.getEnd());

      if (vd != null) {
        String vm = conf.getVm();
        float value = 0;
        while (pCursor < auxiSize && auxi.get(pCursor).isBefore(interval)) {
          pCursor ++;
        }
        if (pCursor < auxiSize) {
          qCursor = pCursor;
          if (vm.equals("total")) {
            if (vd.equals("max")) {
              value = Float.MIN_VALUE;
            } else if (vd.equals("min")) {
              value = Float.MAX_VALUE;
            } else if (vd.equals("product")) {
              value = 1;
            }
            int counter = 0;
            while (qCursor < auxiSize && !interval.isBefore(auxi.get(qCursor))) {
              if (!auxi.get(qCursor).isBefore(interval)) {
                counter ++;
                int start = interval.getStart() > auxi.get(qCursor).getStart() ? interval.getStart() : auxi.get(qCursor).getStart();
                int end = interval.getEnd() < auxi.get(qCursor).getEnd() ? interval.getEnd() : auxi.get(qCursor).getEnd();
                float v = auxi.get(qCursor).getValue() * (end - start + 1)
                    / (auxi.get(qCursor).getEnd() - auxi.get(qCursor).getStart() + 1);
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
              qCursor ++;
            }
            if (counter == 0) {
              value = 0;
            } else {
              if (vd.equals("avg")) {
                value = value / counter;
              }
            }
          } else {
            int maxEnd = 0;
            while (qCursor < auxiSize && !interval.isBefore(auxi.get(qCursor))) {
              if (!auxi.get(qCursor).isBefore(interval)) {
                int start = interval.getStart() > auxi.get(qCursor).getStart() ? interval.getStart() : auxi.get(qCursor).getStart();
                int end = interval.getEnd() < auxi.get(qCursor).getEnd() ? interval.getEnd() : auxi.get(qCursor).getEnd();
                if (start > maxEnd) {
                  if (qCursor + 1 == auxiSize || end < auxi.get(qCursor + 1).start) {
                    value += auxi.get(qCursor).value * (end - start + 1);
                  } else {
                    pipList.add(new PosIndexPair(start - 1, qCursor));
                    pipList.add(new PosIndexPair(end, qCursor));
                  }
                } else {
                  pipList.add(new PosIndexPair(start - 1, qCursor));
                  pipList.add(new PosIndexPair(end, qCursor));
                  if (qCursor + 1 == auxiSize || end < auxi.get(qCursor + 1).start) {
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
                        for (int cur = pCursor; cur <= qCursor; cur ++) {
                          if (significant[cur]) {
                            counter ++;
                            if (vd.equals("sum") || vd.equals("avg")) {
                              segValue += auxi.get(cur).getValue();
                            } else if (vd.equals("product")) {
                              segValue *= auxi.get(cur).getValue();
                            } else if (vd.equals("max")) {
                              segValue = segValue > auxi.get(cur).getValue() ? segValue : auxi.get(cur).getValue();
                            } else if (vd.equals("min")) {
                              segValue = segValue < auxi.get(cur).getValue() ? segValue : auxi.get(cur).getValue();
                            } // *** five vd
                          }
                        }
                        if (counter > 0) {
                          if (vd.equals("avg")) {
                            segValue = segValue / counter;
                          }
                          value += segValue * (pip.getPos() - prePos);
                        }
                      }
                      significant[pip.getIndex()] = !significant[pip.getIndex()];
                      prePos = pip.getPos();
                    }
                    pipList.clear();
                  }
                }
                maxEnd = end > maxEnd ? end : maxEnd;
              }
              qCursor ++;
            }
            value = value / (interval.end - interval.start + 1);
          }
        }
        forwardCache[valueIndex] = new FloatWritable (value);
      }

      if (interval.getMeta() != null) {
        for (int i = 0; i < joinValues[1].size(); i ++) {
          if (interval.getMeta().get(i) != null) {
            forwardCache[i] = interval.getMeta().get(i);
            }
          }
      }

      forward(forwardCache, outputObjInspector);

    }




/*    int leftPos = 0, rightPos = 0;
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
            int maxStart = auxiStream.get(index).start > interval.start ? auxiStream.get(index).start : interval.start;
            int minEnd = auxiStream.get(index).end < interval.end ? auxiStream.get(index).end : interval.end;
            float newValue = auxiStream.get(index).value * (minEnd - maxStart + 1) / (interval.end - interval.start + 1);
            interval.setValue(newValue);
          }
        }
        mainStream.add(interval);
      }
      else {
        Interval interval = right.get(rightPos ++);
        while (!mainStream.isEmpty()) {
          if (mainStream.peek().isBefore(interval)) {
            forwardCache[0] = new Text(curChr);
            forwardCache[1] = new IntWritable(mainStream.peek().start);
            forwardCache[2] = new IntWritable(mainStream.peek().end);
            forwardCache[3] = new FloatWritable(mainStream.peek().value);
            forward(forwardCache, outputObjInspector);
            mainStream.remove();
          } else {
            break;
          }
        }
        for (int index = 0; index < mainStream.size(); index ++) {
          if (mainStream.get(index).overlapsWith(interval)) {
            int maxStart = mainStream.get(index).start > interval.start ? mainStream.get(index).start : interval.start;
            int minEnd = mainStream.get(index).end < interval.end ? mainStream.get(index).end : interval.end;
            float newValue = mainStream.get(index).value +
                interval.value * (minEnd - maxStart + 1) / (mainStream.get(index).end - mainStream.get(index).start + 1);
            mainStream.get(index).setValue(newValue);
          }
        }
        auxiStream.add(interval);
      }
    }
    while (!mainStream.isEmpty()) {
      forwardCache[0] = new Text(curChr);
      forwardCache[1] = new IntWritable(mainStream.peek().start);
      forwardCache[2] = new IntWritable(mainStream.peek().end);
      forwardCache[3] = new FloatWritable(mainStream.peek().value);
      forward(forwardCache, outputObjInspector);
      mainStream.remove();
    } */
//    System.out.println(curChr);
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
    return "PROJECT";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.PROJECT;
  }
}
