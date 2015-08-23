package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.PairLocationCompDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * PairLocationComp operator implementation.
 **/
public class PairLocationCompOperator extends Operator<PairLocationCompDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;

  private transient List<ExprNodeEvaluator>[] joinValues;
  private transient List<ObjectInspector>[] joinValuesObjectInspectors;
  private transient List<ObjectInspector>[] joinValuesStandardObjectInspectors;
  private transient Object[] forwardCache;

  class Interval {
    ArrayList<Object> metadata;
    int start, end;

    public Interval (int start, int end) {
      this.start = start;
      this.end = end;
      metadata = null;
    }

    void setStart (int start) {
      this.start = start;
    }

    void setEnd (int end) {
      this.end = end;
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
  private int leftChrIndex =  -1, leftStartIndex =  -1, leftEndIndex = -1;
  private int rightChrIndex = -1, rightStartIndex = -1, rightEndIndex = -1;
  private transient String curChr = null;


  @SuppressWarnings("unchecked")
  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    storage = new ArrayList [2];
    for (int i = 0; i < storage.length; i ++) {
      storage[i] = new ArrayList<Interval> ();
    }

    joinValues = new List[2];

    populateJoinValues(joinValues, conf.getExprMap());
    joinValuesObjectInspectors = getObjectInspectorsFromEvaluators(joinValues, inputObjInspectors);
    joinValuesStandardObjectInspectors = getStandardObjectInspectors(joinValuesObjectInspectors);

    List<String> outCols = conf.getOutputColumnNames();
    forwardCache = new Object[outCols.size()];
    int leftColsNum = joinValues[0].size();
/*    for (int index = 0; index < outCols.size(); index ++) {
      if (outCols.get(index).equals("chr")) {
        if (index < leftColsNum) {
          leftChrIndex = index;
        } else {
          rightChrIndex = index - leftColsNum;
        }
      } else if (outCols.get(index).equals("chrstart")) {
        if (index < leftColsNum) {
          leftStartIndex = index;
        } else {
          rightStartIndex = index - leftColsNum;
        }
      } else if (outCols.get(index).equals("chrend")) {
        if (index < leftColsNum) {
          leftEndIndex = index;
        } else {
          rightEndIndex = index - leftColsNum;
        }
      }
    } */

    leftChrIndex = conf.getLeftChrIndex();
    leftStartIndex = conf.getLeftStartIndex();
    leftEndIndex = conf.getLeftEndIndex();
    rightChrIndex = conf.getRightChrIndex() - leftColsNum;
    rightStartIndex = conf.getRightStartIndex() - leftColsNum;
    rightEndIndex = conf.getRightEndIndex() - leftColsNum;;

    List<ObjectInspector> combinedObjectInspectors = new ArrayList<ObjectInspector>();
    for (int i = 0; i < joinValuesStandardObjectInspectors.length; i ++) {
      combinedObjectInspectors.addAll(joinValuesStandardObjectInspectors[i]);
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf
        .getOutputColumnNames(), combinedObjectInspectors);

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
    String chr = tag == 0 ? ((Text)nr.get(leftChrIndex)).toString() : ((Text)nr.get(rightChrIndex)).toString();
    int start = tag == 0 ? ((IntWritable)nr.get(leftStartIndex)).get() : ((IntWritable)nr.get(rightStartIndex)).get();
    int end = tag == 0 ? ((IntWritable)nr.get(leftEndIndex)).get() : ((IntWritable)nr.get(rightEndIndex)).get();
    if (curChr == null || !curChr.equals(chr)) {
      curChr = chr;
    }
    Interval interval = new Interval(start, end);

    if (tag == 0) {
      nr.set(leftChrIndex, null);
      nr.set(leftStartIndex, null);
      nr.set(leftEndIndex, null);
    } else {
      nr.set(rightChrIndex, null);
      nr.set(rightStartIndex, null);
      nr.set(rightEndIndex, null);
    }
    interval.setFields(nr);
    store(interval, tag);

  }

  private ArrayList<Object> computeValue(Object row, List<ExprNodeEvaluator> valueFields,
      List<ObjectInspector> valueFieldsOI) throws HiveException {
    ArrayList<Object> nr = new ArrayList<Object>();
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
  }

  private void genAndForwardObjectForMatches() throws HiveException {
    ArrayList<Interval> left = storage[0];
    ArrayList<Interval> right = storage[1];

    Collections.sort(left, new IntervalStartComparator());
    Collections.sort(right, new IntervalStartComparator());

    int leftColsNum = joinValues[0].size();
    int rightColsNum = joinValues[1].size();
    int pCursor = 0, qCursor = 0;
    for (int i = 0; i < left.size(); i ++) {
      while (pCursor < right.size() && right.get(pCursor).getStart() < left.get(i).getStart()) {
        pCursor ++;
      }
      while (qCursor < right.size() && right.get(qCursor).getStart() <= left.get(i).getStart()) {
        qCursor ++;
      }
      for (int j = pCursor; j < qCursor; j ++) {
        if (right.get(j).getEnd() == left.get(i).getEnd()) {
          forwardCache[leftChrIndex] = new Text(curChr);
          forwardCache[leftStartIndex] = new IntWritable(left.get(i).start);
          forwardCache[leftEndIndex] = new IntWritable(left.get(i).end);
          for (int index = 0; index < leftColsNum; index ++) {
            if (left.get(i).getMeta().get(index) != null) {
              forwardCache[index] = left.get(i).getMeta().get(index);
            }
          }
          forwardCache[rightChrIndex + leftColsNum] = new Text(curChr);
          forwardCache[rightStartIndex + leftColsNum] = new IntWritable(right.get(j).start);
          forwardCache[rightEndIndex + leftColsNum] = new IntWritable(right.get(j).end);
          for (int index = 0; index < rightColsNum; index ++) {
            if (right.get(j).getMeta().get(index) != null) {
              forwardCache[index + leftColsNum] = right.get(j).getMeta().get(index);
            }
          }
          forward(forwardCache, outputObjInspector);
        }
      }
    }
  }


  private void genAndForwardObjectForClosestTo() throws HiveException {
    ArrayList<Interval> left = storage[0];
    ArrayList<Interval> right = storage[1];

    Collections.sort(left, new IntervalStartComparator());
    Collections.sort(right, new IntervalStartComparator());

    int leftColsNum = joinValues[0].size();
    int rightColsNum = joinValues[1].size();

    int maxLeftEnd = 0;
    int leftClosestDis = Integer.MAX_VALUE;
    int pCursor = 0;
    ArrayList<Interval> leftCandidates = new ArrayList<Interval> ();

    for (int i = 0; i < right.size(); i ++) {
      Interval interval = right.get(i);
      forwardCache[rightChrIndex + leftColsNum] = new Text(curChr);
      forwardCache[rightStartIndex + leftColsNum] = new IntWritable(right.get(i).start);
      forwardCache[rightEndIndex + leftColsNum] = new IntWritable(right.get(i).end);
      for (int index = 0; index < rightColsNum; index ++) {
        if (right.get(i).getMeta().get(index) != null) {
          forwardCache[index + leftColsNum] = right.get(i).getMeta().get(index);
        }
      }
      while (pCursor < left.size() && left.get(pCursor).isBefore(interval)) {
        if (left.get(pCursor).getEnd() > maxLeftEnd) {
          maxLeftEnd = left.get(pCursor).getEnd();
          leftCandidates.clear();
          leftCandidates.add(left.get(pCursor));
        } else if (left.get(pCursor).getEnd() == maxLeftEnd) {
          leftCandidates.add(left.get(pCursor));
        }
        pCursor ++;
      }
      leftClosestDis = maxLeftEnd == 0 ? Integer.MAX_VALUE : interval.start - maxLeftEnd;
      if (pCursor == left.size()) {
        for (int j = 0; j < leftCandidates.size(); j ++) {
          forwardCache[leftChrIndex] = new Text(curChr);
          forwardCache[leftStartIndex] = new IntWritable(leftCandidates.get(j).start);
          forwardCache[leftEndIndex] = new IntWritable(leftCandidates.get(j).end);
          for (int index = 0; index < leftColsNum; index ++) {
            if (leftCandidates.get(j).getMeta().get(index) != null) {
              forwardCache[index] = leftCandidates.get(j).getMeta().get(index);
            }
          }
          forward(forwardCache, outputObjInspector);
        }
      } else if (left.get(pCursor).overlapsWith(interval)) {
        int qCursor = pCursor;
        while (qCursor < left.size() && !left.get(qCursor).isAfter(interval)) {
          if (left.get(qCursor).overlapsWith(interval)) {
            forwardCache[leftChrIndex] = new Text(curChr);
            forwardCache[leftStartIndex] = new IntWritable(left.get(qCursor).start);
            forwardCache[leftEndIndex] = new IntWritable(left.get(qCursor).end);
            for (int index = 0; index < leftColsNum; index ++) {
              if (left.get(qCursor).getMeta().get(index) != null) {
                forwardCache[index] = left.get(qCursor).getMeta().get(index);
              }
            }
            forward(forwardCache, outputObjInspector);
          }
          qCursor ++;
        }
      } else {
        int rightClosestDis = left.get(pCursor).start - interval.end;
        if (leftClosestDis > rightClosestDis) {
          int qCursor = pCursor;
          int rightMinStart = left.get(pCursor).start;
          do {
            forwardCache[leftChrIndex] = new Text(curChr);
            forwardCache[leftStartIndex] = new IntWritable(left.get(qCursor).start);
            forwardCache[leftEndIndex] = new IntWritable(left.get(qCursor).end);
            for (int index = 0; index < leftColsNum; index ++) {
              if (left.get(qCursor).getMeta().get(index) != null) {
                forwardCache[index] = left.get(qCursor).getMeta().get(index);
              }
            }
            forward(forwardCache, outputObjInspector);
            qCursor ++;
          } while (qCursor < left.size() && left.get(qCursor).start == rightMinStart);
        } else if (leftClosestDis == rightClosestDis) {
          for (int j = 0; j < leftCandidates.size(); j ++) {
            forwardCache[leftChrIndex] = new Text(curChr);
            forwardCache[leftStartIndex] = new IntWritable(leftCandidates.get(j).start);
            forwardCache[leftEndIndex] = new IntWritable(leftCandidates.get(j).end);
            for (int index = 0; index < leftColsNum; index ++) {
              if (leftCandidates.get(j).getMeta().get(index) != null) {
                forwardCache[index] = leftCandidates.get(j).getMeta().get(index);
              }
            }
            forward(forwardCache, outputObjInspector);
          }
          int qCursor = pCursor;
          int rightMinStart = left.get(pCursor).start;
          do {
            forwardCache[leftChrIndex] = new Text(curChr);
            forwardCache[leftStartIndex] = new IntWritable(left.get(qCursor).start);
            forwardCache[leftEndIndex] = new IntWritable(left.get(qCursor).end);
            for (int index = 0; index < leftColsNum; index ++) {
              if (left.get(qCursor).getMeta().get(index) != null) {
                forwardCache[index] = left.get(qCursor).getMeta().get(index);
              }
            }
            forward(forwardCache, outputObjInspector);
            qCursor ++;
          } while (qCursor < left.size() && left.get(qCursor).start == rightMinStart);
        } else {
          for (int j = 0; j < leftCandidates.size(); j ++) {
            forwardCache[leftChrIndex] = new Text(curChr);
            forwardCache[leftStartIndex] = new IntWritable(leftCandidates.get(j).start);
            forwardCache[leftEndIndex] = new IntWritable(leftCandidates.get(j).end);
            for (int index = 0; index < leftColsNum; index ++) {
              if (leftCandidates.get(j).getMeta().get(index) != null) {
                forwardCache[index] = leftCandidates.get(j).getMeta().get(index);
              }
            }
            forward(forwardCache, outputObjInspector);
          }
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
    if (conf.getLocationComparator().equals("matches")) {
      genAndForwardObjectForMatches();
    } else if (conf.getLocationComparator().equals("closestto")) {
      genAndForwardObjectForClosestTo();
    }
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
    return "PAIRLOCATIONCOMPARATOR";
  }

  @Override
  public OperatorType getType() {
    return OperatorType.PAIRLOCATIONCOMP;
  }
}

