//Added by Qiang

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.lazy.LazyFloat;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;



/**
 * I use this UDTF to split some rows into more rows
 *
 */
public class GenericUDTFProject extends GenericUDTF {

  Object forwardObj[] = new Object[4];

  int index = -1;
  int binSize = 0;
  String currChr = null;

  float avgValue = 0;
  boolean firstRow = true;

  int endPosition;
  private static HashMap<String, Integer> chrLength;
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
    chrLength.put("chrS", new Integer(355));  // just for test
    chrLength.put("chrT", new Integer(533));
  }

  @Override
  public void close() throws HiveException {
    constructAndForward();
    outputEmpty();
    }


  private void outputEmpty() throws HiveException {
    // TODO Auto-generated method stub
    int end = ((IntWritable) ((LazyInteger) forwardObj[2]).getWritableObject()).get();
    if (end < endPosition) {
    while (end < endPosition) {
      avgValue = 0;
      index++;
      constructAndForward();
      end = (index + 1) * binSize;
  }
  }
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
    ArrayList<String> fieldNames = new ArrayList<String>();
    ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
    fieldNames.add("chr");
    fieldOIs.add(argOIs[1]);
    fieldNames.add("start");
    fieldOIs.add(argOIs[2]);
    fieldNames.add("endd");
    fieldOIs.add(argOIs[3]);
    fieldNames.add("value");
    fieldOIs.add(argOIs[4]);
//    for (int i = 1; i < 5; i++) {
//      fieldNames.add("c" + i);
//      fieldOIs.add(argOIs[i]);
//    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,
        fieldOIs);
  }

  @Override
  public void process(Object[] args) throws HiveException {
    binSize = ((IntWritable) args[0]).get();
    String chr = ((Text) ((LazyString) args[1]).getWritableObject()).toString();

    if (currChr == null) {
      currChr = chr;
    }

    if (currChr != null && !currChr.equals(chr)) {
//    Because we can't save the original array of Object, I have to save the value temporarily first.
      int tmpStart = ((IntWritable) ((LazyInteger) args[2]).getWritableObject()).get();
      int tmpEnd = ((IntWritable) ((LazyInteger) args[3]).getWritableObject()).get();
      float tmpValue = ((FloatWritable) ((LazyFloat) args[4]).getWritableObject()).get();
      close();
      (((LazyString) args[1]).getWritableObject()).set(chr);
      ((IntWritable) ((LazyInteger) args[2]).getWritableObject()).set(tmpStart);
      ((IntWritable) ((LazyInteger) args[3]).getWritableObject()).set(tmpEnd);
      ((FloatWritable) ((LazyFloat) args[4]).getWritableObject()).set(tmpValue);
      firstRow = true;
      currChr = chr;
      this.process(args);
      return;
    }

    endPosition = chrLength.get(currChr).intValue();

    int start = ((IntWritable) ((LazyInteger) args[2]).getWritableObject()).get();
    int end = ((IntWritable) ((LazyInteger) args[3]).getWritableObject()).get();
    float originalValue = ((FloatWritable) ((LazyFloat) args[4]).getWritableObject()).get();

    int binIndex = (start - 1) / binSize;
    if (end <= (binIndex + 1) * binSize) {
      float value = originalValue * (end - start + 1) / binSize;
      generateNewOutput(binIndex, binSize, value, args);
    }
    else {
      float value = originalValue * ((binIndex + 1) * binSize - start + 1) / binSize;
      generateNewOutput(binIndex, binSize, value, args);
      binIndex++;
      while (end > (binIndex + 1) * binSize) {
        generateNewOutput(binIndex, binSize, originalValue, args);
        binIndex++;
      }
      value = originalValue * (end - binIndex * binSize) / binSize;
      generateNewOutput(binIndex, binSize, value, args);
    }
  }

  private void generateNewOutput(int binIndex, int binSize, float value, Object[] args) throws HiveException {
    // TODO Auto-generated method stub
      if (firstRow) {
        for (int i = 0; i < 4; i++) {
          forwardObj[i] = args[i+1];
        }
        index = binIndex;
        avgValue = value;
        firstRow = false;
      }
      else {
        if (binIndex == index) {
          avgValue += value;
        }
        else if ( binIndex == index + 1) {
          constructAndForward();
          index = binIndex;
          avgValue = value;
        }
        else if (binIndex > index + 1) {
          constructAndForward();
          do {
            index++;
            avgValue = 0;
            constructAndForward();
        } while (binIndex > index + 1);
          index = binIndex;
          avgValue = value;
    }
  }
  }

  private void constructAndForward() throws HiveException {
    // TODO Auto-generated method stub
    int stdStart = index * binSize + 1;
    int stdEnd = (index + 1) * binSize;
    if (stdEnd > endPosition) {
      stdEnd = endPosition;
      avgValue = avgValue * binSize / (stdEnd - stdStart + 1);
    }
    (((LazyString) forwardObj[0]).getWritableObject()).set(currChr);
    ((IntWritable) ((LazyInteger) forwardObj[1]).getWritableObject()).set(stdStart);
    ((IntWritable) ((LazyInteger) forwardObj[2]).getWritableObject()).set(stdEnd);
    ((FloatWritable) ((LazyFloat) forwardObj[3]).getWritableObject()).set(avgValue);
    forward(forwardObj);
  }
}
