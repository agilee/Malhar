/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.datatorrent.lib.math;

import com.datatorrent.engine.TestSink;
import com.datatorrent.lib.math.SumKeyVal;
import com.datatorrent.lib.util.KeyValPair;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * Functional tests for {@link com.datatorrent.lib.math.SumKeyVal}. <p>
 *
 */
public class SumKeyValTest
{
  /**
   * Test operator logic emits correct results.
   */
  @Test
  public void testNodeProcessing()
  {
    SumKeyVal<String, Double> oper = new SumKeyVal<String, Double>();
    oper.setType(Double.class);
    TestSink sumSink = new TestSink();
    oper.sum.setSink(sumSink);

    oper.beginWindow(0); //

    oper.data.process(new KeyValPair("a", 2.0));
    oper.data.process(new KeyValPair("b", 20.0));
    oper.data.process(new KeyValPair("c", 1000.0));
    oper.data.process(new KeyValPair("a", 1.0));
    oper.data.process(new KeyValPair("a", 10.0));
    oper.data.process(new KeyValPair("b", 5.0));
    oper.data.process(new KeyValPair("d", 55.0));
    oper.data.process(new KeyValPair("b", 12.0));
    oper.data.process(new KeyValPair("d", 22.0));
    oper.data.process(new KeyValPair("d", 14.2));
    oper.data.process(new KeyValPair("d", 46.0));
    oper.data.process(new KeyValPair("e", 2.0));
    oper.data.process(new KeyValPair("a", 23.0));
    oper.data.process(new KeyValPair("d", 4.0));

    oper.endWindow(); //

    // payload should be 1 bag of tuples with keys "a", "b", "c", "d", "e"
    Assert.assertEquals("number emitted tuples", 5, sumSink.collectedTuples.size());
    for (Object o: sumSink.collectedTuples) {
      KeyValPair<String, Double> e = (KeyValPair<String, Double>)o;
      Double val = (Double)e.getValue();
      if (e.getKey().equals("a")) {
        Assert.assertEquals("emitted value for 'a' was ", new Double(36), val);
      }
      else if (e.getKey().equals("b")) {
        Assert.assertEquals("emitted tuple for 'b' was ", new Double(37), val);
      }
      else if (e.getKey().equals("c")) {
        Assert.assertEquals("emitted tuple for 'c' was ", new Double(1000), val);
      }
      else if (e.getKey().equals("d")) {
        Assert.assertEquals("emitted tuple for 'd' was ", new Double(141.2), val);
      }
      else if (e.getKey().equals("e")) {
        Assert.assertEquals("emitted tuple for 'e' was ", new Double(2), val);
      }
    }
  }
}