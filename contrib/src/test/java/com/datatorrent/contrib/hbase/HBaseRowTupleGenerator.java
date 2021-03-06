/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hbase;

import com.datatorrent.lib.io.SimpleSinglePortInputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

/**
 *
 */
public class HBaseRowTupleGenerator extends BaseOperator implements InputOperator
{

  int rowCount;

  public final transient DefaultOutputPort<HBaseTuple> outputPort = new DefaultOutputPort<HBaseTuple>();

  @Override
  public void emitTuples()
  {
    HBaseTuple tuple = new HBaseTuple();
    tuple.setRow("row" + rowCount);
    tuple.setColFamily("colfam0");
    tuple.setColName("col" + "-" +0);
    tuple.setColValue("val" + "-" + rowCount + "-" + 0);
    ++rowCount;
    outputPort.emit(tuple);
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    rowCount = 0;
  }

}
