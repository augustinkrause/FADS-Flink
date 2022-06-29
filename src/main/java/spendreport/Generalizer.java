/*
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

package spendreport;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Array;
import java.util.*;

/**
 *
 */
public class Generalizer extends ProcessFunction<Tuple2<Tuple, Long>, Tuple> {

	int k;
	long delayConstraint;
	long reuseConstraint;

	int[] keys;

	public Generalizer(int k, long delayConstraint, long reuseConstraint, int[] keys){
		this.k = k;
		this.delayConstraint = delayConstraint; //how long can a tuple stay at max in the buffer, before being released
		this.reuseConstraint = reuseConstraint; //how long can a cluster be reused

		this.keys = keys; //these are the indices of the fields to be considered QIDs
	}
	// This hook is executed before the processing starts, kind of as a set-up
	@Override
	public void open(Configuration parameters){

	}

	// This hook is executed on each element of the data stream
	@Override
	public void processElement(
			Tuple2<Tuple, Long> element,
			Context context,
			Collector<Tuple> collector) throws Exception {

	}

}
