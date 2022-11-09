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

package fads;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Skeleton code for the datastream walkthrough
 */
public class TransactionGeneralizationJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> lines = env
				.readTextFile("/Users/Augustin/Desktop/augustin/uni/ROC/project/data/database/part_s.tbl")
				.name("Read data");

		TypeInformation[] types = new TypeInformation[9];
		types[0] = Types.INT;
		types[1] = Types.STRING;
		types[2] = Types.STRING;
		types[3] = Types.STRING;
		types[4] = Types.STRING;
		types[5] = Types.INT;
		types[6] = Types.STRING;
		types[7] = Types.DOUBLE;
		types[8] = Types.STRING;
		boolean addPID = true;
		DataStream<Tuple> tuples = lines
				.map(new CSVParser(9, types, "|", addPID, -1))
				.name("parsing");

		TypeInformation[] returnTypes;
		if(addPID){
			returnTypes = new TypeInformation[types.length + 1];
			returnTypes[0] = Types.INT;
			for(int i = 0; i < types.length; i++){
				returnTypes[i + 1] = types[i];
			}
		}else{
			returnTypes = types;
		}
		DataStream<Tuple2<Tuple, Long>> enrichedTuples = tuples
				.map(value -> new Tuple2<>(value, System.currentTimeMillis()))
				.returns(Types.TUPLE(Types.TUPLE(returnTypes), Types.LONG)) //needed, bc in the lambda function type info gts lost
				.name("Enrich with timestamp");

		int[] keys = new int[2];
		keys[0] = 6;
		keys[1] = 8;

		DataStream<Tuple> generalizedTransactions = enrichedTuples
			.process(new Generalizer(10,30, 60000, keys, 0, returnTypes))
			.name("Generalizer");

		/*alerts
			.addSink(new AlertSink())
			.name("send-alerts");*/


		env.execute("Transactions Generalization");
	}
}