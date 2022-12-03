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

import datasources.NYCTaxiRideSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import throughputUtils.ParallelThroughputLogger;

/**
 * Skeleton code for the datastream walkthrough
 */
public class TransactionGeneralizationJob {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Initialize NYCTaxi DataSource
		DataStream<Tuple> messageStream = env
				.addSource(new NYCTaxiRideSource(60000, 1000)).setParallelism(1); //runtime of -1 means throughput/sec

		TypeInformation[] types = new TypeInformation[11];
		types[0] = Types.LONG;
		types[1] = Types.LONG;
		types[2] = Types.LONG;
		types[3] = Types.BOOLEAN;
		types[4] = Types.LONG;
		types[5] = Types.LONG;
		types[6] = Types.DOUBLE;
		types[7] = Types.DOUBLE;
		types[8] = Types.DOUBLE;
		types[9] = Types.DOUBLE;
		types[10] = Types.SHORT;
		/*DataStream<Tuple> tuples = lines
				.map(new CSVParser(9, types, "|", false, 1000))
				.name("parsing");*/

		DataStream<Tuple2<Tuple, Long>> enrichedTuples = messageStream
				.map(value -> new Tuple2<>(value, System.currentTimeMillis()))
				.returns(Types.TUPLE(Types.TUPLE(types), Types.LONG)) //needed, bc in the lambda function type info gts lost
				.name("Enrich with timestamp");

		DataStream<Tuple2<Tuple, Long>> streamWithLogger = enrichedTuples
				.flatMap(new ParallelThroughputLogger<>(1000, "ThroughputLogger"));

		int[] keys = new int[3];
		keys[0] = 0;
		keys[1] = 1;
		keys[2] = 5;
		DataStream<Tuple> generalizedTransactions = streamWithLogger
			.process(new Generalizer(10,30, 60000, keys, 0, types))
			.name("Generalizer");

		/*alerts
			.addSink(new AlertSink())
			.name("send-alerts");*/


		env.execute("Transactions Generalization");
	}
}