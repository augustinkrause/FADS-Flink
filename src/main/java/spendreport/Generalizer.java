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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.*;

/**
 *
 */
public class Generalizer extends ProcessFunction<Tuple2<Tuple, Long>, Tuple> implements Serializable, ResultTypeQueryable {

	private int k;
	private long bufferConstraint;
	private long reuseConstraint;

	private int[] keys;
	private int pidKey;

	private Tuple2<Double, Double>[] globalBounds;

	private PriorityQueue<Tuple2<Tuple, Long>> bufferedTuples;
	private PriorityQueue<Tuple2<Cluster, Long>> bufferedClusters;

	private Stack<Tuple> toBeReleasedTuples = new Stack<>(); //during processing of the stream tuples will be added to this, at the end of processing the stack will be emptied

	private TypeInformation[] types;

	private Collector<Tuple> collector; //TODO: For now only a test

	@Override
	public void close(){

		//release remaining tuples
		while(!this.bufferedTuples.isEmpty()){
			this.generalizeTuple(this.bufferedTuples.poll());
		}

		//release all tuples that have been generalized in the last round
		while(!this.toBeReleasedTuples.isEmpty()){
			System.out.println(this.toBeReleasedTuples.peek());
			this.collector.collect(this.toBeReleasedTuples.pop());
			//this.toBeReleasedTuples.remove(this.toBeReleasedTuples.size() - 1);
		}

	}

	public Generalizer(int k, long bufferConstraint, long reuseConstraint, int[] keys, int pidKey, TypeInformation[] types){
		this.k = k;
		this.bufferConstraint = bufferConstraint; //how long can a tuple stay at max in the buffer, before being released
		this.reuseConstraint = reuseConstraint; //how long can a cluster be reused
		this.globalBounds = new Tuple2[keys.length];

		this.keys = keys; //these are the indices of the fields to be considered QIDs
		this.pidKey = pidKey;

		this.bufferedTuples = new PriorityQueue<>(new EnrichedTupleComparator()); //so that our tuples are always sorted by our used notion of time
		this.bufferedClusters = new PriorityQueue<>(new EnrichedTupleComparator());

		this.types = types;
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
		this.collector = collector;

		//buffer incoming tuple
		bufferedTuples.add(element);

		//update global bounds
		for(int i = 0; i < this.keys.length; i++){
			if (this.globalBounds[i] == null) {
				this.globalBounds[i] = new Tuple2<>(((Number) element.f0.getField(this.keys[i])).doubleValue(), ((Number) element.f0.getField(this.keys[i])).doubleValue());
			}else{
				if(this.globalBounds[i].f0 > ((Number) element.f0.getField(this.keys[i])).doubleValue()) this.globalBounds[i].f0 = ((Number) element.f0.getField(this.keys[i])).doubleValue();
				if(this.globalBounds[i].f1 < ((Number) element.f0.getField(this.keys[i])).doubleValue()) this.globalBounds[i].f1 = ((Number) element.f0.getField(this.keys[i])).doubleValue();
			}
		}

		//remove expired cluster(s)
		while(!this.bufferedClusters.isEmpty() && this.bufferedClusters.peek().f1 + this.reuseConstraint < System.currentTimeMillis()){
			this.bufferedClusters.poll();
		}

		//generalize oldest tuple if buffer constraint is exceeded
		if(this.bufferedTuples.size() > this.bufferConstraint){
			this.generalizeTuple(this.bufferedTuples.poll());
		}

		//release all tuples that have been generalized in the last round
		while(!this.toBeReleasedTuples.isEmpty()){
			System.out.println(this.toBeReleasedTuples.peek());
			collector.collect(this.toBeReleasedTuples.pop());
			//this.toBeReleasedTuples.remove(this.toBeReleasedTuples.size() - 1);
		}

		System.out.println(this.bufferedTuples.size());
	}

	//either releases a tuple along with the other tuples from its k-anonymized cluster, or the tuple gets suppressed and then released
	public void generalizeTuple(Tuple2<Tuple, Long> t){
		if(this.bufferedTuples.size() < this.k - 1){
			//in this case we can't form a new k-anonymized cluster around the tuple
			//so either we reuse an old cluster or suppress the tuple
			Cluster minC = this.findFittingOldCluster(t.f0);
			if(minC != null){
				//release tuple with an old cluster, that has minimal information loss
				Tuple newT = minC.generalize(t.f0);
				this.toBeReleasedTuples.add(newT);
			}else{
				//suppress tuple
				Tuple newT = this.suppress(t.f0);
				this.toBeReleasedTuples.add(newT);
			}
		}else{
			//find k-1 NNs with unique PIDs of t
			Tuple2<Tuple, Long>[] neighbours = this.knn(t);
			Cluster knnC = null;
			if(neighbours != null){
				knnC = new Cluster(neighbours, this.keys);
			}

			//find old cluster with minimal information loss
			Cluster minC = this.findFittingOldCluster(t.f0);

			if(knnC != null){
				if(minC != null && minC.infoLoss(this.globalBounds) < knnC.infoLoss(this.globalBounds)){
					//release tuple with old cluster, that has minimal information loss
					Tuple newT = minC.generalize(t.f0);
					this.toBeReleasedTuples.add(newT);
				}else{
					//release tuple with its k-1 NNs
					for(int i = 0; i < neighbours.length; i++){
						Tuple newT = knnC.generalize(neighbours[i].f0);
						this.bufferedTuples.remove(neighbours[i]); //remove the current tuple from the buffered tuples
						this.toBeReleasedTuples.add(newT);
					}
					Tuple newT = knnC.generalize(t.f0);
					this.toBeReleasedTuples.add(newT);
					this.bufferedClusters.add(new Tuple2<>(knnC, System.currentTimeMillis()));
				}
			}else{
				if(minC != null){
					//release tuple with an old cluster, that has minimal information loss
					Tuple newT = minC.generalize(t.f0);
					this.toBeReleasedTuples.add(newT);
				}else{
					//suppress tuple
					Tuple newT = this.suppress(t.f0);
					this.toBeReleasedTuples.add(newT);
				}
			}
		}
	}


	//finds the k-1 NNs with unique PIDs of t
	public Tuple2<Tuple, Long>[] knn(Tuple2<Tuple, Long> t){
		Tuple2<Tuple, Long>[] toBeSorted = new Tuple2[this.bufferedTuples.size()];

		//retrieve and sort all the tuples by their distance to t
		Iterator<Tuple2<Tuple, Long>> it = this.bufferedTuples.iterator();
		int index = 0;
		while(it.hasNext()){
			toBeSorted[index] = it.next();
			index++;
		}

		Arrays.sort(toBeSorted, new DistanceComparator(t.f0, this.globalBounds)); //sorts the tuples other than t by their "distance" to t (Distance as defined in the FADS-paper)

		//only take the k-1 with unique PID of the sorted ones
		Tuple2<Tuple, Long>[] neighbours = new Tuple2[this.k - 1];
		int count = 0;
		HashMap<Double, Boolean> uniqueKeys = new HashMap<>();
		for(int i = 0; i < toBeSorted.length && count < this.k - 1; i++){
			//only add the next tuple if it has a unique pid
			if(!uniqueKeys.containsKey(toBeSorted[i].f0.getField(this.pidKey))){
				neighbours[count] = toBeSorted[i];
				uniqueKeys.put(toBeSorted[i].f0.getField(this.pidKey), true);
				count++;
			}
		}

		if(count == this.k - 1){
			//we have found enough neighbours and can return them
			return neighbours;
		}else{
			//otherwise the calling function knows we haven't found enough neighbours by returning null
			return null;
		}

	}

	//finds amongst all buffered clusters the cluster that fits t and has minimal information loss
	public Cluster findFittingOldCluster(Tuple t){
		Iterator<Tuple2<Cluster, Long>> it = this.bufferedClusters.iterator();
		Cluster minC = null;
		while(it.hasNext()){
			Cluster c = it.next().f0;
			if(minC == null){
				if(c.fits(t)) minC = c;
			}else{
				if(c.fits(t) && c.infoLoss(this.globalBounds) < minC.infoLoss(this.globalBounds)) minC = c;
			}
		}

		return minC;
	}

	//returns a tuple, the entries of which correspond to the global bounds encountered during the whole processing duration
	public Tuple suppress(Tuple t){
		Tuple newT = Tuple.newInstance(t.getArity());
		for(int i = 0; i < this.keys.length; i++){
			newT.setField(this.globalBounds[i], this.keys[i]);
		}

		for(int i = 0; i < t.getArity(); i++){
			if(newT.getField(i) == null) newT.setField(t.getField(i), i);
		}

		return newT;
	}

	@Override
	public TypeInformation getProducedType() {
		TypeInformation[] outputTypes = new TypeInformation[this.types.length];
		for(int i = 0; i < this.keys.length; i++){
			outputTypes[this.keys[i]] = Types.TUPLE(Types.DOUBLE, Types.DOUBLE);
		}
		for(int i = 0; i < this.types.length; i++){
			if(outputTypes[i] == null) outputTypes[i] = this.types[i];
		}
		return Types.TUPLE(outputTypes);
	}

	//readObject, writeObject, readObjectNoData are functions that are called during serialization of instances of this class
	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		out.writeInt(this.k);
		out.writeLong(this.bufferConstraint);
		out.writeLong(this.reuseConstraint);

		//writing an array requires first to write the length
		out.writeInt(this.keys.length);
		for(int i = 0; i < this.keys.length; i++){
			out.writeInt(this.keys[i]);
		}
		out.writeInt(this.pidKey);

		out.writeInt(this.globalBounds.length);
		for(int i = 0; i < this.globalBounds.length; i++){
			out.writeObject(this.globalBounds[i]);
		}

		out.writeObject(this.bufferedTuples);
		out.writeObject(this.bufferedClusters);

		out.writeObject(this.toBeReleasedTuples);

		out.writeInt(this.types.length);
		for(int i = 0; i < this.types.length; i++){
			out.writeObject(this.types[i]);
		}
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.k = in.readInt();
		this.bufferConstraint = in.readLong();
		this.reuseConstraint  = in.readLong();

		//writing an array requires first to write the length
		int len = in.readInt();
		this.keys = new int[len];
		for(int i = 0; i < len; i++){
			this.keys[i] = in.readInt();
		}
		this.pidKey = in.readInt();

		len = in.readInt();
		this.globalBounds = new Tuple2[len];
		for(int i = 0; i < len; i++){
			this.globalBounds[i] = (Tuple2<Double, Double>) in.readObject();
		}

		this.bufferedTuples = (PriorityQueue<Tuple2<Tuple, Long>>) in.readObject();
		this.bufferedClusters = (PriorityQueue<Tuple2<Cluster, Long>>) in.readObject();

		this.toBeReleasedTuples = (Stack<Tuple>) in.readObject();

		len = in.readInt();
		this.types = new TypeInformation[len];
		for(int i = 0; i < len; i++){
			this.types[i] = (TypeInformation) in.readObject();
		}
	}

	private void readObjectNoData() throws ObjectStreamException {
		throw new NotSerializableException("Serialization error in class " + this.getClass().getName());
	}

	static class EnrichedTupleComparator implements Comparator<Tuple2<?, Long>>, Serializable {

		public int compare(Tuple2<?, Long> o1, Tuple2<?, Long> o2) {
			return o1.f1 > o2.f1 ? 1 : -1;
		}
	}

	static class DistanceComparator implements Comparator<Tuple2<Tuple, Long>>{

		Tuple pivot;
		Tuple2<Double, Double>[] globalBounds;

		public DistanceComparator(Tuple pivot, Tuple2<Double, Double>[] globalBounds){
			this.pivot = pivot;
			this.globalBounds = globalBounds;
		}

		//the distance between tuples, as defined in the paper, is the average distance over all to-be-anonymized attributes of the tuple
		private double averageDistance(Tuple t){
			double sum = 0;
			for(int i = 0; i < this.globalBounds.length; i++){
				sum += (Math.abs( ((Number)  t.getField(i)).doubleValue() - ((Number)  pivot.getField(i)).doubleValue()) / globalBounds[i].f1 - globalBounds[i].f0);
			}

			return sum/this.globalBounds.length;
		}

		public int compare(Tuple2<Tuple, Long> o1, Tuple2<Tuple, Long> o2) {
			return (this.averageDistance(o1.f0) >= this.averageDistance(o2.f0)) ? 1 : -1;
		}
	}

}
