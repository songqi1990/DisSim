package inf.ed.grape.core;

import inf.ed.grape.util.IO;
import inf.ed.grape.util.KV;

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Partitioner, divide a whole graph into several partitions with predefined
 * strategy.
 * 
 * TODO: make it interface, implements by different strategy, invoke by
 * reflection.
 * 
 * @author yecol
 *
 */
public class Partitioner {

	/** Partition graph with a simple strategy, greedy scan and divide. */
	public static int STRATEGY_SIMPLE = 0;

	/** Partition graph with LibMetis. */
	public static int STRATEGY_METIS = 1;

	/** Partition graph with hash vertex. */
	public static int STRATEGY_HASH = 2;

	/** Partition strategy */
	private int strategy;

	/** Partition id */
	private static int currentPartitionId = 0;

	static Logger log = LogManager.getLogger(Partitioner.class);

	public Partitioner(int strategy) {
		this.strategy = strategy;
	}

	public int getNumOfPartitions() {
		return KV.PARTITION_COUNT;
	}

	public boolean hasNextPartitionID() {

		return currentPartitionId < KV.PARTITION_COUNT;
	}

	public int getNextPartitionID() {

		/** Assume have run program lib/metis/gpartition */
		assert this.strategy == STRATEGY_METIS;

		int ret = -1;

		if (currentPartitionId < KV.PARTITION_COUNT) {
			ret = currentPartitionId++;
		}

		return ret;
	}

	public Map<Integer, Integer> getVirtualVertex2PartitionMap() {

		assert this.strategy == STRATEGY_METIS;

		try {
			return IO.loadInt2IntMapFromFile(KV.GRAPH_FILE_PATH + ".vvp");
		} catch (IOException e) {
			log.error("load virtual vertex to partition map failed.");
			e.printStackTrace();
		}
		return null;
	}

	public static int hashVertexToPartition(int vertexID) {

		assert KV.PARTITION_STRATEGY == STRATEGY_HASH;
		// TODO:hash and map vertex to partition;
		return -1;
	}
}
