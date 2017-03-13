package inf.ed.grape.app.pagerank;
//package ed.inf.grape.app;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import ed.inf.grape.graph.Edge;
//import ed.inf.grape.graph.Partition;
//import ed.inf.grape.interfaces.LocalComputeTask;
//import ed.inf.grape.interfaces.Message;
//
//public class SyncPageRankTask extends LocalComputeTask {
//
//	static Logger log = LogManager.getLogger(SyncPageRankTask.class);
//
//	private static final double THREASHOLD = 0.01;
//	private static final double INIT_RANK = 0.5;
//	// private static final double DEAD_NODE_ITERATION_LIMIT = 5;
//	private static final double ITERATION_LIMIT = 10;
//
//	@Override
//	public void compute(Partition partition) {
//
//		log.debug("local compute.");
//
//		for (int vertex : partition.vertexSet()) {
//
//			if (partition.isInnerVertex(vertex)) {
//
//				((PageRankResult) this.getResult()).ranks
//						.put(vertex, INIT_RANK);
//
//				int numOutgoingEdges = partition.outDegreeOf(vertex);
//				for (Edge edge : partition.outgoingEdgesOf(vertex)) {
//					Message<Double> message = new Message<Double>(
//							this.getPartitionID(),
//							partition.getEdgeTarget(edge), INIT_RANK
//									/ numOutgoingEdges);
//					this.getMessages().add(message);
//				}
//			}
//
//		}
//	}
//
//	@Override
//	public void incrementalCompute(Partition partition,
//			List<Message> incomingMessages) {
//
//		log.debug("local incremental compute.");
//		log.debug("incommingMessages.size = " + incomingMessages.size());
//
//		Map<Integer, Double> aggregateByVertex = new HashMap<Integer, Double>();
//		for (Message<Double> m : incomingMessages) {
//
//			double aggreated = 0.0;
//			if (aggregateByVertex.containsKey(m.getDestinationVertexID())) {
//				aggreated = aggregateByVertex.get(m.getDestinationVertexID());
//			}
//			aggregateByVertex.put(m.getDestinationVertexID(),
//					aggreated + m.getContent());
//		}
//
//		log.debug("aggregateIncommingMessages.size = "
//				+ aggregateByVertex.size());
//
//		for (Entry<Integer, Double> entry : aggregateByVertex.entrySet()) {
//
//			int source = entry.getKey();
//
//			int numOutgoingEdges = partition.outDegreeOf(source);
//
//			double updatedRank = 0.0;
//			updatedRank = (0.15 / numOutgoingEdges + 0.85 * entry.getValue());
//			// double oldRank = ((PageRankResult) this.getResult()).ranks
//			// .get(source);
//
//			// if (numOutgoingEdges == 0) {
//			// // dead node
//			// if (this.getSuperstep() < DEAD_NODE_ITERATION_LIMIT) {
//			// updatedRank = (0.15 / numOutgoingEdges + 0.85 * entry
//			// .getValue());
//			// } else {
//			// updatedRank = oldRank;
//			// }
//			// } else {
//			// updatedRank = (0.15 / numOutgoingEdges + 0.85 * entry
//			// .getValue());
//			// }
//
//			// if (Math.abs(updatedRank - oldRank) < minDiff) {
//			// minDiff = Math.abs(updatedRank - oldRank);
//			// }
//			//
//			// if (Math.abs(updatedRank - oldRank) > maxDiff) {
//			// maxDiff = Math.abs(updatedRank - oldRank);
//			// }
//
//			// log.info("source=" + source + ", aggrevalue = " +
//			// entry.getValue()
//			// + ", diff=" + Math.abs(updatedRank - oldRank));
//
//			if (this.getSuperstep() < ITERATION_LIMIT
//			// && Math.abs(updatedRank - oldRank) > THREASHOLD
//			) {
//
//				((PageRankResult) this.getResult()).ranks.put(source,
//						updatedRank);
//
//				for (Edge edge : partition.outgoingEdgesOf(source)) {
//
//					Message<Double> message = new Message<Double>(
//							this.getPartitionID(),
//							partition.getEdgeTarget(edge), updatedRank
//									/ numOutgoingEdges);
//					this.getMessages().add(message);
//				}
//			}
//		}
//
//		// log.debug("superstep = " + this.getSuperstep() + ", minDiff=" +
//		// minDiff
//		// + ", maxDiff=" + maxDiff);
//	}
//}