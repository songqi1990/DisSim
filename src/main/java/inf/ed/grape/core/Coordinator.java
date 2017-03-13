package inf.ed.grape.core;

import inf.ed.grape.communicate.Client2Coordinator;
import inf.ed.grape.communicate.Worker2Coordinator;
import inf.ed.grape.communicate.WorkerProxy;
import inf.ed.grape.interfaces.Query;
import inf.ed.grape.interfaces.Result;
import inf.ed.grape.util.KV;

import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The Class Coordinator.
 * 
 * @author yecol
 */
public class Coordinator extends UnicastRemoteObject implements Worker2Coordinator,
		Client2Coordinator {

	private static final long serialVersionUID = 7264167926318903124L;

	/** The total number of worker threads. */
	private static AtomicInteger totalWorkerThreads = new AtomicInteger(0);

	/** The workerID to WorkerProxy map. */
	private Map<String, WorkerProxy> workerProxyMap = new ConcurrentHashMap<String, WorkerProxy>();

	/** The workerID to Worker map. **/
	private Map<String, Worker> workerMap = new HashMap<String, Worker>();

	/** The partitionID to workerID map. **/
	private Map<Integer, String> partitionWorkerMap;

	/** The virtual vertexID to partitionID map. */
	private Map<Integer, Integer> virtualVertexPartitionMap;

	/** Set of Workers maintained for acknowledgement. */
	private Set<String> workerAcknowledgementSet = new HashSet<String>();

	/** Set of workers who will be active in the next super step. */
	private Set<String> activeWorkerSet = new HashSet<String>();

	/** Set of partial results. partitionID to Results **/
	private Map<Integer, Result> resultMap = new HashMap<Integer, Result>();

	/** The start time. */
	long startTime;

	long superstep = 0;

	/** Partition manager. */
	private Partitioner partitioner;

	static Logger log = LogManager.getLogger(Coordinator.class);

	/**
	 * Instantiates a new coordinator.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 * @throws PropertyNotFoundException
	 *             the property not found exception
	 */
	public Coordinator() throws RemoteException {
		super();

	}

	/**
	 * Gets the active worker set.
	 * 
	 * @return the active worker set
	 */
	public Set<String> getActiveWorkerSet() {
		return activeWorkerSet;
	}

	/**
	 * Sets the active worker set.
	 * 
	 * @param activeWorkerSet
	 *            the new active worker set
	 */
	public void setActiveWorkerSet(Set<String> activeWorkerSet) {
		this.activeWorkerSet = activeWorkerSet;
	}

	/**
	 * Registers the worker computation nodes with the master.
	 * 
	 * @param worker
	 *            Represents the {@link WorkerSyncImpl.WorkerImpl Worker}
	 * @param workerID
	 *            the worker id
	 * @param numWorkerThreads
	 *            Represents the number of worker threads available in the
	 *            worker computation node
	 * @return worker2 master
	 * @throws RemoteException
	 *             the remote exception
	 */
	@Override
	public Worker2Coordinator register(Worker worker, String workerID, int numWorkerThreads)
			throws RemoteException {

		log.debug("Register");
		totalWorkerThreads.getAndAdd(numWorkerThreads);
		WorkerProxy workerProxy = new WorkerProxy(worker, workerID, numWorkerThreads, this);
		workerProxyMap.put(workerID, workerProxy);
		workerMap.put(workerID, worker);
		return (Worker2Coordinator) UnicastRemoteObject.exportObject(workerProxy, 0);
	}

	/**
	 * Gets the worker proxy map info.
	 * 
	 * @return Returns the worker proxy map info
	 */
	public Map<String, WorkerProxy> getWorkerProxyMap() {
		return workerProxyMap;
	}

	/**
	 * Send worker partition info.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void sendWorkerPartitionInfo() throws RemoteException {
		log.debug("sendWorkerPartitionInfo");
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.setWorkerPartitionInfo(virtualVertexPartitionMap, partitionWorkerMap,
					workerMap);
		}
	}

	public void sendQuery(Query query) throws RemoteException {
		log.debug("distribute Query to workers.");
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.setQuery(query);
		}
	}

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws Exception
	 *             the exception
	 */
	public static void main(String[] args) throws Exception {
		System.setSecurityManager(new RMISecurityManager());
		Coordinator coordinator;
		try {
			coordinator = new Coordinator();
			Registry registry = LocateRegistry.createRegistry(KV.RMI_PORT);
			registry.rebind(KV.COORDINATOR_SERVICE_NAME, coordinator);
			log.info("instance is bound to " + KV.RMI_PORT + " and ready.");
		} catch (RemoteException e) {
			Coordinator.log.error(e);
			e.printStackTrace();
		}
	}

	/**
	 * Halts all the workers and prints the final solution.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void halt() throws RemoteException {
		log.debug("Worker Proxy Map " + workerProxyMap);

		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.halt();
		}

		// healthManager.exit();
		long endTime = System.currentTimeMillis();
		log.info("Time taken: " + (endTime - startTime) + " ms");
		// Restore the system back to its initial state
		restoreInitialState();
	}

	/**
	 * Restore initial state of the system.
	 */
	private void restoreInitialState() {
		this.activeWorkerSet.clear();
		this.workerAcknowledgementSet.clear();
		this.partitionWorkerMap.clear();
		this.superstep = 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */

	/**
	 * Removes the worker.
	 * 
	 * @param workerID
	 *            the worker id
	 */
	public void removeWorker(String workerID) {
		workerProxyMap.remove(workerID);
		workerMap.remove(workerID);
	}

	/**
	 * Gets the partition worker map.
	 * 
	 * @return the partition worker map
	 */
	public Map<Integer, String> getPartitionWorkerMap() {
		return partitionWorkerMap;
	}

	/**
	 * Sets the partition worker map.
	 * 
	 * @param partitionWorkerMap
	 *            the partition worker map
	 */
	public void setPartitionWorkerMap(Map<Integer, String> partitionWorkerMap) {
		this.partitionWorkerMap = partitionWorkerMap;
	}

	/**
	 * Defines a deployment convenience to stop each registered.
	 * 
	 * @throws RemoteException
	 *             the remote exception {@link system.Worker Worker} and then
	 *             stops itself.
	 */

	@Override
	public void shutdown() throws RemoteException {
		// if (healthManager != null)
		// healthManager.exit();
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			try {
				workerProxy.shutdown();
			} catch (Exception e) {
				continue;
			}
		}
		java.util.Date date = new java.util.Date();
		log.info("goes down now at :" + new Timestamp(date.getTime()));
		System.exit(0);
	}

	public void loadGraph(String graphFilename) throws RemoteException {

		log.info("load Graph = " + graphFilename + ", strategy=" + KV.PARTITION_STRATEGY);

		startTime = System.currentTimeMillis();

		if (KV.PARTITION_STRATEGY == Partitioner.STRATEGY_METIS) {
			assignDistributedPartitions();
			sendWorkerPartitionInfo();
		}

		if (KV.PARTITION_STRATEGY == Partitioner.STRATEGY_HASH) {
			// TODO: hash vertexID to total threads.
		}
	}

	@Override
	public void putTask(Query query) throws RemoteException {

		log.info("receive task with query = " + query);

		/** initiate local compute tasks. */
		sendQuery(query);

		/** begin to compute. */
		nextLocalCompute();
	}

	/**
	 * Assign distributed partitions to workers, assuming graph has been
	 * partitioned and distributed.
	 */
	public void assignDistributedPartitions() {

		log.info("begin assign distributed partitions.");

		partitioner = new Partitioner(KV.PARTITION_STRATEGY);
		partitionWorkerMap = new HashMap<Integer, String>();

		int totalPartitions = partitioner.getNumOfPartitions(), partitionID;

		// Assign partitions to workers in the ratio of the number of worker
		// threads that each worker has.
		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {

			WorkerProxy workerProxy = entry.getValue();

			// Compute the number of partitions to assign
			int numThreads = workerProxy.getNumThreads();
			double ratio = ((double) (numThreads)) / totalWorkerThreads.get();
			int numPartitionsToAssign = (int) (ratio * totalPartitions);

			log.info("Worker " + workerProxy.getWorkerID() + " with " + numThreads
					+ " threads got " + numPartitionsToAssign + " partition.");

			List<Integer> workerPartitionIDs = new ArrayList<Integer>();
			for (int i = 0; i < numPartitionsToAssign; i++) {
				if (partitioner.hasNextPartitionID()) {
					partitionID = partitioner.getNextPartitionID();

					activeWorkerSet.add(entry.getKey());
					log.info("Adding partition " + partitionID + " to worker "
							+ workerProxy.getWorkerID());
					workerPartitionIDs.add(partitionID);
					partitionWorkerMap.put(partitionID, workerProxy.getWorkerID());
				}
			}
			workerProxy.addPartitionIDList(workerPartitionIDs);
		}

		if (partitioner.hasNextPartitionID()) {
			// Add the remaining partitions (if any) in a round-robin fashion.
			Iterator<Map.Entry<String, WorkerProxy>> workerMapIter = workerProxyMap.entrySet()
					.iterator();

			partitionID = partitioner.getNextPartitionID();

			while (partitionID != -1) {
				// If the remaining partitions is greater than the number of the
				// workers, start iterating from the beginning again.
				if (!workerMapIter.hasNext()) {
					workerMapIter = workerProxyMap.entrySet().iterator();
				}

				WorkerProxy workerProxy = workerMapIter.next().getValue();

				activeWorkerSet.add(workerProxy.getWorkerID());
				log.info("Adding partition " + partitionID + " to worker "
						+ workerProxy.getWorkerID());
				partitionWorkerMap.put(partitionID, workerProxy.getWorkerID());
				workerProxy.addPartitionID(partitionID);

				partitionID = partitioner.getNextPartitionID();
			}
		}

		// get virtual vertex to partition map
		this.virtualVertexPartitionMap = partitioner.getVirtualVertex2PartitionMap();
	}

	/**
	 * Start super step.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public void nextLocalCompute() throws RemoteException {
		log.info("next local compute. superstep = " + superstep);

		this.workerAcknowledgementSet.clear();
		this.workerAcknowledgementSet.addAll(this.activeWorkerSet);

		for (String workerID : this.activeWorkerSet) {
			this.workerProxyMap.get(workerID).nextLocalCompute(superstep);
		}
		this.activeWorkerSet.clear();
	}

	@Override
	public synchronized void localComputeCompleted(String workerID, Set<String> activeWorkerIDs)
			throws RemoteException {

		log.info("receive acknowledgement from worker " + workerID + "\n saying activeWorkers: "
				+ activeWorkerIDs.toString());

		// in synchronised model, coordinator receive activeWorkers and
		// prepare for next round of local computations.
		this.activeWorkerSet.addAll(activeWorkerIDs);
		this.workerAcknowledgementSet.remove(workerID);

		if (this.workerAcknowledgementSet.size() == 0) {
			superstep++;
			if (activeWorkerSet.size() != 0)
				nextLocalCompute();
			else {
				finishLocalCompute();
			}
		}
	}

	public void finishLocalCompute() throws RemoteException {

		/**
		 * TODO: send flag to coordinator. the coordinator determined the next
		 * step, whether save results or assemble results.
		 * 
		 * TODO: if assemble enabled, then assemble results from workers. and
		 * write results to file.
		 */

		this.resultMap.clear();

		this.workerAcknowledgementSet.clear();
		this.workerAcknowledgementSet.addAll(this.workerProxyMap.keySet());

		for (Map.Entry<String, WorkerProxy> entry : workerProxyMap.entrySet()) {
			WorkerProxy workerProxy = entry.getValue();
			workerProxy.processPartialResult();
		}

		log.info("finish local compute. with round = " + superstep);

	}

	public synchronized void receivePartialResults(String workerID,
			Map<Integer, Result> mapPartitionID2Result) {

		log.debug("current ack set = " + this.workerAcknowledgementSet.toString());
		log.debug("receive partitial results = " + workerID);

		for (Entry<Integer, Result> entry : mapPartitionID2Result.entrySet()) {
			resultMap.put(entry.getKey(), entry.getValue());
		}

		this.workerAcknowledgementSet.remove(workerID);

		if (this.workerAcknowledgementSet.size() == 0) {

			/** receive all the partial results, assemble them. */
			log.info("assemble a final result");

			try {

				Result finalResult = (Result) Class.forName(KV.CLASS_RESULT).newInstance();
				finalResult.assemblePartialResults(resultMap.values());
				finalResult.writeToFile(KV.OUTPUT_DIR + "finalResult.rlt");

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void preProcess() throws RemoteException {
		this.loadGraph(KV.GRAPH_FILE_PATH);
	}

	@Override
	public void postProcess() throws RemoteException {
		// TODO Auto-generated method stub

	}

	@Override
	public void sendPartialResult(String workerID, Map<Integer, Result> mapPartitionID2Result)
			throws RemoteException {
		this.receivePartialResults(workerID, mapPartitionID2Result);
	}

	@Override
	public synchronized void vote2halt(String workerID) throws RemoteException {
		log.info("receive vote2halt signal from worker " + workerID);
		this.workerAcknowledgementSet.remove(workerID);

		if (this.workerAcknowledgementSet.size() == 0) {
			if (this.confirmHalt() == false)
				voteAgain();
			else {
				finishLocalCompute();
			}
		}
	}

	private boolean confirmHalt() throws RemoteException {
		log.info("confirming.");
		for (String workerID : this.activeWorkerSet) {
			if (!this.workerProxyMap.get(workerID).isHalt()) {
				log.info(workerID + " is working.");
				return false;
			}
		}
		return true;
	}

	/**
	 * Vote to halt again.
	 */
	private void voteAgain() throws RemoteException {
		log.info("Vote again.");

		this.workerAcknowledgementSet.clear();
		this.workerAcknowledgementSet.addAll(this.activeWorkerSet);

		for (String workerID : this.activeWorkerSet) {
			this.workerProxyMap.get(workerID).voteAgain();
		}
	}
}
