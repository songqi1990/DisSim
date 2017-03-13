package inf.ed.grape.core;

import inf.ed.grape.communicate.Worker2Coordinator;
import inf.ed.grape.communicate.Worker2WorkerProxy;
import inf.ed.grape.graph.Partition;
import inf.ed.grape.interfaces.LocalComputeTask;
import inf.ed.grape.interfaces.Message;
import inf.ed.grape.interfaces.Query;
import inf.ed.grape.interfaces.Result;
import inf.ed.grape.util.Dev;
import inf.ed.grape.util.KV;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Represents the computation node.
 * 
 * @author Yecol
 */

public class WorkerSyncImpl extends UnicastRemoteObject implements Worker {

	private static final long serialVersionUID = 1L;

	/** The number of threads. */
	private int numThreads;

	/** The total partitions assigned. */
	private int totalPartitionsAssigned;

	/** The queue of partitions in the current super step. */
	private BlockingQueue<LocalComputeTask> currentLocalComputeTaskQueue;

	/** The queue of partitions in the next super step. */
	private BlockingQueue<LocalComputeTask> nextLocalComputeTasksQueue;

	/** hosting partitions */
	private Map<Integer, Partition> partitions;

	/** Host name of the node with time stamp information. */
	private String workerID;

	/** Coordinator Proxy object to interact with Master. */
	private Worker2Coordinator coordinatorProxy;

	/** VertexID 2 PartitionID Map */
	private Map<Integer, Integer> mapVertexIdToPartitionId;

	/** PartitionID to WorkerID Map. */
	private Map<Integer, String> mapPartitionIdToWorkerId;

	/** Worker2WorkerProxy Object. */
	private Worker2WorkerProxy worker2WorkerProxy;

	/** Worker to Outgoing Messages Map. */
	private ConcurrentHashMap<String, List<Message<?>>> outgoingMessages;

	/** PartitionID to Outgoing Results Map. */
	private ConcurrentHashMap<Integer, Result> partialResults;

	/** partitionId to Previous Incoming messages - Used in current Super Step. */
	private ConcurrentHashMap<Integer, List<Message<?>>> previousIncomingMessages;

	/** partitionId to Current Incoming messages - used in next Super Step. */
	private ConcurrentHashMap<Integer, List<Message<?>>> currentIncomingMessages;

	/**
	 * boolean variable indicating whether the partitions can be worked upon by
	 * the workers in each super step.
	 **/
	private boolean flagLocalCompute = false;
	/**
	 * boolean variable to determine if a Worker can send messages to other
	 * Workers and to Master. It is set to true when a Worker is sending
	 * messages to other Workers.
	 */
	private boolean stopSendingMessage;

	private boolean flagLastStep = false;

	/** The super step counter. */
	private long superstep = 0;

	static Logger log = LogManager.getLogger(WorkerSyncImpl.class);

	/**
	 * Instantiates a new worker.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	public WorkerSyncImpl() throws RemoteException {
		InetAddress address = null;
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMdd.HHmmss.SSS");
		String timestamp = simpleDateFormat.format(new Date());
		String hostName = new String();
		try {
			address = InetAddress.getLocalHost();
			hostName = address.getHostName();
		} catch (UnknownHostException e) {
			hostName = "UnKnownHost";
			log.error(e);
		}

		this.workerID = "sync_" + hostName + "_" + timestamp;
		this.partitions = new HashMap<Integer, Partition>();
		this.currentLocalComputeTaskQueue = new LinkedBlockingDeque<LocalComputeTask>();
		this.nextLocalComputeTasksQueue = new LinkedBlockingQueue<LocalComputeTask>();
		this.currentIncomingMessages = new ConcurrentHashMap<Integer, List<Message<?>>>();
		this.partialResults = new ConcurrentHashMap<Integer, Result>();
		this.previousIncomingMessages = new ConcurrentHashMap<Integer, List<Message<?>>>();
		this.outgoingMessages = new ConcurrentHashMap<String, List<Message<?>>>();
		this.numThreads = Math.min(Runtime.getRuntime().availableProcessors(),
				KV.MAX_THREAD_LIMITATION);
		this.stopSendingMessage = false;

		for (int i = 0; i < numThreads; i++) {
			log.debug("Starting syncThread " + (i + 1));
			WorkerThread workerThread = new WorkerThread();
			workerThread.setName(this.workerID + "_th" + i);
			workerThread.start();
		}

	}

	/**
	 * Adds the partition to be assigned to the worker.
	 * 
	 * @param partition
	 *            the partition to be assigned
	 * @throws RemoteException
	 *             the remote exception
	 */
	@Override
	public void addPartition(Partition partition) throws RemoteException {
		log.info("receive partition:" + partition.getPartitionInfo());
		log.debug(Dev.currentRuntimeState());
		this.partitions.put(partition.getPartitionID(), partition);
		log.debug(Dev.currentRuntimeState());
	}

	/**
	 * Adds the partition list.
	 * 
	 * @param workerPartitions
	 *            the worker partitions
	 * @throws RemoteException
	 *             the remote exception
	 */
	@Override
	public void addPartitionList(List<Partition> workerPartitions) throws RemoteException {
		log.debug(Dev.currentRuntimeState());
		for (Partition p : workerPartitions) {
			log.info("receive partition:" + p.getPartitionInfo());
			this.partitions.put(p.getPartitionID(), p);
		}
		log.debug(Dev.currentRuntimeState());
	}

	@Override
	public void addPartitionIDList(List<Integer> workerPartitionIDs) throws RemoteException {

		for (int partitionID : workerPartitionIDs) {

			if (!this.partitions.containsKey(partitionID)) {

				String filename = KV.GRAPH_FILE_PATH + ".p" + String.valueOf(partitionID);

				Partition partition = new Partition(partitionID);
				partition.loadPartitionDataFromEVFile(filename);
				this.partitions.put(partitionID, partition);
			}
		}

		log.debug(Dev.currentRuntimeState());
	}

	@Override
	public void addPartitionID(int partitionID) throws RemoteException {

		if (!this.partitions.containsKey(partitionID)) {

			String filename = KV.GRAPH_FILE_PATH + ".p" + String.valueOf(partitionID);

			Partition partition = new Partition(partitionID);
			partition.loadPartitionDataFromEVFile(filename);
			this.partitions.put(partitionID, partition);
		}

		log.debug(Dev.currentRuntimeState());

	}

	/**
	 * Gets the num threads.
	 * 
	 * @return the num threads
	 */
	@Override
	public int getNumThreads() {
		return numThreads;
	}

	/**
	 * Gets the worker id.
	 * 
	 * @return the worker id
	 */
	@Override
	public String getWorkerID() {
		return workerID;
	}

	/**
	 * The Class SyncWorkerThread.
	 */
	private class WorkerThread extends Thread {

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				while (flagLocalCompute || flagLastStep) {
					log.debug(this + "superstep loop start for superstep " + superstep
							+ "laststep = " + flagLastStep);
					try {

						LocalComputeTask localComputeTask = currentLocalComputeTaskQueue.take();

						Partition workingPartition = partitions.get(localComputeTask
								.getPartitionID());

						if (flagLastStep) {

							if (KV.ENABLE_ASSEMBLE == false) {

								System.out.println(KV.OUTPUT_DIR);
								String dir = KV.OUTPUT_DIR.substring(1, KV.OUTPUT_DIR.length() - 1);

								localComputeTask.getPartialResult()
										.writeToFile(
												dir + workerID + localComputeTask.getPartitionID()
														+ ".rlt");
							}

							partialResults.put(localComputeTask.getPartitionID(),
									localComputeTask.getPartialResult());
						}

						else {

							if (superstep == 0) {

								/** begin step. initial compute */
								localComputeTask.compute(workingPartition);
								updateOutgoingMessages(localComputeTask.getMessages());
							}

							else {

								/** not begin step. incremental compute */
								List<Message<?>> messageForWorkingPartition = previousIncomingMessages
										.get(localComputeTask.getPartitionID());

								if (messageForWorkingPartition != null) {

									localComputeTask.incrementalCompute(workingPartition,
											messageForWorkingPartition);

									updateOutgoingMessages(localComputeTask.getMessages());
								}
							}

							localComputeTask.prepareForNextCompute();
						}

						nextLocalComputeTasksQueue.add(localComputeTask);
						checkAndSendMessage();

					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}

	}

	/**
	 * Check and send message. Notice: this is a critical code area, which
	 * should put outside of the thread code.
	 * 
	 * @throws RemoteException
	 */

	private synchronized void checkAndSendMessage() {

		log.debug("synchronized checkAndSendMessage!");
		if ((!stopSendingMessage) && (nextLocalComputeTasksQueue.size() == totalPartitionsAssigned)) {
			log.debug("sendMessage!");

			stopSendingMessage = true;

			if (flagLastStep) {

				flagLastStep = false;

				if (KV.ENABLE_ASSEMBLE) {
					log.debug("assemble = true. " + this + "send partital result");
					try {
						coordinatorProxy.sendPartialResult(workerID, partialResults);
					} catch (RemoteException e) {
						e.printStackTrace();
					}
				}
			}

			else {

				log.debug(" Worker: Superstep " + superstep + " completed.");

				flagLocalCompute = false;

				for (Entry<String, List<Message<?>>> entry : outgoingMessages.entrySet()) {
					try {
						worker2WorkerProxy.sendMessage(entry.getKey(), entry.getValue());
					} catch (RemoteException e) {
						System.out.println("Can't send message to Worker " + entry.getKey()
								+ " which is down");
					}
				}

				// This worker will be active only if it has some messages
				// queued up in the next superstep.
				// activeWorkerSet will have all the workers who will be
				// active
				// in the next superstep.
				Set<String> activeWorkerSet = new HashSet<String>();
				activeWorkerSet.addAll(outgoingMessages.keySet());
				if (currentIncomingMessages.size() > 0) {
					activeWorkerSet.add(workerID);
				}
				// Send a message to the Master saying that this superstep
				// has
				// been completed.
				try {
					coordinatorProxy.localComputeCompleted(workerID, activeWorkerSet);
				} catch (RemoteException e) {
					e.printStackTrace();
				}
			}

		}
	}

	/**
	 * Halts the run for this application and prints the output in a file.
	 * 
	 * @throws RemoteException
	 *             the remote exception
	 */
	@Override
	public void halt() throws RemoteException {
		System.out.println("Worker Machine " + workerID + " halts");
		this.restoreInitialState();
	}

	/**
	 * Restore the worker to the initial state
	 */
	private void restoreInitialState() {
		// this.partitionQueue.clear();
		this.currentIncomingMessages.clear();
		this.outgoingMessages.clear();
		this.mapPartitionIdToWorkerId.clear();
		// this.currentPartitionQueue.clear();
		this.previousIncomingMessages.clear();
		this.stopSendingMessage = false;
		this.flagLocalCompute = false;
		this.totalPartitionsAssigned = 0;
	}

	/**
	 * Updates the outgoing messages for every superstep.
	 * 
	 * @param messagesFromCompute
	 *            Represents the map of destination vertex and its associated
	 *            message to be send
	 */
	private void updateOutgoingMessages(List<Message<?>> messagesFromCompute) {
		log.debug("updateOutgoingMessages.size = " + messagesFromCompute.size());

		String workerID = null;
		int vertexID = -1;
		int partitionID = -1;
		List<Message<?>> workerMessages = null;

		for (Message<?> message : messagesFromCompute) {

			vertexID = message.getDestinationVertexID();

			if (!mapVertexIdToPartitionId.containsKey(vertexID)) {
				/** inner nodes not contained in the map. */
				updateIncomingMessages(message.getSourcePartitionID(), message);
			} else {

				partitionID = mapVertexIdToPartitionId.get(vertexID);
				workerID = mapPartitionIdToWorkerId.get(partitionID);

				if (workerID.equals(this.workerID)) {

					/** send message to self. only multiple threads valid */
					updateIncomingMessages(partitionID, message);
				} else {

					if (outgoingMessages.containsKey(workerID)) {
						outgoingMessages.get(workerID).add(message);
					} else {
						workerMessages = new ArrayList<Message<?>>();
						workerMessages.add(message);
						outgoingMessages.put(workerID, workerMessages);
					}
				}
			}
		}
	}

	/**
	 * Sets the worker partition info.
	 * 
	 * @param totalPartitionsAssigned
	 *            the total partitions assigned
	 * @param mapVertexIdToPartitionId
	 *            the map vertex id to partition id
	 * @param mapPartitionIdToWorkerId
	 *            the map partition id id to worker id
	 * @param mapWorkerIdToWorker
	 *            the map worker id to worker
	 * @throws RemoteException
	 */
	@Override
	public void setWorkerPartitionInfo(int totalPartitionsAssigned,
			Map<Integer, Integer> mapVertexIdToPartitionId,
			Map<Integer, String> mapPartitionIdToWorkerId, Map<String, Worker> mapWorkerIdToWorker)
			throws RemoteException {
		log.info("WorkerImpl: setWorkerPartitionInfo");
		log.info("totalPartitionsAssigned " + totalPartitionsAssigned
				+ " mapPartitionIdToWorkerId: " + mapPartitionIdToWorkerId);
		log.info("vertex2partitionMapSize: " + mapVertexIdToPartitionId.size());
		this.totalPartitionsAssigned = totalPartitionsAssigned;
		this.mapVertexIdToPartitionId = mapVertexIdToPartitionId;
		this.mapPartitionIdToWorkerId = mapPartitionIdToWorkerId;
		this.worker2WorkerProxy = new Worker2WorkerProxy(mapWorkerIdToWorker);
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
		if (System.getSecurityManager() == null) {
			System.setSecurityManager(new SecurityManager());
		}
		try {
			String coordinatorMachineName = args[0];
			log.info("masterMachineName " + coordinatorMachineName);

			String masterURL = "//" + coordinatorMachineName + "/" + KV.COORDINATOR_SERVICE_NAME;
			Worker2Coordinator worker2Coordinator = (Worker2Coordinator) Naming.lookup(masterURL);
			Worker worker = new WorkerSyncImpl();
			Worker2Coordinator coordinatorProxy = worker2Coordinator.register(worker,
					worker.getWorkerID(), worker.getNumThreads());

			worker.setCoordinatorProxy(coordinatorProxy);
			log.info("Worker is bound and ready for computations ");

		} catch (Exception e) {
			log.error("ComputeEngine exception:");
			e.printStackTrace();
		}
	}

	/**
	 * Sets the master proxy.
	 * 
	 * @param masterProxy
	 *            the new master proxy
	 */
	@Override
	public void setCoordinatorProxy(Worker2Coordinator coordinatorProxy) {
		this.coordinatorProxy = coordinatorProxy;
	}

	/**
	 * Receive message.
	 * 
	 * @param incomingMessages
	 *            the incoming messages
	 * @throws RemoteException
	 *             the remote exception
	 */
	@Override
	public void receiveMessage(List<Message<?>> incomingMessages) throws RemoteException {

		// log.debug("onRecevieIncomingMessages: " + incomingMessages.size());
		// log.debug(incomingMessages.toString());

		/** partitionID to message list */
		List<Message<?>> partitionMessages = null;

		int partitionID = -1;
		int vertexID = -1;

		for (Message<?> message : incomingMessages) {
			vertexID = message.getDestinationVertexID();
			partitionID = mapVertexIdToPartitionId.get(vertexID);
			if (currentIncomingMessages.containsKey(partitionID)) {
				currentIncomingMessages.get(partitionID).add(message);
			} else {
				partitionMessages = new ArrayList<Message<?>>();
				partitionMessages.add(message);
				currentIncomingMessages.put(partitionID, partitionMessages);
			}
		}
	}

	/**
	 * Receives the messages sent by all the vertices in the same node and
	 * updates the current incoming message queue.
	 * 
	 * @param destinationVertex
	 *            Represents the destination vertex to which the message has to
	 *            be sent
	 * @param incomingMessage
	 *            Represents the incoming message for the destination vertex
	 */
	public void updateIncomingMessages(int partitionID, Message<?> incomingMessage) {
		List<Message<?>> partitionMessages = null;
		if (currentIncomingMessages.containsKey(partitionID)) {
			currentIncomingMessages.get(partitionID).add(incomingMessage);
		} else {
			partitionMessages = new ArrayList<Message<?>>();
			partitionMessages.add(incomingMessage);
			currentIncomingMessages.put(partitionID, partitionMessages);
		}
	}

	/** shutdown the worker */
	@Override
	public void shutdown() throws RemoteException {
		java.util.Date date = new java.util.Date();
		log.info("Worker" + workerID + " goes down now at :" + new Timestamp(date.getTime()));
		System.exit(0);
	}

	@Override
	public void nextLocalCompute(long superstep) throws RemoteException {

		/**
		 * Next local compute. No generated new local compute tasks. Transit
		 * compute task and status from the last step.
		 * */

		this.superstep = superstep;

		// Put all elements in current incoming queue to previous incoming queue
		// and clear the current incoming queue.
		this.previousIncomingMessages.clear();
		this.previousIncomingMessages.putAll(this.currentIncomingMessages);
		this.currentIncomingMessages.clear();

		this.stopSendingMessage = false;
		this.flagLocalCompute = true;

		this.outgoingMessages.clear();

		// Put all local compute tasks in current task queue.
		// clear the completed partitions.
		// Note: To avoid concurrency issues, it is very important that
		// completed partitions is cleared before the Worker threads start to
		// operate on the partition queue in the next super step
		BlockingQueue<LocalComputeTask> temp = new LinkedBlockingDeque<LocalComputeTask>(
				nextLocalComputeTasksQueue);
		this.nextLocalComputeTasksQueue.clear();
		this.currentLocalComputeTaskQueue.addAll(temp);

	}

	@Override
	public void setQuery(Query query) throws RemoteException {

		/**
		 * Get distributed query from coordinator. and instantiate local compute
		 * tasks.
		 * */

		log.info("Get query:" + query.toString());

		for (Entry<Integer, Partition> entry : this.partitions.entrySet()) {

			try {

				LocalComputeTask localComputeTask = (LocalComputeTask) Class.forName(
						KV.CLASS_LOCAL_COMPUTE_TASK).newInstance();
				localComputeTask.init(query, entry.getKey());
				this.nextLocalComputeTasksQueue.add(localComputeTask);

			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// new PageRankTask(query, entry.getKey());

		log.info("Instantiate " + this.nextLocalComputeTasksQueue.size() + " local task.");

	}

	@Override
	public void processPartialResult() throws RemoteException {

		log.debug("processPartialResult.");

		BlockingQueue<LocalComputeTask> temp = new LinkedBlockingDeque<LocalComputeTask>(
				nextLocalComputeTasksQueue);
		this.nextLocalComputeTasksQueue.clear();
		this.currentLocalComputeTaskQueue.addAll(temp);

		this.flagLastStep = true;
		this.stopSendingMessage = false;

	}

	@Override
	public void vote2halt() throws RemoteException {
		throw new IllegalArgumentException("this mothod doesn't support in synchronised model.");
	}

	@Override
	public void voteAgain() throws RemoteException {
		throw new IllegalArgumentException("this mothod doesn't support in synchronised model.");
	}

	@Override
	public boolean isHalt() {
		throw new IllegalArgumentException("this mothod doesn't support in synchronised model.");
	}
}
