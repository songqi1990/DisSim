package inf.ed.grape.communicate;

import inf.ed.grape.core.Worker;
import inf.ed.grape.interfaces.Result;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Set;

/**
 * Defines an interface to register remote Worker with the coordinator
 * 
 * @author Yecol
 */

public interface Worker2Coordinator extends java.rmi.Remote, Serializable {

	public Worker2Coordinator register(Worker worker, String workerID, int numWorkerThreads)
			throws RemoteException;

	/* for synchronised model only */
	public void localComputeCompleted(String workerID, Set<String> activeWorkerIDs)
			throws RemoteException;

	/* for asynchronised model only */
	public void vote2halt(String workerID) throws RemoteException;

	public void sendPartialResult(String workerID, Map<Integer, Result> mapPartitionID2Result)
			throws RemoteException;

	public void shutdown() throws RemoteException;

}
