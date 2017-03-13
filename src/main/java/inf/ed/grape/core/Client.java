package inf.ed.grape.core;

import inf.ed.grape.communicate.Client2Coordinator;
import inf.ed.grape.interfaces.Query;
import inf.ed.grape.util.KV;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class Client {

	public static void main(String[] args) throws RemoteException, NotBoundException,
			MalformedURLException, ClassNotFoundException, InstantiationException,
			IllegalAccessException {
		String coordinatorMachineName = args[0];
		String coordinatorURL = "//" + coordinatorMachineName + "/" + KV.COORDINATOR_SERVICE_NAME;
		Client2Coordinator client2Coordinator = (Client2Coordinator) Naming.lookup(coordinatorURL);
		runApplication(client2Coordinator);
	}

	/**
	 * Run application.
	 * 
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	private static void runApplication(Client2Coordinator client2Coordinator)
			throws RemoteException, InstantiationException, IllegalAccessException,
			ClassNotFoundException {

		client2Coordinator.preProcess();
		Query q = (Query) Class.forName(KV.CLASS_QUERY).newInstance();
		client2Coordinator.putTask(q);
	}

}
