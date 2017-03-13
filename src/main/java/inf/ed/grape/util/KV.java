package inf.ed.grape.util;

public class KV {

	/** coordinator service name */
	public static final String COORDINATOR_SERVICE_NAME = "grape-coordinator";

	/** coordinator RMI service port */
	public static int RMI_PORT = 1099;

	public static int MAX_THREAD_LIMITATION = Integer.MAX_VALUE;

	public static String GRAPH_FILE_PATH = null;
	public static String QUERY_FILE_PATH = null;
	public static int PARTITION_COUNT = -1;
	public static int PARTITION_STRATEGY = -1;
	public static String OUTPUT_DIR = null;

	public static String CLASS_LOCAL_COMPUTE_TASK = null;
	public static String CLASS_MESSAGE = null;
	public static String CLASS_RESULT = null;
	public static String CLASS_QUERY = null;

	public static boolean ENABLE_COORDINATOR = false;
	public static boolean ENABLE_ASSEMBLE = false;
	public static boolean ENABLE_SYNC = false;
	public static boolean ENABLE_LOCAL_BATCH = false;
	public static boolean ENABLE_LOCAL_INCREMENTAL = false;
	public static boolean ENABLE_LOCAL_MESSAGE = false;

	/** load constant from properties file */
	static {
		try {
			RMI_PORT = Config.getInstance().getIntProperty("RMI_PORT");

			MAX_THREAD_LIMITATION = Config.getInstance().getIntProperty(
					"THREAD_LIMIT_ON_EACH_MACHINE");

			GRAPH_FILE_PATH = Config.getInstance().getStringProperty("GRAPH_FILE_PATH");

			QUERY_FILE_PATH = Config.getInstance().getStringProperty("QUERY_FILE_PATH");

			OUTPUT_DIR = Config.getInstance().getStringProperty("OUTPUT_DIR");

			PARTITION_COUNT = Config.getInstance().getIntProperty("PARTITION_COUNT");

			PARTITION_STRATEGY = Config.getInstance().getIntProperty("PARTITION_STRATEGY");

			ENABLE_COORDINATOR = Config.getInstance().getBooleanProperty("ENABLE_COORDINATOR");

			ENABLE_ASSEMBLE = Config.getInstance().getBooleanProperty("ENABLE_ASSEMBLE");

			ENABLE_SYNC = Config.getInstance().getBooleanProperty("ENABLE_SYNC");

			ENABLE_LOCAL_BATCH = Config.getInstance().getBooleanProperty("ENABLE_LOCAL_BATCH");

			ENABLE_LOCAL_INCREMENTAL = Config.getInstance().getBooleanProperty(
					"ENABLE_LOCAL_INCREMENTAL");

			ENABLE_LOCAL_MESSAGE = Config.getInstance().getBooleanProperty("ENABLE_LOCAL_MESSAGE");

			CLASS_LOCAL_COMPUTE_TASK = Config.getInstance()
					.getStringProperty("CLASS_LOCAL_COMPUTE");

			CLASS_RESULT = Config.getInstance().getStringProperty("CLASS_RESULT");

			CLASS_MESSAGE = Config.getInstance().getStringProperty("CLASS_MESSAGE");

			CLASS_QUERY = Config.getInstance().getStringProperty("CLASS_QUERY");

			// TODO:validate configuration, some combination are not valid.

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
