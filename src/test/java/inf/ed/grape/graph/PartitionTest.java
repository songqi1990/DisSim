package inf.ed.grape.graph;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class PartitionTest {

	static Logger log = LogManager.getLogger(PartitionTest.class);

	String filePath = "dataset/vldb2014/s.p1";

	@Test
	public void loadPartitionTest() {
		Partition p = new Partition(1);
		p.loadPartitionDataFromEVFile(filePath);
		log.debug(p.getPartitionInfo());
	}

}
