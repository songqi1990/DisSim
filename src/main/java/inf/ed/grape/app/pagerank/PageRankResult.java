package inf.ed.grape.app.pagerank;

import inf.ed.grape.interfaces.Result;
import inf.ed.grape.util.IO;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PageRankResult extends Result {

	private static final long serialVersionUID = -3137868401868468522L;

	static Logger log = LogManager.getLogger(Result.class);

	public Map<Integer, Double> ranks;

	public PageRankResult() {
		ranks = new HashMap<Integer, Double>();
	}

	@Override
	public void assemblePartialResults(Collection<Result> partialResults) {
		// TODO Auto-generated method stub

		for (Result result : partialResults) {

			PageRankResult pgresult = (PageRankResult) result;
			this.ranks.putAll(pgresult.ranks);
		}
	}

	@Override
	public void writeToFile(String filename) {

		/** add how to assemble a final result from results */
		log.debug("write result to file: " + filename);
		IO.writeMapToFile(this.ranks, filename);
	}

}
