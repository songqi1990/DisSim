package inf.ed.grape.app.simulation;

import inf.ed.grape.interfaces.Result;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SimulationResult extends Result {

	private static final long serialVersionUID = 1L;

	private Int2ObjectMap<IntSet> sim = new Int2ObjectOpenHashMap<IntSet>();

	static Logger log = LogManager.getLogger(SimulationResult.class);

	public Int2ObjectMap<IntSet> getSim() {
		return this.sim;
	}

	@Override
	public void assemblePartialResults(Collection<Result> partialResults) {

		for (Result result : partialResults) {
			SimulationResult sr = (SimulationResult) result;

			log.info(sr.getSim().toString());

			for (int key : sr.getSim().keySet()) {
				if (!this.sim.containsKey(key)) {
					this.sim.put(key, new IntOpenHashSet());
				}
				this.sim.get(key).addAll(sr.getSim().get(key));
			}
		}
	}

	@Override
	public void writeToFile(String filename) {
		log.info("final result:");
		System.out.println(this.sim.toString());
	}

}
