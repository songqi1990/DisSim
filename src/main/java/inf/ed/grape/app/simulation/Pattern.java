package inf.ed.grape.app.simulation;

import inf.ed.grape.interfaces.Query;
import inf.ed.grape.util.KV;
import inf.ed.graph.structure.Graph;
import inf.ed.graph.structure.OrthogonalEdge;
import inf.ed.graph.structure.OrthogonalGraph;
import inf.ed.graph.structure.adaptor.VertexOString;

import java.io.Serializable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Pattern implements Query, Serializable {

	static Logger log = LogManager.getLogger(Pattern.class);

	private static final long serialVersionUID = 1L;
	private Graph<VertexOString, OrthogonalEdge> graph;

	public Graph<VertexOString, OrthogonalEdge> getGraph() {
		return this.graph;
	}

	public Pattern() {
		this.loadPatternFromFile(KV.QUERY_FILE_PATH);
	}

	public boolean loadPatternFromFile(String filePath) {
		this.graph = new OrthogonalGraph<VertexOString>(VertexOString.class);
		this.graph.loadGraphFromVEFile(filePath);
		log.debug("Query loaded as below.");
		this.graph.display(1000);

		return true;
	}

	public String toString() {
		graph.display(1000);
		return "";
	}
}
