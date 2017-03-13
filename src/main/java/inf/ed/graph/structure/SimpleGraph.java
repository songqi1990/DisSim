package inf.ed.graph.structure;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;

public class SimpleGraph<V extends Vertex, E extends Edge> implements Graph<V, E> {

	private DirectedGraph<V, E> graph;
	private VertexFactory<V> vertexFactory;
	private Map<Integer, V> vertices;
	static Logger log = LogManager.getLogger(SimpleGraph.class);

	public SimpleGraph(Class<V> vertexClass, Class<E> edgeClass) {
		vertexFactory = new VertexFactory<V>(vertexClass);
		SimpleEdgeFactory<V, E> ef = new SimpleEdgeFactory<V, E>(edgeClass);
		graph = new SimpleDirectedGraph<V, E>(ef);
	}

	@Override
	public boolean addVertex(V vertex) {
		if (vertices != null) {
			vertices.put(vertex.getID(), vertex);
		}
		return graph.addVertex(vertex);
	}

	@Override
	public boolean addEdge(E edge) {
		throw new IllegalArgumentException("simple graph doesn't support this method.");
	}

	@Override
	public boolean addEdge(V from, V to) {
		graph.addEdge(from, to);
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<V> getChildren(V vertex) {

		Set<V> children = new HashSet<V>();
		for (E e : graph.outgoingEdgesOf(vertex)) {
			children.add((V) e.to());
		}
		return children;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<V> getParents(V vertex) {

		Set<V> parents = new HashSet<V>();
		for (E e : graph.incomingEdgesOf(vertex)) {
			parents.add((V) e.from());
		}
		return parents;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<V> getNeighbours(V vertex) {

		Set<V> neighbours = new HashSet<V>();
		for (E e : graph.incomingEdgesOf(vertex)) {
			neighbours.add((V) e.from());
		}
		for (E e : graph.outgoingEdgesOf(vertex)) {
			neighbours.add((V) e.to());
		}
		return neighbours;
	}

	@Override
	public Set<Integer> getChildren(int vertexID) {
		Set<Integer> children = new HashSet<Integer>();
		for (E e : graph.outgoingEdgesOf(this.allVertices().get(vertexID))) {
			children.add(e.to().getID());
		}
		return children;
	}

	@Override
	public Set<Integer> getParents(int vertexID) {

		Set<Integer> parents = new HashSet<Integer>();
		for (E e : graph.incomingEdgesOf(this.allVertices().get(vertexID))) {
			parents.add(e.from().getID());
		}
		return parents;
	}

	@Override
	public Set<Integer> getNeighbours(int vertexID) {
		Set<Integer> neighbours = new HashSet<Integer>();
		for (E e : graph.incomingEdgesOf(this.allVertices().get(vertexID))) {
			neighbours.add(e.from().getID());
		}
		for (E e : graph.outgoingEdgesOf(this.allVertices().get(vertexID))) {
			neighbours.add(e.to().getID());
		}
		return neighbours;
	}

	@Override
	public int edgeSize() {
		return graph.edgeSet().size();
	}

	@Override
	public int vertexSize() {
		return graph.vertexSet().size();
	}

	@Override
	public V getVertex(int vID) {
		return this.allVertices().get(vID);
	}

	@Override
	public V getRandomVertex() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<E> allEdges() {
		return graph.edgeSet();
	}

	@Override
	public Map<Integer, V> allVertices() {

		if (vertices == null) {
			vertices = new HashMap<Integer, V>();
			for (V vertex : graph.vertexSet()) {
				vertices.put(vertex.getID(), vertex);
			}
		}

		return vertices;
	}

	@Override
	public void clear() {
		throw new IllegalArgumentException("Simple graph doesn't support this method currently.");
	}

	@Override
	public void clearEdges() {
		throw new IllegalArgumentException("Simple graph doesn't support this method currently.");

	}

	@Override
	public boolean contains(int vertexID) {
		return this.allVertices().containsKey(vertexID);
	}

	@Override
	public boolean contains(V from, V to) {
		return graph.containsEdge(from, to);
	}

	@Override
	public boolean contains(E edge) {
		return graph.containsEdge(edge);
	}

	@Override
	public int degree(V vertex) {
		throw new IllegalArgumentException("Simple graph doesn't support this method currently.");
	}

	@Override
	public Set<E> getEdges(V vertex1, V vertex2) {
		return graph.getAllEdges(vertex1, vertex2);
	}

	@Override
	public boolean hasCycles() {
		throw new IllegalArgumentException("Simple graph doesn't support this method currently.");
	}

	@Override
	public boolean removeEdge(E edge) {
		return graph.removeEdge(edge);
	}

	@Override
	public boolean removeEdge(V from, V to) {
		graph.removeEdge(from, to);
		return true;
	}

	@Override
	public boolean removeVertex(V vertex) {
		if (vertices != null) {
			vertices.remove(vertex.getID());
		}
		return graph.removeVertex(vertex);
	}

	@Override
	public void finalizeGraph() {
	}

	@Override
	public boolean loadGraphFromVEFile(String filePathWithoutExtension) {
		FileInputStream fileInputStream = null;
		Scanner sc = null;

		try {

			/** load vertices */
			fileInputStream = new FileInputStream(filePathWithoutExtension + ".v");

			sc = new Scanner(fileInputStream, "UTF-8");
			while (sc.hasNextLine()) {

				V v = vertexFactory.createVertexWithString(sc.nextLine());
				this.addVertex(v);
			}

			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (sc != null) {
				sc.close();
			}

			/** load edges */
			fileInputStream = new FileInputStream(filePathWithoutExtension + ".e");
			sc = new Scanner(fileInputStream, "UTF-8");
			while (sc.hasNextLine()) {

				String[] elements = sc.nextLine().split("\t");

				V source = this.getVertex(Integer.parseInt(elements[0].trim()));
				V target = this.getVertex(Integer.parseInt(elements[1].trim()));

				this.addEdge(source, target);
			}

			if (fileInputStream != null) {
				fileInputStream.close();
			}
			if (sc != null) {
				sc.close();
			}

			log.info("graph loaded.");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	@Override
	public boolean contains(int fromID, int toID) {
		return graph.containsEdge(this.getVertex(fromID), this.getVertex(toID));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Graph<V, E> getSubgraph(Class<V> vertexClass, Class<E> edgeClass, V center, int bound) {

		Graph<V, E> subgraph = new SimpleGraph<V, E>(vertexClass, edgeClass);

		Queue<V> q = new LinkedList<V>();
		Int2IntMap visited = new Int2IntOpenHashMap();
		q.add(center);
		visited.put(center.getID(), 0);
		subgraph.addVertex(center);

		while (!q.isEmpty()) {
			V current = q.poll();
			int dist = visited.get(current.getID());
			if (dist >= bound) {
				break;
			}
			for (E e : graph.outgoingEdgesOf(current)) {
				V tv = (V) e.to();
				if (!visited.keySet().contains(tv.getID())) {
					q.add(tv);
					visited.put(tv.getID(), dist + 1);
					if (!subgraph.contains(tv.getID())) {
						subgraph.addVertex(tv);
					}
				}
				if (!subgraph.contains(current, tv)) {
					subgraph.addEdge(current, tv);
				}
			}
			for (E e : graph.incomingEdgesOf(current)) {
				V fv = (V) e.from();
				if (!visited.keySet().contains(fv.getID())) {
					q.add(fv);
					visited.put(fv.getID(), dist + 1);
					if (!subgraph.contains(fv.getID())) {
						subgraph.addVertex(fv);
					}
				}
				if (!subgraph.contains(fv, current)) {
					subgraph.addEdge(fv, current);
				}
			}
		}
		return subgraph;
	}

	@Override
	public void display(int limit) {
		System.out.println("The graph has the following structure: ");
		System.out.println(graph.vertexSet().size() + " vertices -  limit " + limit);
		int i = 0;
		for (V v : graph.vertexSet()) {
			if (i > limit) {
				break;
			} else {
				i++;
				System.out.println(v);
			}
		}
		i = 0;
		System.out.println(graph.edgeSet().size() + " edges -  limit " + limit);
		for (E e : graph.edgeSet()) {
			if (i > limit) {
				break;
			} else {
				i++;
				System.out.println(e);
			}
		}
	}

	@Override
	public int getRadius(V vertex) {
		// TODO Auto-generated method stub
		return 0;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int getDiameter() {
		int max = 0;
		Int2IntMap visited = new Int2IntOpenHashMap();
		Queue<V> q = new LinkedList<V>();

		for (V vf : graph.vertexSet()) {
			visited.clear();
			q.clear();
			q.add(vf);
			visited.put(vf.getID(), 0);
			while (!q.isEmpty()) {
				V v = q.poll();
				int dist = visited.get(v.getID());
				for (E e : graph.outgoingEdgesOf(v)) {
					V tv = (V) e.to();
					if (!visited.keySet().contains(tv.getID())) {
						q.add(tv);
						visited.put(tv.getID(), dist + 1);
					}
				}
				for (E e : graph.incomingEdgesOf(v)) {
					V fv = (V) e.from();
					if (!visited.keySet().contains(fv.getID())) {
						q.add(fv);
						visited.put(fv.getID(), dist + 1);
					}
				}
			}

			for (int v : visited.keySet()) {
				int dist = visited.get(v);
				if (dist > max) {
					max = dist;
				}
			}
		}
		return max;
	}

}
