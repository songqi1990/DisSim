package inf.ed.graph.structure;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * OrthogonalGraph (十字链表法) Ref:http://blog.fishc.com/2535.html
 * 
 * @author Xin Wang
 */
public class OrthogonalGraph<V extends OrthogonalVertex> implements Graph<V, OrthogonalEdge>,
		Serializable {

	static Logger log = LogManager.getLogger(OrthogonalGraph.class);

	private static final long serialVersionUID = 1L;

	// private TIntObjectHashMap<V> vertices;
	private HashMap<Integer, V> vertices;
	private int edgeSize; // amount of edges
	private VertexFactory<V> vertexFactory;

	public OrthogonalGraph(Class<V> vertexClass) {
		// this.vertices = new TIntObjectHashMap<V>();
		this.vertices = new HashMap<Integer, V>();
		this.vertexFactory = new VertexFactory<V>(vertexClass);
	}

	public boolean addVertex(V vertex) {
		this.vertices.put(vertex.getID(), vertex);
		return true;
	}

	public boolean addEdge(V from, V to) {
		OrthogonalEdge e = new OrthogonalEdge();
		e.SetFromNode(from);
		e.SetToNode(to);
		OrthogonalEdge hlink = to.GetFirstIn();
		OrthogonalEdge tlink = from.GetFirstOut();
		e.SetHLink(hlink);
		e.SetTLink(tlink);
		from.SetFirstOut(e);
		to.SetFirstIn(e);
		this.edgeSize++;
		return true;
	}

	public int edgeSize() {
		log.warn("Warning: if vertex or edge changed, then the edgesize is incorrect.");
		return this.edgeSize;
	}

	public int vertexSize() {
		return this.vertices.size();
	}

	public boolean contains(int vertexID) {
		return this.vertices.containsKey(vertexID);
	}

	public boolean removeEdge(V fv, V tv) {
		OrthogonalEdge e1 = fv.GetFirstOut();
		if (e1.to().equals(tv)) { // ���fv��firstout��ǡ���Ǳ�ɾ���ı�
			fv.SetFirstOut(e1.GetTLink()); // e1��tlink����Ϊnull
		} else {
			for (OrthogonalEdge e = e1; e != null; e = e.GetTLink()) { // ������e����ͬtail
				// node�ı�
				OrthogonalEdge ne = e.GetTLink(); // ne ����Ϊnull
				if (ne != null) {
					if (ne.to().equals(tv)) { // ne �Ǽ���ɾ���ı�,
												// ��ô��Ҫ��e.tlink����Ϊne.tlink
						e.SetTLink(ne.GetTLink());
						break;
					}
				}
			}
		}

		e1 = tv.GetFirstIn();
		if (e1.from().equals(fv)) { // ���tv��firstin��ǡ���Ǳ�ɾ���ı�
			tv.SetFirstIn(e1.GetHLink()); // e1.hlink����Ϊnull
		} else {
			for (OrthogonalEdge e = e1; e != null; e = e.GetHLink()) {
				OrthogonalEdge ne = e.GetHLink(); // ne ����Ϊnull
				if (ne != null) {
					if (ne.from().equals(fv)) { // ne
												// �Ǽ���ɾ���ıߣ���ô��Ҫ��e.hlink����Ϊne.hlink
						e.SetHLink(ne.GetHLink());
						break;
					}
				}
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	public boolean removeVertex(V vertex) {
		this.vertices.remove(vertex.getID());
		// TODO 需要调整this.edgeSize
		// FIXME 需要调整this.edgeSize
		// process n's parents
		OrthogonalEdge ep = vertex.GetFirstIn();
		if (ep != null) {
			for (OrthogonalEdge e = ep; e != null; e = e.GetHLink()) {
				V fv = (V) e.from();
				for (OrthogonalEdge e1 = fv.GetFirstOut(); e1 != null; e1 = e1.GetTLink()) {
					OrthogonalEdge e2 = e1.GetTLink(); // e2.GetToNode() 有可能是n
					V cfv = (V) e1.to();
					if (cfv.equals(vertex)) { // e1.firstout 指向了被删除的边
						fv.SetFirstOut(e2); // 如果e1.tonode = n,
											// 则设置fv的firstout为e2
						break;
					} else {
						V cfv2 = (V) e2.to();
						if (cfv2.equals(vertex)) {
							e1.SetTLink(e2.GetTLink()); // e2是e1的邻边,
														// 如果e2.tonode = n
														// (说明e2为被删除的边),
														// 则设置e1的tlink为e2的tlink
							break;
						}
					}
				}
			}
		}

		// process n's children
		ep = vertex.GetFirstOut();
		if (ep != null) {
			for (OrthogonalEdge e = ep; e != null; e = e.GetTLink()) {
				V tv = (V) e.to();
				for (OrthogonalEdge e1 = tv.GetFirstIn(); e1 != null; e1 = e1.GetHLink()) {
					OrthogonalEdge e2 = e1.GetHLink();
					V ptv = (V) e1.from();
					if (ptv.equals(vertex)) {
						tv.SetFirstIn(e2);
						break;
					} else {
						V ptv2 = (V) e2.from();
						if (ptv2.equals(vertex)) {
							e1.SetHLink(e2.GetHLink());
							break;
						}
					}
				}
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	public Set<V> getChildren(V vertex) {
		HashSet<V> cSet = new HashSet<V>();
		OrthogonalEdge e1 = vertex.GetFirstOut();
		for (OrthogonalEdge e = e1; e != null; e = e.GetTLink()) {
			V child = (V) e.to();
			cSet.add(child);
		}
		return cSet;
	}

	@SuppressWarnings("unchecked")
	public Set<V> getParents(V vertex) {
		HashSet<V> pSet = new HashSet<V>();
		OrthogonalEdge e1 = vertex.GetFirstIn();
		for (OrthogonalEdge e = e1; e != null; e = e.GetHLink()) {
			V parent = (V) e.from();
			pSet.add(parent);
		}
		return pSet;
	}

	public Set<V> getNeighbours(V vertex) {

		Set<V> neighbours = this.getChildren(vertex);
		neighbours.addAll(this.getParents(vertex));
		return neighbours;

	}

	@SuppressWarnings("unchecked")
	public Set<Integer> getChildren(int vertexID) {
		HashSet<Integer> cSet = new HashSet<Integer>();
		OrthogonalEdge e1 = this.getVertex(vertexID).GetFirstOut();
		for (OrthogonalEdge e = e1; e != null; e = e.GetTLink()) {
			V child = (V) e.to();
			cSet.add(child.getID());
		}
		return cSet;
	}

	@SuppressWarnings("unchecked")
	public Set<Integer> getParents(int vertexID) {
		HashSet<Integer> pSet = new HashSet<Integer>();
		OrthogonalEdge e1 = this.getVertex(vertexID).GetFirstIn();
		for (OrthogonalEdge e = e1; e != null; e = e.GetHLink()) {
			V parent = (V) e.from();
			pSet.add(parent.getID());
		}
		return pSet;
	}

	@Override
	public Set<Integer> getNeighbours(int vertexID) {
		Set<Integer> neighbours = this.getChildren(vertexID);
		neighbours.addAll(this.getParents(vertexID));
		return neighbours;
	}

	public V getVertex(int vID) {
		return this.vertices.get(vID);
	}

	public Map<Integer, V> allVertices() {
		// System.out.println("before all v: " + this.vertices);
		// System.out.println("after all v: " +
		// TDecorators.wrap(this.vertices));
		// return TDecorators.wrap(this.vertices);
		return this.vertices;
	}

	public V getRandomVertex() {
		// pick a random vertex should not call in a large graph
		assert this.vertexSize() < 1000;
		// using TIntObjectHashMap
		// int size = this.vertices.keys().length;
		// Random r = new Random();
		// int randomID = this.vertices.keys()[r.nextInt(size)];
		// return this.vertices.get(randomID);
		int item = new Random().nextInt(this.vertexSize());
		int i = 0;
		for (V v : this.vertices.values()) {
			if (i == item)
				return v;
			i = i + 1;
		}

		return null;
	}

	public boolean contains(V from, V to) {
		Queue<OrthogonalEdge> q = new LinkedList<OrthogonalEdge>();
		OrthogonalEdge e = from.GetFirstOut();
		if (e == null) {
			return false;
		} else {
			q.add(e);
		}

		while (!q.isEmpty()) {
			OrthogonalEdge ee = q.poll();
			if (ee.to() == to) {
				// TODO: check whether this judgement is correct.
				return true;
			}
			OrthogonalEdge ne = ee.GetTLink();
			if (ne != null) {
				q.add(ne);
			}
		}
		return false;
	}

	public int degree(V vertex) {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public boolean addEdge(OrthogonalEdge edge) {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public boolean contains(OrthogonalEdge edge) {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public boolean removeEdge(OrthogonalEdge edge) {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public Set<OrthogonalEdge> getEdges(V vertex1, V vertex2) {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public void clear() {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public void clearEdges() {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public boolean hasCycles() {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	public Set<OrthogonalEdge> allEdges() {
		throw new IllegalArgumentException(
				"Orthogonal graph doesn't support this method with certain para(s).");
	}

	@Override
	public void finalizeGraph() {
		return;
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
		V from = this.vertices.get(fromID);
		Queue<OrthogonalEdge> q = new LinkedList<OrthogonalEdge>();
		OrthogonalEdge e = from.GetFirstOut();
		if (e == null) {
			return false;
		} else {
			q.add(e);
		}

		while (!q.isEmpty()) {
			OrthogonalEdge ee = q.poll();
			if (ee.to().getID() == toID) {
				return true;
			}
			OrthogonalEdge ne = ee.GetTLink();
			if (ne != null) {
				q.add(ne);
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Graph<V, OrthogonalEdge> getSubgraph(Class<V> vertexClass,
			Class<OrthogonalEdge> edgeClass, V center, int bound) {
		OrthogonalGraph<V> subgraph = new OrthogonalGraph<V>(vertexClass);

		HashMap<V, Integer> visited = new HashMap<V, Integer>();
		Queue<V> q = new LinkedList<V>();
		q.add(center);
		V cv = (V) center.copyWithoutEdge();
		visited.put(cv, 0);
		subgraph.addVertex(cv);
		OrthogonalEdge e = null;
		int distance = 0;
		while (!q.isEmpty()) {
			V vertex = q.poll();
			V cvcopy = subgraph.getVertex(vertex.getID());
			distance = visited.get(cvcopy);
			if (distance >= bound) {
				break;
				// FIXME: add edges with nodes hop = bound
			}

			OrthogonalEdge e1 = vertex.GetFirstOut();
			for (e = e1; e != null; e = e.GetTLink()) {
				V tv = (V) e.to();
				V tvcopy = (V) tv.copyWithoutEdge();
				if (!visited.keySet().contains(tv)) {
					q.add(tv);
					subgraph.addVertex(tvcopy);
					visited.put(tvcopy, distance + 1);
				}
				subgraph.addEdge(cvcopy, tvcopy);
			}

			// get all parents.
			e1 = vertex.GetFirstIn();
			for (e = e1; e != null; e = e.GetHLink()) {
				V fv = (V) e.from();
				V fvcopy = (V) fv.copyWithoutEdge();
				if (!visited.keySet().contains(fv)) {
					q.add(fv);
					subgraph.addVertex(fvcopy);
					visited.put(fvcopy, distance + 1);
				}
				subgraph.addEdge(fvcopy, cvcopy);
			}

		}
		return subgraph;
	}

	@Override
	public void display(int limit) {
		System.out.println("The graph has the following structure: ");
		int i = 0;
		for (V n : this.allVertices().values()) {
			if (i < limit) {
				OrthogonalEdge e1 = n.GetFirstOut();
				String s = "";
				if (e1 != null) {
					for (OrthogonalEdge e = e1; e != null; e = e.GetTLink()) {
						s = s + ", " + e.to().getID();
					}
					if (!s.equals(""))
						s = s.substring(2);
				}
				System.out.println(n.getID() + ", links: " + s);
				i++;
			} else
				break;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public int getRadius(V center) {
		int max = 0;
		HashMap<V, Integer> visited = new HashMap<V, Integer>();
		Queue<V> q = new LinkedList<V>();
		q.add(center);
		visited.put(center, 0);
		OrthogonalEdge e = null;
		int distance = 0;
		while (!q.isEmpty()) {
			V vertex = q.poll();
			distance = visited.get(vertex);
			if (distance > max) {
				max = distance;
			}

			OrthogonalEdge e1 = vertex.GetFirstOut();
			for (e = e1; e != null; e = e.GetTLink()) {
				V tv = (V) e.to();
				if (!visited.keySet().contains(tv)) {
					q.add(tv);
					visited.put(tv, distance + 1);
				}
			}

			e1 = vertex.GetFirstIn();
			for (e = e1; e != null; e = e.GetHLink()) {
				V fv = (V) e.from();
				if (!visited.keySet().contains(fv)) {
					q.add(fv);
					visited.put(fv, distance + 1);
				}
			}
		}
		return max;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int getDiameter() {
		int max = 0;
		Int2IntMap visited = new Int2IntOpenHashMap();
		Queue<V> q = new LinkedList<V>();

		for (V vf : vertices.values()) {
			visited.clear();
			q.clear();
			q.add(vf);
			visited.put(vf.getID(), 0);
			while (!q.isEmpty()) {
				V v = q.poll();
				int dist = visited.get(v);
				OrthogonalEdge e1 = this.getVertex(v.getID()).GetFirstOut();
				for (OrthogonalEdge e = e1; e != null; e = e.GetTLink()) {
					V tv = (V) e.to();
					if (!visited.keySet().contains(tv.getID())) {
						q.add(tv);
						visited.put(tv.getID(), dist + 1);
					}
				}
				e1 = this.getVertex(v.getID()).GetFirstIn();
				for (OrthogonalEdge e = e1; e != null; e = e.GetHLink()) {
					V fv = (V) e.from();
					if (!visited.keySet().contains(fv)) {
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
