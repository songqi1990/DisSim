package inf.ed.graph.structure;

import java.util.Map;

public class EdgeFactory<V, E> {

	private final Class<V> vertexClass;
	private final Class<E> edgeClass;

	public EdgeFactory(Class<V> vertexClass, Class<E> edgeClass) {
		this.edgeClass = edgeClass;
		this.vertexClass = vertexClass;
	}

	public E createEdge() {
		try {
			return this.edgeClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Vertex factory failed", e);
		}
	}

	public E createEdgeWithMap(Map<String, Object> o) {
		try {
			V fromVertex = this.vertexClass.getDeclaredConstructor(Map.class, String.class)
					.newInstance(o, "f");
			V toVertex = this.vertexClass.getDeclaredConstructor(Map.class, String.class)
					.newInstance(o, "t");
			return this.edgeClass.getDeclaredConstructor(this.vertexClass, this.vertexClass)
					.newInstance(fromVertex, toVertex);
		} catch (Exception e) {
			throw new RuntimeException("Vertex factory with object failed", e);
		}
	}

	public E createEdgeWithString(String s) {
		try {
			return this.edgeClass.getDeclaredConstructor(String.class).newInstance(s);
		} catch (Exception e) {
			throw new RuntimeException("Vertex factory with string failed", e);
		}
	}

}
