package inf.ed.graph.structure;

import org.jgrapht.EdgeFactory;

public class SimpleEdgeFactory<V, E> implements EdgeFactory<V, E> {

	private final Class<E> edgeClass;

	public SimpleEdgeFactory(Class<E> edgeClass) {
		this.edgeClass = edgeClass;
	}

	@Override
	public E createEdge(V sourceVertex, V targetVertex) {
		try {
			return this.edgeClass.getDeclaredConstructor(Object.class, Object.class).newInstance(
					sourceVertex, targetVertex);
		} catch (Exception e) {
			throw new RuntimeException("Edge factory with object failed", e);
		}
	}

}
