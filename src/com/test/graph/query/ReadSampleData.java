package com.test.graph.query;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.oss.driver.api.core.CqlSession;

public class ReadSampleData {

	private static final Logger LOG = LoggerFactory.getLogger(ReadSampleData.class);

	public static void main(String[] args) {

		CqlSession session = CqlSession.builder().build();
		GraphTraversalSource g = AnonymousTraversalSource.traversal()
				.withRemote(DseGraph.remoteConnectionBuilder(session).build());

		LOG.info("Started.. ");

		List<Map<Object, Object>> vertices = g.V().hasLabel("Access").has("tenantId", "tenant1").has("appId", "abcd")
				.has("nativeType", "Accounts").has("entityKey", P.gte("8")).limit(100000)
				.valueMap("entityKey", "tenantId", "appId", "nativeType").toList();

		LOG.info("Verticies: " + vertices.size());

		AtomicInteger total = new AtomicInteger(0);

		List<Map<Object, Object>> data = vertices.parallelStream().filter(new Predicate<Map<Object, Object>>() {
			ThreadLocal<GraphTraversalSource> g = new ThreadLocal<GraphTraversalSource>();

			@Override
			public boolean test(Map<Object, Object> t) {
				// TODO Auto-generated method stub
				if (g.get() == null) {
					g.set(AnonymousTraversalSource.traversal()
							.withRemote(DseGraph.remoteConnectionBuilder(session).build()));
				}

				List<Vertex> alerts = g.get().V().hasLabel("Access").has("tenantId", "tenant1").has("appId", "abcd")
						.has("nativeType", "Accounts").has("entityKey", ((List) t.get("entityKey")).get(0)).outE("Is")
						.inV().outE("With_Alert").inV().has("level", P.within(1, 2, 3)).toList();

				if (total.incrementAndGet() % 10000 == 0) {
					LOG.info("Total Searches: " + total.get());
				}

				return alerts != null && alerts.size() > 0;
			}

		}).collect(Collectors.toList());

		LOG.info("Total matches: " + data.size());

		System.exit(0);
	}

}
