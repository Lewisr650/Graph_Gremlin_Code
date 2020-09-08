package com.test.graph.insert;

import java.util.List;
import java.util.UUID;

import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.oss.driver.api.core.CqlSession;

public class InsertSampleData {

	private static final Logger LOG = LoggerFactory.getLogger(InsertSampleData.class);


	public static void main(String[] args) {

		
		CqlSession session = CqlSession.builder().build();
		GraphTraversalSource g = AnonymousTraversalSource.traversal()
				.withRemote(DseGraph.remoteConnectionBuilder(session).build());

		int idx = 0;
		for (idx = 0; idx < 100000; idx++) {
			GraphTraversal<Vertex, Vertex> traversal = g.addV("Access").property("appId", "abcd")
					.property("nativeType", "Accounts").property("tenantId", "tenant1")
					.property("entityKey", "8a819b536a00e7e4016a01a0fadf0616" + idx).as("access");

			traversal.addV("Entity").property("appId", "abcd").property("nativeType", "Accounts")
					.property("tenantId", "tenant1").property("entityKey", "8a819b536a00e7e4016a01a0fadf0616" + idx)
					.property("entityGlobalId", UUID.randomUUID()).as("entity")
					

					.addE("Is").from("access").to("entity")

					.addV("Alert").property("tenantId", "tenant1").property("appId", "PlatformAccess")
					.property("nativeType", "Unix Platform Access").property("taskId", 10).property("alertType", "Info")
					.property("entityGlobalId", UUID.randomUUID()).

					property("level", Byte.parseByte("" + (int)((Math.random() * 1000) % 3) + 1)).as("alert")
					
					.addE("With_Alert").from("entity").to("alert").iterate();
			;
			// as("access").iterate()
			if (idx % 1000 == 0) {
				LOG.info("IDX : " + idx);
			}
		}

//		List<Vertex> vertices = g.V().hasLabel("Access").toList();
//
//		for (Vertex v : vertices) {
//			System.out.println(v);
//			System.out.println("Tenant: " + v.property("tenantId"));
//			
//
//		}

		System.exit(0);
	}

}
