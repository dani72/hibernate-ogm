/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.utils;

import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.model.MapObject;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import static org.hibernate.annotations.SourceType.DB;

/**
 * A builder for mocked {@link MongoClient} instances which return given {@link DBObject}s for given collections.
 * <p>
 * Note that currently only one {@code DBObject} is allowed per collection, but this could be expanded into a more
 * general mechanism if required.
 *
 * @author Gunnar Morling
 */
public class MockMongoClientBuilder {

	/**
	 * Builds an new mock MongoDB client.
	 *
	 * @return a builder context following the fluent invocation pattern.
	 */
//	public static MockMongoClientBuilderContext mockClient() {
//		return new MockMongoClientBuilderContext();
//	}
//
//	public static class MockMongoClientBuilderContext {
//
//		private final Map<String, Table> collections = new HashMap<>();
//
//		/**
//		 * Registers the given {@link DBObject} with the specified collection. The object can be retrieved from the
//		 * collection via {@link DBCollection#findOne(DBObject, DBObject))}.
//		 * <p>
//		 * Note that currently only one {@code DBObject} is supported per collection, but this could be expanded into a
//		 * more general mechanism if required.
//		 */
//		public MockMongoClientBuilderContext insert(String collectionName, MapObject object) {
//			Table collection = mock( Table.class );
//			collections.put( collectionName, collection );
//
//			when( collection.get( any( MapObject.class ), any( MapObject.class )) ).thenReturn( object );
//
//			return this;
//		}
//
//		/**
//		 * Builds and returns a mock MongoDB client based on the given configuration.
//		 */
//		public MockMongoClient build() {
//			DB database = mock( DB.class );
//
//			DBCollection defaultCollection = mock( DBCollection.class );
//			when( database.getCollection( anyString() ) ).thenReturn( defaultCollection );
//
//			for ( Entry<String, DBCollection> collection : collections.entrySet() ) {
//				when( database.getCollection( collection.getKey() ) ).thenReturn( collection.getValue() );
//			}
//
//			MongoClient mongoClient = mock( MongoClient.class );
//			when( mongoClient.getDatabaseNames() ).thenReturn( Collections.<String>emptyList() );
//			when( mongoClient.getDB( anyString() ) ).thenReturn( database );
//
//			return new MockMongoClient( collections, defaultCollection, mongoClient );
//		}
//	}
//
//	/**
//	 * A mock client for MongoDB.
//	 *
//	 * @author Gunnar Morling
//	 */
//	public static class MockMongoClient {
//
//		private final Map<String, DBCollection> collections;
//		private final DBCollection defaultCollection;
//		private final MongoClient client;
//
//		public MockMongoClient(Map<String, DBCollection> collections, DBCollection defaultCollection, MongoClient client) {
//			this.collections = collections;
//			this.defaultCollection = defaultCollection;
//			this.client = client;
//		}
//
//		/**
//		 * Returns a mock {@link MongoClient}.
//		 */
//		public MongoClient getClient() {
//			return client;
//		}
//
//		/**
//		 * Returns the collection with a given name. This is a Mockito mock object, so verifications can be performed on
//		 * it.
//		 */
//		public DBCollection getCollection(String collectionName) {
//			DBCollection collection = collections.get( collectionName );
//			return collection != null ? collection : defaultCollection;
//		}
//	}
}
