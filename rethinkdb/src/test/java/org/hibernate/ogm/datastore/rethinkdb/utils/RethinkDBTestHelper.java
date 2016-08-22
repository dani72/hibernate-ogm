/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.ogm.datastore.rethinkdb.utils;

import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.model.MapObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.OgmSessionFactory;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.rethinkdb.RethinkDB;
import org.hibernate.ogm.datastore.rethinkdb.RethinkDBDialect;
import org.hibernate.ogm.datastore.rethinkdb.configuration.impl.RethinkDBConfiguration;
import org.hibernate.ogm.datastore.rethinkdb.impl.RethinkDBDatastoreProvider;
import org.hibernate.ogm.datastore.rethinkdb.logging.impl.Log;
import org.hibernate.ogm.datastore.rethinkdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.spi.DatastoreConfiguration;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.exception.impl.Exceptions;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.utils.GridDialectTestHelper;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;

/**
 * @author Guillaume Scheibel &lt;guillaume.scheibel@gmail.com&gt;
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt;
 */
public class RethinkDBTestHelper implements GridDialectTestHelper {

	private static final Log log = LoggerFactory.getLogger();

	static {
		// Read host and port from environment variable
		// Maven's surefire plugin set it to the string 'null'
		String mongoHostName = System.getenv( "MONGODB_HOSTNAME" );
		if ( isNotNull( mongoHostName ) ) {
			System.getProperties().setProperty( OgmProperties.HOST, mongoHostName );
		}
		String mongoPort = System.getenv( "MONGODB_PORT" );
		if ( isNotNull( mongoPort ) ) {
			System.getProperties().setProperty( OgmProperties.PORT, mongoPort );
		}
	}

	private static boolean isNotNull(String mongoHostName) {
		return mongoHostName != null && mongoHostName.length() > 0 && ! "null".equals( mongoHostName );
	}

	@Override
	public long getNumberOfEntities(Session session) {
		return getNumberOfEntities( session.getSessionFactory() );
	}

	@Override
	public long getNumberOfEntities(SessionFactory sessionFactory) {
		RethinkDBDatastoreProvider provider = RethinkDBTestHelper.getProvider( sessionFactory );
		Db db = provider.getDatabase();
		int count = 0;

		for ( String collectionName : getEntityCollections( sessionFactory ) ) {
//			count += db.table( collectionName ).count();
		}

		return count;
	}

	private boolean isSystemCollection(String collectionName) {
		return collectionName.startsWith( "system." );
	}


	@Override
	public long getNumberOfAssociations(Session session) {
		return getNumberOfAssociations( session.getSessionFactory() );
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory) {
		long associationCount = getNumberOfAssociationsFromGlobalCollection( sessionFactory );
		associationCount += getNumberOfAssociationsFromDedicatedCollections( sessionFactory );
//		associationCount += getNumberOfEmbeddedAssociations( sessionFactory );

		return associationCount;
	}

	public long getNumberOfAssociationsFromGlobalCollection(SessionFactory sessionFactory) {
		Db db = getProvider( sessionFactory ).getDatabase();
//		return db.table(RethinkDBConfiguration.DEFAULT_ASSOCIATION_STORE ).count();
return 0;
	}

	public long getNumberOfAssociationsFromDedicatedCollections(SessionFactory sessionFactory) {
		Db db = getProvider( sessionFactory ).getDatabase();

		Set<String> associationCollections = getDedicatedAssociationCollections( sessionFactory );
		long associationCount = 0;
		for ( String collectionName : associationCollections ) {
//???			associationCount += db.table( collectionName ).count();

                    return 0;
		}

		return associationCount;
	}

	// TODO Use aggregation framework for a more efficient solution; Given that there will only be a few
	// test collections/entities, that's good enough for now
	public long getNumberOfEmbeddedAssociations(SessionFactory sessionFactory) {
		Db db = getProvider( sessionFactory ).getDatabase();
		long associationCount = 0;

		for ( String entityCollection : getEntityCollections( sessionFactory ) ) {
//			Curosr entities = db.getAll( entityCollection )run();

//			while ( entities.hasNext() ) {
//				MapObject entity = entities.next();
//				associationCount += getNumberOfEmbeddedAssociations( entity );
//			}
		}

		return associationCount;
	}

	private int getNumberOfEmbeddedAssociations(MapObject entity) {
		int numberOfReferences = 0;

		for ( Object fieldName : entity.keySet() ) {
			Object field = entity.get( (String)fieldName );
			if ( isAssociation( field ) ) {
				numberOfReferences++;
			}
		}

		return numberOfReferences;
	}

	private boolean isAssociation(Object field) {
		return ( field instanceof List );
	}

	private Set<String> getEntityCollections(SessionFactory sessionFactory) {
		Db db = RethinkDBTestHelper.getProvider( sessionFactory ).getDatabase();
		Set<String> names = new HashSet<String>();

//		for ( String collectionName : db.getCollectionNames() ) {
//			if ( !isSystemCollection( collectionName ) &&
//					!isDedicatedAssociationCollection( collectionName ) &&
//					!isGlobalAssociationCollection( collectionName ) ) {
//				names.add( collectionName );
//			}
//		}

		return names;
	}

	private Set<String> getDedicatedAssociationCollections(SessionFactory sessionFactory) {
		Db db = RethinkDBTestHelper.getProvider( sessionFactory ).getDatabase();
		Set<String> names = new HashSet<String>();

//		for ( String collectionName : db.getCollectionNames() ) {
//			if ( isDedicatedAssociationCollection( collectionName ) ) {
//				names.add( collectionName );
//			}
//		}

		return names;
	}

	private boolean isDedicatedAssociationCollection(String collectionName) {
		return collectionName.startsWith( RethinkDBDialect.ASSOCIATIONS_COLLECTION_PREFIX );
	}

	private boolean isGlobalAssociationCollection(String collectionName) {
		return collectionName.equals(RethinkDBConfiguration.DEFAULT_ASSOCIATION_STORE );
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> extractEntityTuple(Session session, EntityKey key) {
		RethinkDBDatastoreProvider provider = RethinkDBTestHelper.getProvider( session.getSessionFactory() );
		MapObject finder = com.rethinkdb.RethinkDB.r.hashMap( RethinkDBDialect.ID_FIELDNAME, key.getColumnValues()[0] );
		MapObject result = provider.getDatabase().table( key.getTable() ).get( finder ).run( provider.getConnection());
		replaceIdentifierColumnName( result, key );
		return result;
	}

	/**
	 * The MongoDB dialect replaces the name of the column identifier, so when the tuple is extracted from the db
	 * we replace the column name of the identifier with the original one.
	 * We are assuming the identifier is not embedded and is a single property.
	 */
	private void replaceIdentifierColumnName(MapObject result, EntityKey key) {
		Object idValue = result.get( RethinkDBDialect.ID_FIELDNAME );
		result.remove( RethinkDBDialect.ID_FIELDNAME );
		result.put( key.getColumnNames()[0], idValue );
	}

	@Override
	public boolean backendSupportsTransactions() {
		return false;
	}

	private static RethinkDBDatastoreProvider getProvider(SessionFactory sessionFactory) {
		DatastoreProvider provider = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService(
				DatastoreProvider.class );
		if ( !( RethinkDBDatastoreProvider.class.isInstance( provider ) ) ) {
			throw new RuntimeException( "Not testing with MongoDB, cannot extract underlying cache" );
		}
		return RethinkDBDatastoreProvider.class.cast( provider );
	}

	@Override
	public void dropSchemaAndDatabase(SessionFactory sessionFactory) {
//		RethinkDBDatastoreProvider provider = getProvider( sessionFactory );
//		try {
//			provider.getDatabase().dropDatabase();
//		}
//		catch ( MongoException ex ) {
//			throw log.unableToDropDatabase( ex, provider.getDatabase().getName() );
//		}
	}

	@Override
	public Map<String, String> getEnvironmentProperties() {
		//read variables from the System properties set in the static initializer
		Map<String,String> envProps = new HashMap<String, String>(2);
		copyFromSystemPropertiesToLocalEnvironment( OgmProperties.HOST, envProps );
		copyFromSystemPropertiesToLocalEnvironment( OgmProperties.PORT, envProps );
		copyFromSystemPropertiesToLocalEnvironment( OgmProperties.USERNAME, envProps );
		copyFromSystemPropertiesToLocalEnvironment( OgmProperties.PASSWORD, envProps );
		return envProps;
	}

	private void copyFromSystemPropertiesToLocalEnvironment(String environmentVariableName, Map<String, String> envProps) {
		String value = System.getProperties().getProperty( environmentVariableName );
		if ( value != null && value.length() > 0 ) {
			envProps.put( environmentVariableName, value );
		}
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory, AssociationStorageType type) {
		switch ( type ) {
			case ASSOCIATION_DOCUMENT:
				return getNumberOfAssociationsFromGlobalCollection( sessionFactory );
			case IN_ENTITY:
				return getNumberOfEmbeddedAssociations( sessionFactory );
			default:
				throw new IllegalArgumentException( "Unexpected association storaget type " + type );
		}
	}

	@Override
	public GridDialect getGridDialect(DatastoreProvider datastoreProvider) {
		return new RethinkDBDialect( (RethinkDBDatastoreProvider) datastoreProvider );
	}

	public static void assertDbObject(OgmSessionFactory sessionFactory, String collection, String queryDbObject, String expectedDbObject) {
		assertDbObject( sessionFactory, collection, queryDbObject, null, expectedDbObject );
	}

	public static void assertDbObject(OgmSessionFactory sessionFactory, String collection, String queryDbObject, String projectionDbObject, String expectedDbObject) {
//		MapObject finder = (MapObject) JSON.parse( queryDbObject );
//		MapObject fields = projectionDbObject != null ? (MapObject) JSON.parse( projectionDbObject ) : null;
//
//		RethinkDBDatastoreProvider provider = MongoDBTestHelper.getProvider( sessionFactory );
//		MapObject actual = provider.getDatabase().table( collection ).get( finder).pluck(fields ).run( null);
//
//		assertJsonEquals( expectedDbObject, actual.toString() );
	}

	public static Map<String, MapObject> getIndexes(OgmSessionFactory sessionFactory, String collection) {
//		RethinkDBDatastoreProvider provider = MongoDBTestHelper.getProvider( sessionFactory );
		List<MapObject> indexes = null; //??? provider.getDatabase().table( collection ).getIndexInfo();
//		Map<String, MapObject> indexMap = new HashMap<>();
//		for (MapObject index : indexes) {
//			indexMap.put( index.get( "name" ).toString(), index );
//		}
		return null;
	}

	public static void dropIndexes(OgmSessionFactory sessionFactory, String collection) {
//		RethinkDBDatastoreProvider provider = RethinkDBTestHelper.getProvider( sessionFactory );
//		provider.getDatabase().table( collection ).dropIndexes();
	}

	public static void assertJsonEquals(String expectedJson, String actualJson) {
		try {
			JSONCompareResult result = JSONCompare.compareJSON( expectedJson, actualJson, JSONCompareMode.NON_EXTENSIBLE );

			if ( result.failed() ) {
				throw new AssertionError(result.getMessage() + "; Actual: " + actualJson);
			}
		}
		catch (JSONException e) {
			Exceptions.<RuntimeException>sneakyThrow( e );
		}
	}

	@Override
	public Class<? extends DatastoreConfiguration<?>> getDatastoreConfigurationType() {
		return RethinkDB.class;
	}
}
