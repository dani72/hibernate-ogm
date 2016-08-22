/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.test.loading;

import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.gen.ast.Table;
import com.rethinkdb.model.MapObject;
import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;
import java.util.Set;

import org.hibernate.ogm.datastore.document.cfg.DocumentStoreProperties;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.rethinkdb.RethinkDBDialect;
import org.hibernate.ogm.datastore.rethinkdb.impl.RethinkDBDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;

/**
 * @author Guillaume Scheibel &lt;guillaume.scheibel@gmail.com&gt;
 */
public class LoadSelectedColumnsInEntityTest extends LoadSelectedColumnsCollectionTest {

	@Override
	protected void configure(Map<String, Object> settings) {
		settings.put(
				DocumentStoreProperties.ASSOCIATIONS_STORE,
				AssociationStorageType.IN_ENTITY
		);
	}

	@Override
	protected void addExtraColumn() {
		RethinkDBDatastoreProvider provider = (RethinkDBDatastoreProvider) super.getService( DatastoreProvider.class );
		Db database = provider.getDatabase();
		Table collection = database.table( "Project" );

		MapObject query = com.rethinkdb.RethinkDB.r.hashMap();
		query.put( "_id", "projectID" );

		MapObject updater = com.rethinkdb.RethinkDB.r.hashMap();
		updater.put( "$push", com.rethinkdb.RethinkDB.r.hashMap( "extraColumn", 1 ) );
		collection.update( updater );
	}

	@Override
	protected void checkLoading( Map<String, Object> associationObject) {
		/*
		 * The only column (except _id) that needs to be retrieved is "modules"
		 * So we should have 2 columns
		 */
		final Set<?> retrievedColumns = associationObject.keySet();
		assertThat( retrievedColumns ).hasSize( 2 ).containsOnly( RethinkDBDialect.ID_FIELDNAME, "modules" );
	}
}
