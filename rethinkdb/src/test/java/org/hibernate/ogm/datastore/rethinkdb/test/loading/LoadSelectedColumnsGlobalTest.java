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
import java.util.Map;

import org.hibernate.ogm.datastore.document.cfg.DocumentStoreProperties;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.rethinkdb.impl.RethinkDBDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;

/**
 * @author Guillaume Scheibel &lt;guillaume.scheibel@gmail.com&gt;
 */
public class LoadSelectedColumnsGlobalTest extends LoadSelectedColumnsCollectionTest {

	@Override
	protected void configure(Map<String, Object> settings) {
		settings.put(
				DocumentStoreProperties.ASSOCIATIONS_STORE,
				AssociationStorageType.ASSOCIATION_DOCUMENT
		);
	}

	/**
	 * To be sure the datastoreProvider retrieves only the columns we want,
	 * an extra column is manually added to the association document
	 */
	@Override
	protected void addExtraColumn() {
		RethinkDBDatastoreProvider provider = (RethinkDBDatastoreProvider) super.getService( DatastoreProvider.class );
		Db database = provider.getDatabase();
		Table collection = database.table( "Associations" );

		final MapObject idObject = com.rethinkdb.RethinkDB.r.hashMap();
		idObject.put( "Project_id", "projectID" );
		idObject.put( "table", "Project_Module" );

		MapObject query = com.rethinkdb.RethinkDB.r.hashMap();
		query.put( "_id", idObject );

		MapObject updater = com.rethinkdb.RethinkDB.r.hashMap();
		updater.put( "$push", com.rethinkdb.RethinkDB.r.hashMap( "extraColumn", 1 ) );
		collection.update( updater );
	}
}
