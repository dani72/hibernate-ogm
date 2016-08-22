/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.dialect.impl;


import com.rethinkdb.model.MapObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hibernate.ogm.datastore.document.association.impl.DocumentHelpers;
import org.hibernate.ogm.datastore.document.association.spi.AssociationRows;
import org.hibernate.ogm.datastore.rethinkdb.RethinkDBDialect;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationType;
import static org.hibernate.ogm.datastore.rethinkdb.dialect.impl.RethinkHelpers.getValueOrNull;

/**
 * An association snapshot based on a {@link DBObject} retrieved from MongoDB.
 *
 * @author Alan Fitton &lt;alan at eth0.org.uk&gt;
 * @author Emmanuel Bernard &lt;emmanuel@hibernate.org&gt;
 * @author Gunnar Morling
 */
public class RethinkDBAssociationSnapshot extends AssociationRows {

	private static final String EMBEDDABLE_COLUMN_PREFIX = ".value.";

	private final Map<String, Object> dbObject;

	public RethinkDBAssociationSnapshot(Map<String, Object> document, AssociationKey associationKey, AssociationStorageStrategy storageStrategy) {
		super(associationKey, getRows( document, associationKey, storageStrategy ), RethinkDBAssociationRowFactory.INSTANCE );
		this.dbObject = document;
	}

	//not for embedded
	public MapObject getQueryObject() {
		MapObject query = com.rethinkdb.RethinkDB.r.hashMap();
		query.put( RethinkDBDialect.ID_FIELDNAME, dbObject.get( RethinkDBDialect.ID_FIELDNAME ) );
		return query;
	}

	private static Collection<?> getRows(Map<String, Object> document, AssociationKey associationKey, AssociationStorageStrategy storageStrategy) {
		Collection<?> rows = null;

		if ( associationKey.getMetadata().getAssociationType() == AssociationType.ONE_TO_ONE ) {
			Object oneToOneValue = getValueOrNull( document, associationKey.getMetadata().getCollectionRole(), Object.class );
			if ( oneToOneValue != null ) {
				rows = Collections.singletonList( oneToOneValue );
			}
		}
		else {
			Object toManyValue;

			if ( storageStrategy == AssociationStorageStrategy.IN_ENTITY ) {
				toManyValue = getValueOrNull( document, associationKey.getMetadata().getCollectionRole() );
			}
			else {
				toManyValue = document.get( RethinkDBDialect.ROWS_FIELDNAME );
			}

			// list of rows
			if ( toManyValue instanceof Collection ) {
				rows = (Collection<?>) toManyValue;
			}
			// a map-typed association, rows are organized by row key
			else if ( toManyValue instanceof MapObject ) {
				rows = getRowsFromMapAssociation( associationKey, (MapObject) toManyValue );
			}
		}

		return rows != null ? rows : Collections.emptyList();
	}

	/**
	 * Restores the list representation of the given map-typed association. E.g. { 'home' : 123, 'work' : 456 } will be
	 * transformed into [{ 'addressType='home', 'address_id'=123}, { 'addressType='work', 'address_id'=456} ]) as
	 * expected by the row accessor.
	 */
	private static Collection<?> getRowsFromMapAssociation(AssociationKey associationKey, MapObject value) {
		String rowKeyIndexColumn = associationKey.getMetadata().getRowKeyIndexColumnNames()[0];
		List<MapObject> rows = new ArrayList<>();

		String[] associationKeyColumns = associationKey.getMetadata()
				.getAssociatedEntityKeyMetadata()
				.getAssociationKeyColumns();

		// Omit shared prefix of compound ids, will be handled in the row accessor
		String prefix = DocumentHelpers.getColumnSharedPrefix( associationKeyColumns );
		prefix = prefix == null ? "" : prefix + ".";

		String embeddedValueColumnPrefix = associationKey.getMetadata().getCollectionRole() + EMBEDDABLE_COLUMN_PREFIX;

		// restore the list representation
		for ( Object rowKey : value.keySet() ) {
			Object mapRow = value.get( rowKey );

			// include the row key index column
			MapObject row = com.rethinkdb.RethinkDB.r.hashMap();
			row.put( rowKeyIndexColumn, rowKey );

			// several value columns, copy them all
			if ( mapRow instanceof MapObject ) {
				for ( String column : associationKey.getMetadata().getAssociatedEntityKeyMetadata().getAssociationKeyColumns() ) {
					// The column is part of an element collection; Restore the "value" node in the hierarchy
					if ( column.startsWith( embeddedValueColumnPrefix ) ) {
						RethinkHelpers.setValue(
								row,
								column.substring( associationKey.getMetadata().getCollectionRole().length() + 1 ),
								( (MapObject) mapRow ).get( column.substring( embeddedValueColumnPrefix.length() ) )
						);
					}
					else {
						row.put(
								column.substring( prefix.length() ),
								( (MapObject) mapRow ).get( column.substring( prefix.length() ) )
						);
					}
				}
			}
			// single value column
			else {
				row.put( associationKey.getMetadata().getAssociatedEntityKeyMetadata().getAssociationKeyColumns()[0], mapRow );
			}

			rows.add( row );
		}

		return rows;
	}

	// TODO This only is used for tests; Can we get rid of it?
	public Map<String, Object> getDBObject() {
		return this.dbObject;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append( "MongoDBAssociationSnapshot(" );
		sb.append( size() );
		sb.append( ") RowKey entries)." );
		return sb.toString();
	}
}
