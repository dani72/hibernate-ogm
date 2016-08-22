/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.dialect.impl;

import com.rethinkdb.model.MapObject;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.hibernate.ogm.datastore.rethinkdb.RethinkDBDialect;

import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.TupleSnapshot;

/**
 * A {@link TupleSnapshot} based on a {@link DBObject} retrieved from MongoDB.
 *
 * @author Guillaume Scheibel &lt;guillaume.scheibel@gmail.com&gt;
 * @author Christopher Auston
 */
public class RethinkDBTupleSnapshot implements TupleSnapshot {

	public static final Pattern EMBEDDED_FIELDNAME_SEPARATOR = Pattern.compile( "\\." );

	/**
	 * Identifies the purpose a {@link RethinkDBTupleSnapshot}.
	 *
	 * @author Davide D'Alto &lt;davide@hibernate.org&gt;
	 */
	public enum SnapshotType {
		INSERT, UPDATE
	}

	private final Map<String, Object> dbObject;
	private final EntityKeyMetadata keyMetadata;
	private SnapshotType snapshotType;

	public RethinkDBTupleSnapshot( Map<String, Object> dbObject, EntityKeyMetadata meta, SnapshotType snapshotType) {
		this.dbObject = dbObject;
		this.keyMetadata = meta;
		this.snapshotType = snapshotType;
	}

	public Map<String, Object> getDbObject() {
		return dbObject;
	}

	@Override
	public Set<String> getColumnNames() {
		return dbObject.keySet();
	}

	public SnapshotType getSnapshotType() {
		return snapshotType;
	}

	public void setSnapshotType(SnapshotType snapshotType) {
		this.snapshotType = snapshotType;
	}

	@Override
	public boolean isEmpty() {
		return dbObject.keySet().isEmpty();
	}

	public boolean isKeyColumn(String column) {
		return keyMetadata != null && keyMetadata.isKeyColumn( column );
	}

	@Override
	public Object get(String column) {
		return isKeyColumn( column ) ? getKeyColumnValue( column ) : getValue( dbObject, column );
	}

	private Object getKeyColumnValue(String column) {
		Object idField = dbObject.get( RethinkDBDialect.ID_FIELDNAME );

		// single-column key will be stored as is
		if ( keyMetadata.getColumnNames().length == 1 ) {
			return idField;
		}
		// multi-column key nested within DBObject
		else {
			// the name of the column within the id object
			if ( column.contains( RethinkDBDialect.PROPERTY_SEPARATOR ) ) {
				column = column.substring( column.indexOf( RethinkDBDialect.PROPERTY_SEPARATOR ) + 1 );
			}

			return getValue( (Map<String, Object>) idField, column );
		}
	}

	/**
	 * The internal structure of a {@link DBOject} is like a tree. Each embedded object is a new {@code DBObject}
	 * itself. We traverse the tree until we've arrived at a leaf and retrieve the value from it.
	 */
	private Object getValue(Map<String, Object> dbObject, String column) {
		Object valueOrNull = RethinkHelpers.getValueOrNull( dbObject, column );
		return valueOrNull;
	}
}
