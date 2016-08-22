/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.dialect.impl;

import com.rethinkdb.model.MapObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.ogm.datastore.document.association.spi.AssociationRow;
import org.hibernate.ogm.datastore.document.association.spi.AssociationRow.AssociationRowAccessor;
import org.hibernate.ogm.datastore.document.association.spi.AssociationRowFactory;
import org.hibernate.ogm.datastore.document.association.spi.StructureOptimizerAssociationRowFactory;

/**
 * {@link AssociationRowFactory} which creates association rows based on the {@link DBObject} based representation used
 * in MongoDB.
 *
 * @author Gunnar Morling
 * @author Emmanuel Bernard &lt;emmanuel@hibernate.org&gt;
 */
public class RethinkDBAssociationRowFactory extends StructureOptimizerAssociationRowFactory<MapObject> {

	public static final RethinkDBAssociationRowFactory INSTANCE = new RethinkDBAssociationRowFactory();

	private RethinkDBAssociationRowFactory() {
		super( MapObject.class );
	}

	@Override
	protected MapObject getSingleColumnRow(String columnName, Object value) {
		MapObject dbObjectAsRow = com.rethinkdb.RethinkDB.r.hashMap();
		RethinkHelpers.setValue( dbObjectAsRow, columnName, value );
		return dbObjectAsRow;
	}

	@Override
	protected AssociationRowAccessor<MapObject> getAssociationRowAccessor(String[] prefixedColumns, String prefix) {
		return prefix != null ? new RethinkDBAssociationRowAccessor(prefixedColumns, prefix) : RethinkDBAssociationRowAccessor.INSTANCE;
	}

	private static class RethinkDBAssociationRowAccessor implements AssociationRow.AssociationRowAccessor<MapObject> {

		private static final RethinkDBAssociationRowAccessor INSTANCE = new RethinkDBAssociationRowAccessor();

		private final String prefix;
		private final List<String> prefixedColumns;

		public RethinkDBAssociationRowAccessor() {
			this( null, null );
		}

		public RethinkDBAssociationRowAccessor(String[] prefixedColumns, String prefix) {
			this.prefix = prefix;
			if ( prefix != null ) {
				this.prefixedColumns = Arrays.asList( prefixedColumns );
			}
			else {
				this.prefixedColumns = new ArrayList<String>( 0 );
			}
		}

		@Override
		public Set<String> getColumnNames( MapObject row) {
			Set<String> columnNames = new HashSet<>();
			addColumnNames( row, columnNames, "" );
			for ( String prefixedColumn : prefixedColumns ) {
				String unprefixedColumn = removePrefix( prefixedColumn );
				if ( columnNames.contains( unprefixedColumn ) ) {
					columnNames.remove( unprefixedColumn );
					columnNames.add( prefixedColumn );
				}
			}
			return columnNames;
		}

		// only call if you have a prefix
		private String removePrefix(String prefixedColumn) {
			return prefixedColumn.substring( prefix.length() + 1 ); // prefix + "."
		}

		private void addColumnNames( MapObject row, Set<String> columnNames, String prefix) {
			for ( Object field : row.keySet() ) {
				Object sub = row.get( field );
				if ( sub instanceof RethinkDBAssociationRowAccessor ) {
					addColumnNames( (MapObject) sub, columnNames, RethinkHelpers.flatten( prefix, (String)field ) );
				}
				else {
					columnNames.add( RethinkHelpers.flatten( prefix, (String)field ) );
				}
			}
		}

		@Override
		public Object get(MapObject row, String column) {
			if ( prefixedColumns.contains( column ) ) {
				column = removePrefix( column );
			}
			return RethinkHelpers.getValueOrNull( row, column );
		}
	}
}
