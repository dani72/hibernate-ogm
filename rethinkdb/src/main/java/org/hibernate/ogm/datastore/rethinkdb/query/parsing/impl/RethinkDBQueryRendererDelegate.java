/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.impl;

import com.rethinkdb.model.MapObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.SingleEntityQueryBuilder;
import org.hibernate.hql.ast.spi.SingleEntityQueryRendererDelegate;
import org.hibernate.ogm.persister.impl.OgmEntityPersister;
import org.hibernate.ogm.util.impl.StringHelper;

/**
 * Parser delegate which creates MongoDB queries in form of {@link DBObject}s.
 *
 * @author Gunnar Morling
 */
public class RethinkDBQueryRendererDelegate extends SingleEntityQueryRendererDelegate<MapObject, RethinkDBQueryParsingResult> {

	private final SessionFactoryImplementor sessionFactory;
	private final RethinkDBPropertyHelper propertyHelper;
	private MapObject orderBy;
	/*
	 * The fields for which needs to be aggregated using $unwind when running the query
	 */
	private List<String> unwinds;

	public RethinkDBQueryRendererDelegate(SessionFactoryImplementor sessionFactory, EntityNamesResolver entityNames, RethinkDBPropertyHelper propertyHelper, Map<String, Object> namedParameters) {
		super(
				propertyHelper,
				entityNames,
				SingleEntityQueryBuilder.getInstance( new RethinkDBPredicateFactory( propertyHelper ), propertyHelper ),
				namedParameters );

		this.sessionFactory = sessionFactory;
		this.propertyHelper = propertyHelper;
	}

	@Override
	public RethinkDBQueryParsingResult getResult() {
		OgmEntityPersister entityPersister = (OgmEntityPersister) sessionFactory.getEntityPersister( targetType.getName() );

		return new RethinkDBQueryParsingResult(
				targetType,
				entityPersister.getTableName(),
				builder.build(),
				getProjectionDBObject(),
				orderBy,
				unwinds
		);
	}

	@Override
	public void setPropertyPath(PropertyPath propertyPath) {
		if ( status == Status.DEFINING_SELECT ) {
			List<String> pathWithoutAlias = resolveAlias( propertyPath );
			if ( propertyHelper.isSimpleProperty( pathWithoutAlias ) ) {
				projections.add( propertyHelper.getColumnName( targetTypeName, propertyPath.getNodeNamesWithoutAlias() ) );
			}
			else if ( propertyHelper.isNestedProperty( pathWithoutAlias ) ) {
				if ( propertyHelper.isEmbeddedProperty( targetTypeName, pathWithoutAlias ) ) {
					String columnName = propertyHelper.getColumnName( targetTypeName, pathWithoutAlias );
					projections.add( columnName );
					List<String> associationPath = propertyHelper.findAssociationPath( targetTypeName, pathWithoutAlias );
					// Currently, it is possible to nest only one association inside an embedded
					if ( associationPath != null ) {
						if ( unwinds == null ) {
							unwinds = new ArrayList<String>();
						}
						String field = StringHelper.join( associationPath, "." );
						if ( !unwinds.contains( field ) ) {
							unwinds.add( field );
						}
					}
				}
				else {
					throw new UnsupportedOperationException( "Selecting associated properties not yet implemented." );
				}
			}
		}
		else {
			this.propertyPath = propertyPath;
		}
	}

	/**
	 * Returns the projection columns of the parsed query in form of a {@code DBObject} as expected by MongoDB.
	 *
	 * @return a {@code DBObject} representing the projections of the query
	 */
	private MapObject getProjectionDBObject() {
		if ( projections.isEmpty() ) {
			return null;
		}

		MapObject projectionDBObject = com.rethinkdb.RethinkDB.r.hashMap();

		for ( String projection : projections ) {
			projectionDBObject.put( projection, 1 );
		}

		return projectionDBObject;
	}

	@Override
	protected void addSortField(PropertyPath propertyPath, String collateName, boolean isAscending) {
		if ( orderBy == null ) {
			orderBy = com.rethinkdb.RethinkDB.r.hashMap();
		}

		String columnName = propertyHelper.getColumnName( targetType, propertyPath.getNodeNamesWithoutAlias() );

		// BasicDBObject is essentially a LinkedHashMap, so in case of several sort keys they'll be evaluated in the
		// order they're inserted here, which is the order within the original statement
		orderBy.put( columnName, isAscending ? 1 : -1 );
	}
}
