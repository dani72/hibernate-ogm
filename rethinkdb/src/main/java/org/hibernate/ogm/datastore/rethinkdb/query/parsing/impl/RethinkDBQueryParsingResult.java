/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.impl;

import com.rethinkdb.model.MapObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.hibernate.ogm.datastore.rethinkdb.query.impl.RethinkDBQueryDescriptor;
import org.hibernate.ogm.datastore.rethinkdb.query.impl.RethinkDBQueryDescriptor.Operation;
import org.hibernate.ogm.query.spi.QueryParsingResult;

/**
 * The result of walking a query parse tree using a {@link MongoDBQueryRendererDelegate}.
 *
 * @author Gunnar Morling
 */
public class RethinkDBQueryParsingResult implements QueryParsingResult {

	private final Class<?> entityType;
	private final String collectionName;
	private final MapObject query;
	private final MapObject projection;
	private final MapObject orderBy;
	private final List<String> unwinds;

	public RethinkDBQueryParsingResult(Class<?> entityType, String collectionName, MapObject query, MapObject projection, MapObject orderBy, List<String> unwinds) {
		this.entityType = entityType;
		this.collectionName = collectionName;
		this.query = query;
		this.projection = projection;
		this.orderBy = orderBy;
		this.unwinds = unwinds;
	}

	public MapObject getQuery() {
		return query;
	}

	public Class<?> getEntityType() {
		return entityType;
	}

	public MapObject getProjection() {
		return projection;
	}

	public MapObject getOrderBy() {
		return orderBy;
	}

	public List<String> getUnwinds() {
		return unwinds;
	}

	@Override
	public Object getQueryObject() {
		return new RethinkDBQueryDescriptor(
			collectionName,
			unwinds == null ? Operation.FIND : Operation.AGGREGATE,
			query,
			projection,
			orderBy,
			null,
			null,
			unwinds
		);
	}

	@Override
	public List<String> getColumnNames() {
		//TODO Non-scalar case
		return projection != null ? new ArrayList<String>( projection.keySet() ) : Collections.<String>emptyList();
	}

	@Override
	public String toString() {
		return "MongoDBQueryParsingResult [entityType=" + entityType.getSimpleName() + ", query=" + query + ", projection=" + projection + "]";
	}
}
