/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl;

import com.rethinkdb.model.MapObject;
import java.util.Arrays;

import org.hibernate.hql.ast.spi.predicate.NegatablePredicate;
import org.hibernate.hql.ast.spi.predicate.RangePredicate;

/**
 * MongoDB-based implementation of {@link RangePredicate}.
 *
 * @author Gunnar Morling
 */
public class RethinkDBRangePredicate extends RangePredicate<MapObject> implements NegatablePredicate<MapObject> {

	public RethinkDBRangePredicate(String propertyName, Object lower, Object upper) {
		super( propertyName, lower, upper );
	}

	@Override
	public MapObject getQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap(
				"$and",
				Arrays.<MapObject>asList(
						com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$gte", lower ) ),
						com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$lte", upper ) )
				)
		);
	}

	@Override
	public MapObject getNegatedQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap(
				"$or",
				Arrays.<MapObject>asList(
						com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$lt", lower ) ),
						com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$gt", upper ) )
				)
		);
	}
}
