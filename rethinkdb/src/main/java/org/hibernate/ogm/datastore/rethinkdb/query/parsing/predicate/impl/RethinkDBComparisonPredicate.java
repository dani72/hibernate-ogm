/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl;

import com.rethinkdb.model.MapObject;
import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate;
import org.hibernate.hql.ast.spi.predicate.NegatablePredicate;

/**
 * MongoDB-based implementation of {@link ComparisonPredicate}.
 *
 * @author Gunnar Morling
 */
public class RethinkDBComparisonPredicate extends ComparisonPredicate<MapObject> implements NegatablePredicate<MapObject> {

	public RethinkDBComparisonPredicate(String propertyName, ComparisonPredicate.Type comparisonType, Object value) {
		super( propertyName, comparisonType, value );
	}

	@Override
	protected MapObject getStrictlyLessQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$lt", value ) );
	}

	@Override
	protected MapObject getLessOrEqualsQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$lte", value ) );
	}

	@Override
	protected MapObject getEqualsQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap( propertyName, value );
	}

	@Override
	protected MapObject getGreaterOrEqualsQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$gte", value ) );
	}

	@Override
	protected MapObject getStrictlyGreaterQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$gt", value ) );
	}

	@Override
	public MapObject getNegatedQuery() {
		switch ( type ) {
			case LESS:
				return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$gte", value ) );
			case LESS_OR_EQUAL:
				return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$gt", value ) );
			case EQUALS:
				return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$ne", value ) );
			case GREATER_OR_EQUAL:
				return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$lt", value ) );
			case GREATER:
				return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$lte", value ) );
			default:
				throw new UnsupportedOperationException( "Unsupported comparison type: " + type );
		}
	}
}
