/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl;

import org.hibernate.hql.ast.spi.predicate.IsNullPredicate;
import org.hibernate.hql.ast.spi.predicate.NegatablePredicate;
import com.rethinkdb.model.MapObject;

/**
 * MongoDB-based implementation of {@link IsNullPredicate}.
 *
 * @author Gunnar Morling
 */
public class RethinkDBIsNullPredicate extends IsNullPredicate<MapObject> implements NegatablePredicate<MapObject> {

	public RethinkDBIsNullPredicate(String propertyName) {
		super( propertyName );
	}

	@Override
	public MapObject getQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$exists", false ) );
	}

	@Override
	public MapObject getNegatedQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$exists", true ) );
	}
}
