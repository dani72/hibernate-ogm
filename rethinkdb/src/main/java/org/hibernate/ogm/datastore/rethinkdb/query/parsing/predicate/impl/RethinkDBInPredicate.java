/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl;

import com.rethinkdb.model.MapObject;
import java.util.List;
import org.hibernate.hql.ast.spi.predicate.InPredicate;
import org.hibernate.hql.ast.spi.predicate.NegatablePredicate;

/**
 * MongoDB-based implementation of {@link InPredicate}.
 *
 * @author Gunnar Morling
 */
public class RethinkDBInPredicate extends InPredicate<MapObject> implements NegatablePredicate<MapObject> {

	public RethinkDBInPredicate(String propertyName, List<Object> values) {
		super( propertyName, values );
	}

	@Override
	public MapObject getQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap(propertyName, com.rethinkdb.RethinkDB.r.hashMap("$in", values ) );
	}

	@Override
	public MapObject getNegatedQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$nin", values ) );
	}
}
