/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.hql.ast.spi.predicate.DisjunctionPredicate;
import org.hibernate.hql.ast.spi.predicate.NegatablePredicate;
import org.hibernate.hql.ast.spi.predicate.Predicate;
import com.rethinkdb.model.MapObject;

/**
 * MongoDB-based implementation of {@link DisjunctionPredicate}.
 *
 * @author Gunnar Morling
 */
public class RethinkDBDisjunctionPredicate extends DisjunctionPredicate<MapObject> implements NegatablePredicate<MapObject> {

	@Override
	public MapObject getQuery() {
		List<MapObject> elements = new ArrayList<MapObject>();

		for ( Predicate<MapObject> child : children ) {
			elements.add( child.getQuery() );
		}

		return com.rethinkdb.RethinkDB.r.hashMap("$or", elements);
	}

	@Override
	public MapObject getNegatedQuery() {
		List<MapObject> elements = new ArrayList<MapObject>();

		for ( Predicate<MapObject> child : children ) {
			elements.add( ( (NegatablePredicate<MapObject>) child ).getNegatedQuery() );
		}

		return com.rethinkdb.RethinkDB.r.hashMap("$and", elements);
	}
}
