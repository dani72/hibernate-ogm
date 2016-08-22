/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl;

import org.hibernate.hql.ast.spi.predicate.NegatablePredicate;
import org.hibernate.hql.ast.spi.predicate.NegationPredicate;

import com.rethinkdb.model.MapObject;

/**
 * MongoDB-based implementation of {@link NegationPredicate}.
 *
 * @author Gunnar Morling
 */
public class RethinkDBNegationPredicate extends NegationPredicate<MapObject> implements NegatablePredicate<MapObject> {

	@Override
	public MapObject getQuery() {
		return ( (NegatablePredicate<MapObject>) getChild() ).getNegatedQuery();
	}

	@Override
	public MapObject getNegatedQuery() {
		return getChild().getQuery();
	}
}
