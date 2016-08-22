/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl;

import com.rethinkdb.model.MapObject;
import org.hibernate.hql.ast.spi.predicate.RootPredicate;

/**
 * MongoDB-based implementation of {@link RootPredicate}.
 *
 * @author Gunnar Morling
 */
public class RethinkDBRootPredicate extends RootPredicate<MapObject> {

	@Override
	public MapObject getQuery() {
		return child == null ? com.rethinkdb.RethinkDB.r.hashMap() : child.getQuery();
	}
}
