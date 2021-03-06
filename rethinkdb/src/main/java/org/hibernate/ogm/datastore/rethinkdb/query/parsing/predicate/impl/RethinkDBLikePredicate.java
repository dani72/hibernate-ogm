/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl;

import java.util.regex.Pattern;

import org.hibernate.hql.ast.spi.predicate.LikePredicate;
import org.hibernate.hql.ast.spi.predicate.NegatablePredicate;
import org.hibernate.ogm.util.parser.impl.LikeExpressionToRegExpConverter;
import com.rethinkdb.model.MapObject;

/**
 * MongoDB-based implementation of {@link LikePredicate}.
 *
 * @author Gunnar Morling
 */
public class RethinkDBLikePredicate extends LikePredicate<MapObject> implements NegatablePredicate<MapObject> {

	private final Pattern pattern;

	public RethinkDBLikePredicate(String propertyName, String patternValue, Character escapeCharacter) {
		super( propertyName, patternValue, escapeCharacter );

		LikeExpressionToRegExpConverter converter = new LikeExpressionToRegExpConverter( escapeCharacter );
		pattern = converter.getRegExpFromLikeExpression( patternValue );
	}

	@Override
	public MapObject getQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap(propertyName, pattern );
	}

	@Override
	public MapObject getNegatedQuery() {
		return com.rethinkdb.RethinkDB.r.hashMap( propertyName, com.rethinkdb.RethinkDB.r.hashMap( "$not", pattern ) );
	}
}
