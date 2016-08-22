/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.impl;

import com.rethinkdb.model.MapObject;
import java.util.List;

import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate;
import org.hibernate.hql.ast.spi.predicate.ComparisonPredicate.Type;
import org.hibernate.hql.ast.spi.predicate.ConjunctionPredicate;
import org.hibernate.hql.ast.spi.predicate.DisjunctionPredicate;
import org.hibernate.hql.ast.spi.predicate.InPredicate;
import org.hibernate.hql.ast.spi.predicate.IsNullPredicate;
import org.hibernate.hql.ast.spi.predicate.LikePredicate;
import org.hibernate.hql.ast.spi.predicate.NegationPredicate;
import org.hibernate.hql.ast.spi.predicate.PredicateFactory;
import org.hibernate.hql.ast.spi.predicate.RangePredicate;
import org.hibernate.hql.ast.spi.predicate.RootPredicate;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl.RethinkDBComparisonPredicate;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl.RethinkDBConjunctionPredicate;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl.RethinkDBDisjunctionPredicate;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl.RethinkDBInPredicate;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl.RethinkDBIsNullPredicate;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl.RethinkDBLikePredicate;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl.RethinkDBNegationPredicate;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl.RethinkDBRangePredicate;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.predicate.impl.RethinkDBRootPredicate;

/**
 * Factory for {@link org.hibernate.hql.ast.spi.predicate.Predicate}s creating MongoDB queries in form of
 * {@link DBObject}s.
 *
 * @author Gunnar Morling
 */
public class RethinkDBPredicateFactory implements PredicateFactory<MapObject> {

	private final RethinkDBPropertyHelper propertyHelper;

	public RethinkDBPredicateFactory(RethinkDBPropertyHelper propertyHelper) {
		this.propertyHelper = propertyHelper;
	}

	@Override
	public RootPredicate<MapObject> getRootPredicate(String entityType) {
		return new RethinkDBRootPredicate();
	}

	@Override
	public ComparisonPredicate<MapObject> getComparisonPredicate(String entityType, Type comparisonType, List<String> propertyPath, Object value) {
		String columnName = columnName( entityType, propertyPath );
		return new RethinkDBComparisonPredicate( columnName, comparisonType, value );
	}

	@Override
	public RangePredicate<MapObject> getRangePredicate(String entityType, List<String> propertyPath, Object lowerValue, Object upperValue) {
		String columnName = columnName( entityType, propertyPath );
		return new RethinkDBRangePredicate( columnName, lowerValue, upperValue );
	}

	@Override
	public NegationPredicate<MapObject> getNegationPredicate() {
		return new RethinkDBNegationPredicate();
	}

	@Override
	public DisjunctionPredicate<MapObject> getDisjunctionPredicate() {
		return new RethinkDBDisjunctionPredicate();
	}

	@Override
	public ConjunctionPredicate<MapObject> getConjunctionPredicate() {
		return new RethinkDBConjunctionPredicate();
	}

	@Override
	public InPredicate<MapObject> getInPredicate(String entityType, List<String> propertyPath, List<Object> typedElements) {
		String columnName = columnName( entityType, propertyPath );
		return new RethinkDBInPredicate( columnName, typedElements );
	}

	@Override
	public IsNullPredicate<MapObject> getIsNullPredicate(String entityType, List<String> propertyPath) {
		String columnName = columnName( entityType, propertyPath );
		return new RethinkDBIsNullPredicate( columnName );
	}

	@Override
	public LikePredicate<MapObject> getLikePredicate(String entityType, List<String> propertyPath, String patternValue, Character escapeCharacter) {
		String columnName = columnName( entityType, propertyPath );
		return new RethinkDBLikePredicate( columnName, patternValue, escapeCharacter );
	}

	private String columnName(String entityType, List<String> propertyPath) {
		return propertyHelper.getColumnName( entityType, propertyPath );
	}
}
