/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.impl;

import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.hql.QueryParser;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.ogm.datastore.rethinkdb.logging.impl.Log;
import org.hibernate.ogm.datastore.rethinkdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.query.spi.BaseQueryParserService;
import org.hibernate.ogm.query.spi.QueryParserService;
import org.hibernate.ogm.query.spi.QueryParsingResult;
import org.hibernate.ogm.service.impl.SessionFactoryEntityNamesResolver;

/**
 * A {@link QueryParserService} implementation which creates MongoDB queries in form of {@link DBObject}s.
 *
 * @author Gunnar Morling
 */
public class RethinkDBBasedQueryParserService extends BaseQueryParserService {

	private static final Log log = LoggerFactory.getLogger();

	private volatile SessionFactoryEntityNamesResolver entityNamesResolver;

	@Override
	public QueryParsingResult parseQuery(SessionFactoryImplementor sessionFactory, String queryString, Map<String, Object> namedParameters) {
		QueryParser queryParser = new QueryParser();
		RethinkDBProcessingChain processingChain = createProcessingChain( sessionFactory, unwrap( namedParameters ) );

		RethinkDBQueryParsingResult result = queryParser.parseQuery( queryString, processingChain );
		log.createdQuery( queryString, result );

		return result;
	}

	@Override
	public QueryParsingResult parseQuery(SessionFactoryImplementor sessionFactory, String queryString) {
		throw new UnsupportedOperationException( "MongoDB does not support parameterized queries. Parameter values " +
				"must be passed to the query parser." );
	}

	@Override
	public boolean supportsParameters() {
		return false;
	}

	private RethinkDBProcessingChain createProcessingChain(SessionFactoryImplementor sessionFactory, Map<String, Object> namedParameters) {
		EntityNamesResolver entityNamesResolver = getDefinedEntityNames( sessionFactory );

		return new RethinkDBProcessingChain(
				sessionFactory,
				entityNamesResolver,
				namedParameters );
	}

	private EntityNamesResolver getDefinedEntityNames(SessionFactory sessionFactory) {
		if ( entityNamesResolver == null ) {
			entityNamesResolver = new SessionFactoryEntityNamesResolver( sessionFactory );
		}
		return entityNamesResolver;
	}
}
