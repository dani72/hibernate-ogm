/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.query.parsing.nativequery.impl;

import com.rethinkdb.model.MapObject;
import org.hibernate.ogm.datastore.rethinkdb.query.impl.RethinkDBQueryDescriptor;
import org.hibernate.ogm.datastore.rethinkdb.query.impl.RethinkDBQueryDescriptor.Operation;
import org.hibernate.ogm.util.impl.StringHelper;

/**
 * Builder for {@link RethinkDBQueryDescriptor}s.
 *
 * @author Gunnar Morling
 * @author Thorsten Möller
 * @author Guillaume Smet
 */
public class RethinkDBQueryDescriptorBuilder {

	private String collection;
	private Operation operation;
	/**
	 * Overloaded to be the 'document' for a FINDANDMODIFY query (which is a kind of criteria),
	 */
	private String criteria;
	private String projection;
	private String orderBy;
	/**
	 * Document or array of documents to insert/update for an INSERT/UPDATE query.
	 */
	private String updateOrInsert;
	private String options;

	public boolean setCollection(String collection) {
		this.collection = collection.trim();
		return true;
	}

	public boolean setOperation(Operation operation) {
		this.operation = operation;
		return true;
	}

	public boolean setCriteria(String criteria) {
		this.criteria = criteria;
		return true;
	}

	public boolean setProjection(String projection) {
		this.projection = projection;
		return true;
	}

	public boolean setOrderBy(String orderBy) {
		this.orderBy = orderBy;
		return true;
	}

	public boolean setOptions(String options) {
		this.options = options;
		return true;
	}

	public boolean setUpdateOrInsert(String updateOrInsert) {
		this.updateOrInsert = updateOrInsert;
		return true;
	}

	public RethinkDBQueryDescriptor build() {
		return new RethinkDBQueryDescriptor(
				collection,
				operation,
				parse( criteria ),
				parse( projection ),
				parse( orderBy ),
				parse( options ),
				parse( updateOrInsert ),
				null );
	}

	/**
	 * Currently, there is no way to parse an array while supporting BSON and JSON extended syntax. So for now, we build
	 * an object from the JSON string representing an array or an object, parse this object then extract the array/object.
	 *
	 * See <a href="https://jira.mongodb.org/browse/JAVA-2186">https://jira.mongodb.org/browse/JAVA-2186</a>.
	 *
	 * @param json a JSON string representing an array or an object
	 * @return a {@code DBObject} representing the array ({@code BasicDBList}) or the object ({@code BasicDBObject})
	 */
	public MapObject parse(String json) {
		if ( StringHelper.isNullOrEmptyString( json ) ) {
			return null;
		}
		MapObject object = null; //??? BasicDBObject.parse( "{ 'json': " + json + "}" );
		return (MapObject) object.get( "json" );
	}

}
