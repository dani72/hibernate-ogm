/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb;

import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.ogm.datastore.document.cfg.DocumentStoreProperties;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;

/**
 * Properties for configuring the MongoDB datastore via {@code persistence.xml} or {@link StandardServiceRegistryBuilder}.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 * @author Gunnar Morling
 */
public final class RethinkDBProperties implements DocumentStoreProperties {

	/**
	 * Configuration property for defining the acknowledgement of write operations. Supported values are the
	 * {@link WriteConcernType} enum or the String representations of its constants.
	 * <p>
	 * Specify {@link WriteConcernType#CUSTOM} in conjunction with {@link #WRITE_CONCERN_TYPE} to specify a custom
	 * {@link WriteConcern} implementation.
	 * <p>
	 * Defaults to {@link WriteConcernType#ACKNOWLEDGED}.
	 *
	 * @see WriteConcern
	 */
	public static final String WRITE_CONCERN = "hibernate.ogm.rethinkdb.write_concern";

	/**
	 * Configuration property for setting a custom {@link WriteConcern} implementation. Can be given as fully-qualified
	 * class name, class-object or instance of the implementation type. If not given as instance, the specified type
	 * must have a default (no-args) constructor.
	 * <p>
	 * Only takes affect if {@link #WRITE_CONCERN} is set to {@link WriteConcernType#CUSTOM}.
	 */
	public static final String WRITE_CONCERN_TYPE = "hibernate.ogm.rethinkdb.write_concern_type";

	/**
	 * Configuration property for setting the read preference. Supported values are the {@link ReadPreferenceType} enum
	 * or the String representations of its constants.
	 * <p>
	 * Defaults to {@link ReadPreferenceType#PRIMARY}.
	 *
	 * @see ReadPreference
	 */
	public static final String READ_PREFERENCE = "hibernate.ogm.rethinkdb.read_preference";

	/**
	 * Configuration property for specifying how to store association documents. Only applicable if
	 * {@link DocumentStoreProperties#ASSOCIATIONS_STORE} is set to {@link AssociationStorageType#ASSOCIATION_DOCUMENT}.
	 * Supported values are the {@link org.hibernate.ogm.datastore.rethinkdb.options.AssociationDocumentStorageType} enum or the String representations of its constants.
	 * Defaults to {@link org.hibernate.ogm.datastore.mongodb.options.AssociationDocumentStorageType#GLOBAL_COLLECTION}.
	 */
	public static final String ASSOCIATION_DOCUMENT_STORAGE = "hibernate.ogm.rethinkdb.association_document_storage";

	/**
	 * Specify the authentication mechanism that MongoDB will use to authenticate the connection.
	 * Possible values are listed in {@link org.hibernate.ogm.datastore.rethinkdb.options.AuthenticationMechanismType}.
	 *
	 * {@code BEST} (default) will handshake with the server to find the best authentication mechanism.
	 *
	 * @see com.mongodb.MongoCredential
	 */
	public static final String AUTHENTICATION_MECHANISM = "hibernate.ogm.rethinkdb.authentication_mechanism";

	/**
	 * Property prefix for MongoDB driver settings which needs to be passed on to the driver. Refer to
	 * the options of {@link com.mongodb.MongoClientOptions.Builder} for a list of available properties.
	 * All string, int and boolean builder methods can be configured, eg {@code hibernate.ogm.mongodb.driver.maxWaitTime = 1000}.
	 */
	public static final String RETHINKDB_DRIVER_SETTINGS_PREFIX = "hibernate.ogm.rethinkdb.driver";

	private RethinkDBProperties() {
	}
}
