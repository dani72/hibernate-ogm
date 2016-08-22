/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.options.navigation.impl;

import org.hibernate.ogm.datastore.document.options.navigation.spi.BaseDocumentStoreGlobalContext;
import org.hibernate.ogm.datastore.rethinkdb.options.AssociationDocumentStorageType;
import org.hibernate.ogm.datastore.rethinkdb.options.impl.AssociationDocumentStorageOption;
import org.hibernate.ogm.options.navigation.spi.ConfigurationContext;
import org.hibernate.ogm.util.impl.Contracts;
import org.hibernate.ogm.datastore.rethinkdb.options.navigation.RethinkDBGlobalContext;
import org.hibernate.ogm.datastore.rethinkdb.options.navigation.RethinkDBEntityContext;

/**
 * Converts global MongoDB options.
 *
 * @author Davide D'Alto &lt;davide@hibernate.org&gt;
 * @author Gunnar Morling
 */
public abstract class RethinkDBGlobalContextImpl extends BaseDocumentStoreGlobalContext<RethinkDBGlobalContext, RethinkDBEntityContext> implements
		RethinkDBGlobalContext {

	public RethinkDBGlobalContextImpl(ConfigurationContext context) {
		super( context );
	}

	@Override
	public RethinkDBGlobalContext associationDocumentStorage(AssociationDocumentStorageType associationDocumentStorage) {
		Contracts.assertParameterNotNull( associationDocumentStorage, "associationDocumentStorage" );
		addGlobalOption( new AssociationDocumentStorageOption(), associationDocumentStorage );
		return this;
	}
}
