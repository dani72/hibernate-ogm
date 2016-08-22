/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.options.navigation;

import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.document.options.navigation.DocumentStoreEntityContext;
import org.hibernate.ogm.datastore.rethinkdb.options.AssociationDocumentStorageType;

/**
 * Allows to configure MongoDB-specific options applying on a global level. These options may be overridden for single
 * properties.
 *
 * @author Davide D'Alto &lt;davide@hibernate.org&gt;
 * @author Gunnar Morling
 */
public interface RethinkDBEntityContext extends DocumentStoreEntityContext<RethinkDBEntityContext, RethinkDBPropertyContext> {

	/**
	 * Specifies how association documents should be persisted. Only applies when the association storage strategy is
	 * set to {@link AssociationStorageType#ASSOCIATION_DOCUMENT}.
	 *
	 * @param associationDocumentStorage the association document type to be used when not configured the property level
	 * @return this context, allowing for further fluent API invocations
	 */
	RethinkDBEntityContext associationDocumentStorage(AssociationDocumentStorageType associationDocumentStorage);
}
