/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.options.navigation;

import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.document.options.navigation.DocumentStorePropertyContext;
import org.hibernate.ogm.datastore.rethinkdb.options.AssociationDocumentStorageType;

/**
 * Allows to configure MongoDB-specific options for a single property.
 *
 * @author Davide D'Alto &lt;davide@hibernate.org&gt;
 * @author Gunnar Morling
 */
public interface RethinkDBPropertyContext extends DocumentStorePropertyContext<RethinkDBEntityContext, RethinkDBPropertyContext> {

	/**
	 * Specifies how association documents should be persisted. Only applies when the current property represents an
	 * association and the association storage strategy is set to {@link AssociationStorageType#ASSOCIATION_DOCUMENT}.
	 *
	 * @param associationDocumentStorage the association document type to be used; overrides any settings on the entity
	 * or global level
	 * @return this context, allowing for further fluent API invocations
	 */
	RethinkDBPropertyContext associationDocumentStorage(AssociationDocumentStorageType associationDocumentStorage);
}
