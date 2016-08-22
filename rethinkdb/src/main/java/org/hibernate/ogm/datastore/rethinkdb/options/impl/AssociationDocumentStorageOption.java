/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.options.impl;

import org.hibernate.ogm.datastore.rethinkdb.RethinkDBProperties;
import org.hibernate.ogm.datastore.rethinkdb.options.AssociationDocumentStorageType;
import org.hibernate.ogm.options.spi.UniqueOption;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;

/**
 * Specifies whether association documents should be stored in a separate collection per association type or in one
 * global collection for all associations.
 *
 * @author Gunnar Morling
 */
public class AssociationDocumentStorageOption extends UniqueOption<AssociationDocumentStorageType> {

	@Override
	public AssociationDocumentStorageType getDefaultValue(ConfigurationPropertyReader propertyReader) {
		return propertyReader.property( RethinkDBProperties.ASSOCIATION_DOCUMENT_STORAGE, AssociationDocumentStorageType.class )
				.withDefault( AssociationDocumentStorageType.GLOBAL_COLLECTION )
				.getValue();
	}
}
