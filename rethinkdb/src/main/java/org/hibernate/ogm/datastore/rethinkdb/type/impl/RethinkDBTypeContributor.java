/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.type.impl;

import org.hibernate.boot.model.TypeContributions;
import org.hibernate.boot.model.TypeContributor;
import org.hibernate.service.ServiceRegistry;

/**
 * Registers custom types (ORM types, no grid types). Detected via the JDK service loader mechanism.
 *
 * @author Gunnar Morling
 */
public class RethinkDBTypeContributor implements TypeContributor {

	@Override
	public void contribute(TypeContributions typeContributions, ServiceRegistry serviceRegistry) {
	}
}
