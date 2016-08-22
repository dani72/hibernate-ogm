/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.test.lifecycle;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * @author Gunnar Morling
 */
@Entity
public class BarKeeper {

	private String id;
	private String name;

	BarKeeper() {
	}

	BarKeeper(String id, String name) {
		this.id = id;
		this.name = name;
	}

	@Id
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
