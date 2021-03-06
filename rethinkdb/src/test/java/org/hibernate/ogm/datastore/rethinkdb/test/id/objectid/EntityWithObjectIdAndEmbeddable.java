/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.test.id.objectid;

import javax.persistence.Embedded;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import org.hibernate.ogm.backendtck.queries.StoryBranch;

/**
 * @author Davide D'Alto
 */
@Entity
public class EntityWithObjectIdAndEmbeddable {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private String id;

	@Embedded
	private StoryBranch anEmbeddable;

	public EntityWithObjectIdAndEmbeddable() {
	}

	public EntityWithObjectIdAndEmbeddable(StoryBranch anEmbeddable) {
		this.anEmbeddable = anEmbeddable;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public StoryBranch getAnEmbeddable() {
		return anEmbeddable;
	}

	public void setAnEmbeddable(StoryBranch details) {
		this.anEmbeddable = details;
	}
}
