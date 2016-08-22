/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.test.id.objectid;

import java.util.HashSet;
import java.util.Set;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;

/**
 * @author Gunnar Morling
 */
@Entity
public class MusicGenre {

	private String id;
	private String name;
	private Set<Bar> playedIn = new HashSet<>();

	MusicGenre() {
	}

	MusicGenre(String name) {
		this.name = name;
	}

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
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

	@OneToMany(mappedBy = "musicGenre")
	public Set<Bar> getPlayedIn() {
		return playedIn;
	}

	public void setPlayedIn(Set<Bar> playedIn) {
		this.playedIn = playedIn;
	}
}