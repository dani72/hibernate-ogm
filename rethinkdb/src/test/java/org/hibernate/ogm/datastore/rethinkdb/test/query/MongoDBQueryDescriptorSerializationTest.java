/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.test.query;

import static org.fest.assertions.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import org.hibernate.ogm.datastore.rethinkdb.query.impl.RethinkDBQueryDescriptor;
import org.hibernate.ogm.datastore.rethinkdb.query.impl.RethinkDBQueryDescriptor.Operation;
import org.junit.Test;

/**
 * Tests the serialization and de-serialization of {@link RethinkDBQueryDescriptor}.
 *
 * @author Gunnar Morling
 */
public class MongoDBQueryDescriptorSerializationTest {

	@Test
	public void canSerializeAndDeserialize() throws Exception {
		RethinkDBQueryDescriptor descriptor = new RethinkDBQueryDescriptor(
				"test",
				Operation.FIND,
				com.rethinkdb.RethinkDB.r.hashMap( "foo", "bar" ),
				com.rethinkdb.RethinkDB.r.hashMap( "foo", 1 ),
				com.rethinkdb.RethinkDB.r.hashMap( "bar", 1 ),
				com.rethinkdb.RethinkDB.r.hashMap(),
				com.rethinkdb.RethinkDB.r.hashMap(),
				Arrays.asList( "foo, bar" )
		);

		byte[] bytes = serialize( descriptor );
		RethinkDBQueryDescriptor deserializedDescriptor = deserialize( bytes );

		assertThat( deserializedDescriptor.getCollectionName() ).isEqualTo( descriptor.getCollectionName() );
		assertThat( deserializedDescriptor.getOperation() ).isEqualTo( descriptor.getOperation() );
		assertThat( deserializedDescriptor.getCriteria() ).isEqualTo( descriptor.getCriteria() );
		assertThat( deserializedDescriptor.getProjection() ).isEqualTo( descriptor.getProjection() );
		assertThat( deserializedDescriptor.getOrderBy() ).isEqualTo( descriptor.getOrderBy() );
		assertThat( deserializedDescriptor.getUnwinds() ).isEqualTo( descriptor.getUnwinds() );
	}

	private byte[] serialize(RethinkDBQueryDescriptor descriptor) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream = new ObjectOutputStream( outputStream );
		objectOutputStream.writeObject( descriptor );
		objectOutputStream.close();
		return outputStream.toByteArray();
	}

	private RethinkDBQueryDescriptor deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
		ObjectInputStream inputStream = new ObjectInputStream( new ByteArrayInputStream( bytes ) );
		RethinkDBQueryDescriptor deserializedDescriptor = (RethinkDBQueryDescriptor) inputStream.readObject();
		return deserializedDescriptor;
	}
}
