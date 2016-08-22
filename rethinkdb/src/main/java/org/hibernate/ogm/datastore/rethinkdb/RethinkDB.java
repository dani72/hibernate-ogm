/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb;

import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.rethinkdb.options.navigation.impl.RethinkDBEntityContextImpl;
import org.hibernate.ogm.datastore.rethinkdb.options.navigation.impl.RethinkDBGlobalContextImpl;
import org.hibernate.ogm.datastore.rethinkdb.options.navigation.impl.RethinkDBPropertyContextImpl;
import org.hibernate.ogm.datastore.spi.DatastoreConfiguration;
import org.hibernate.ogm.options.navigation.spi.ConfigurationContext;
import org.hibernate.ogm.datastore.rethinkdb.options.navigation.RethinkDBGlobalContext;

/**
 * Allows to configure options specific to the RethinkDB document data store.
 *
 * @author Gunnar Morling
 */
public class RethinkDB implements DatastoreConfiguration<RethinkDBGlobalContext> {

    /**
     * Short name of this data store provider.
     *
     * @see OgmProperties#DATASTORE_PROVIDER
     */
    public static final String DATASTORE_PROVIDER_NAME = "RETHINKDB";

    @Override
    public RethinkDBGlobalContext getConfigurationBuilder(ConfigurationContext context) {
        return context.createGlobalContext(RethinkDBGlobalContextImpl.class, RethinkDBEntityContextImpl.class, RethinkDBPropertyContextImpl.class);
    }
}
