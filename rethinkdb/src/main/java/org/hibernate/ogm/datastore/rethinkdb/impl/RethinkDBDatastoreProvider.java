/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.impl;

import com.rethinkdb.RethinkDB;
import java.util.Map;
import com.rethinkdb.gen.ast.Db;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Connection.Builder;
import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.ogm.datastore.rethinkdb.RethinkDBDialect;
import org.hibernate.ogm.datastore.rethinkdb.configuration.impl.RethinkDBConfiguration;
import org.hibernate.ogm.datastore.rethinkdb.logging.impl.Log;
import org.hibernate.ogm.datastore.rethinkdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.rethinkdb.query.parsing.impl.RethinkDBBasedQueryParserService;
import org.hibernate.ogm.datastore.spi.BaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.SchemaDefiner;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.options.spi.OptionsService;
import org.hibernate.ogm.query.spi.QueryParserService;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

/**
 * Provides access to a MongoDB instance
 *
 * @author Guillaume Scheibel &lt;guillaume.scheibel@gmail.com&gt;
 * @author Gunnar Morling
 */
public class RethinkDBDatastoreProvider extends BaseDatastoreProvider implements Startable, Stoppable, Configurable, ServiceRegistryAwareService {

    private static final Log log = LoggerFactory.getLogger();

    private ServiceRegistryImplementor serviceRegistry;

    private Connection connection;
    private Db database;
    private RethinkDBConfiguration config;

    public RethinkDBDatastoreProvider() {
    }

    /**
     * Only used in tests.
     *
     * @param connection the client to connect to rethinkdb
     */
    public RethinkDBDatastoreProvider(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void configure(Map configurationValues) {
        OptionsService optionsService = serviceRegistry.getService(OptionsService.class);
        ClassLoaderService classLoaderService = serviceRegistry.getService(ClassLoaderService.class);
        ConfigurationPropertyReader propertyReader = new ConfigurationPropertyReader(configurationValues, classLoaderService);

        try {
            this.config = new RethinkDBConfiguration(propertyReader, optionsService.context().getGlobalOptions());
        } catch (Exception e) {
            // Wrap Exception in a ServiceException to make the stack trace more friendly
            // Otherwise a generic unable to request service is thrown
            throw log.unableToConfigureDatastoreProvider(e);
        }
    }

    @Override
    public void injectServices(ServiceRegistryImplementor serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    public Class<? extends GridDialect> getDefaultDialect() {
        return RethinkDBDialect.class;
    }

    @Override
    public Class<? extends QueryParserService> getDefaultQueryParserServiceType() {
        return RethinkDBBasedQueryParserService.class;
    }

    @Override
    public Class<? extends SchemaDefiner> getSchemaDefinerType() {
        return RethinkDBSchemaDefiner.class;
    }

    @Override
    public boolean allowsTransactionEmulation() {
        return true;
    }

    @Override
    public void start() {
        try {
            if (connection == null) {
                connection = createRethinkdbClient(config);
            }
            database = extractDatabase( connection, config);
        } catch (Exception e) {
            // Wrap Exception in a ServiceException to make the stack trace more friendly
            // Otherwise a generic unable to request service is thrown
            throw log.unableToStartDatastoreProvider(e);
        }
    }

    protected Connection createRethinkdbClient(RethinkDBConfiguration config) {
        Builder options = config.buildOptions();
        
        return options.connect();
    }

    @Override
    public void stop() {
        log.disconnectingFromMongo();
        connection.close();
    }

    public Db getDatabase() {
        return database;
    }

    public Connection getConnection() {
        return connection;
    }
    
    private Db extractDatabase(Connection connection, RethinkDBConfiguration config) {
        String databaseName = config.getDatabaseName();
        log.connectingToMongoDatabase(databaseName);

        return RethinkDB.r.db( databaseName);
    }
}
