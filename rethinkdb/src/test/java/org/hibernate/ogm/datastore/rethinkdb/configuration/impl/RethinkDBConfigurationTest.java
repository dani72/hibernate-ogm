/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.configuration.impl;

import com.rethinkdb.net.Connection;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.hibernate.boot.registry.classloading.internal.ClassLoaderServiceImpl;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.options.navigation.impl.OptionsContextImpl;
import org.hibernate.ogm.options.navigation.source.impl.OptionValueSources;
import org.hibernate.ogm.options.spi.OptionsContext;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import static org.junit.Assert.assertNotNull;

/**
 * @author Hardy Ferentschik
 */
public class RethinkDBConfigurationTest {

    private Map<String, String> configProperties;
    private ConfigurationPropertyReader propertyReader;
    private OptionsContext globalOptions;

    @Before
    public void setUp() {
        configProperties = new HashMap<>();
        configProperties.put(OgmProperties.DATABASE, "foo");

        propertyReader = new ConfigurationPropertyReader(configProperties, new ClassLoaderServiceImpl());
        globalOptions = OptionsContextImpl.forGlobal(OptionValueSources.getDefaultSources(propertyReader));
    }

    private Connection.Builder createMongoClientOptions() {
        RethinkDBConfiguration mongoConfig = new RethinkDBConfiguration(
                propertyReader,
                globalOptions
        );
        assertNotNull(mongoConfig);

        Connection.Builder clientOptions = mongoConfig.buildOptions();
        assertNotNull(clientOptions);
        return clientOptions;
    }
}
