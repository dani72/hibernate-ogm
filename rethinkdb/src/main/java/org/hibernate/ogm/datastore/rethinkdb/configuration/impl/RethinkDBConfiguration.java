/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.rethinkdb.configuration.impl;

import com.rethinkdb.RethinkDB;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Connection.Builder;
import org.hibernate.ogm.cfg.spi.DocumentStoreConfiguration;
import org.hibernate.ogm.datastore.rethinkdb.RethinkDBProperties;
import org.hibernate.ogm.datastore.rethinkdb.impl.RethinkDBDatastoreProvider;
import org.hibernate.ogm.datastore.rethinkdb.logging.impl.Log;
import org.hibernate.ogm.datastore.rethinkdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.options.spi.OptionsContext;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;

/**
 * Configuration for {@link RethinkDBDatastoreProvider}.
 *
 * @author Guillaume Scheibel &lt;guillaume.scheibel@gmail.com&gt;
 * @author Gunnar Morling
 * @author Hardy Ferentschik
 */
public class RethinkDBConfiguration extends DocumentStoreConfiguration {

    public static final String DEFAULT_ASSOCIATION_STORE = "Associations";

    private static final int DEFAULT_PORT = 27017;
    private static final Log log = LoggerFactory.getLogger();

    private final ConfigurationPropertyReader propertyReader;

    /**
     * Creates a new {@link MongoDBConfiguration}.
     *
     * @param propertyReader provides access to configuration values given via
     * {@code persistence.xml} etc.
     * @param globalOptions global settings given via an option configurator
     */
    public RethinkDBConfiguration(ConfigurationPropertyReader propertyReader, OptionsContext globalOptions) {
        super(propertyReader, DEFAULT_PORT);

        this.propertyReader = propertyReader;
    }

    /**
     * Create a {@link MongoClientOptions} using the
     * {@link RethinkDBConfiguration}.
     *
     * @return the {@link MongoClientOptions} corresponding to the
     * {@link RethinkDBConfiguration}
     */
    public Connection.Builder buildOptions() {
        Builder builder = RethinkDB.r.connection();

        Map<String, Method> settingsMap = createSettingsMap();
        for (Map.Entry<String, Method> entry : settingsMap.entrySet()) {
            String setting = RethinkDBProperties.RETHINKDB_DRIVER_SETTINGS_PREFIX + "." + entry.getKey();
            // we know that there is exactly one parameter
            Class<?> type = entry.getValue().getParameterTypes()[0];

            // for reflection purposes we need to deal with wrapper classes
            if (int.class.equals(type)) {
                type = Integer.class;
            }
            if (boolean.class.equals(type)) {
                type = Boolean.class;
            }

            Object property = propertyReader.property(setting, type).withDefault(null).getValue();
            if (property == null) {
                continue;
            }

            Method settingMethod = entry.getValue();
            try {
                settingMethod.invoke(builder, property);
            } catch (InvocationTargetException | IllegalAccessException e) {
                throw log.unableToInvokeMethodViaReflection(
                        settingMethod.getDeclaringClass().getName(),
                        settingMethod.getName()
                );
            }
        }

        return builder;
    }

    private Map<String, Method> createSettingsMap() {
        Map<String, Method> settingsMap = new HashMap<>();

        Method[] methods = Builder.class.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getParameterTypes().length == 1) {
                Class<?> parameterType = method.getParameterTypes()[0];
                // we just care of string, int and boolean setters
                if (String.class.equals(parameterType)
                        || int.class.equals(parameterType)
                        || boolean.class.equals(parameterType)) {
                    settingsMap.put(method.getName(), method);
                }
            }
        }

        return settingsMap;
    }
}
