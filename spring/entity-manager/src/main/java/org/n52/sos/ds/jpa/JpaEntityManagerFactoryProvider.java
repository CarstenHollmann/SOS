package org.n52.sos.ds.jpa;

import javax.inject.Inject;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.cfg.AvailableSettings;
import org.n52.iceland.ds.ConnectionProviderException;
import org.n52.iceland.ds.DataConnectionProvider;
import org.n52.janmayen.lifecycle.Constructable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JpaEntityManagerFactoryProvider implements DataConnectionProvider, Constructable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JpaEntityManagerFactoryProvider.class);

    @Inject
    private DatabaseConfig databaseConfig;
    private int maxConnections;


    @Override
    public Object getConnection()
            throws ConnectionProviderException {
        return databaseConfig.sessionFactory().openSession();
    }

    @Override
    public void returnConnection(Object connection) {
        try {
            if (connection instanceof Session) {
                Session session = (Session) connection;
                if (session.isOpen()) {
                    session.clear();
                    session.close();
                }
            }
        } catch (HibernateException he) {
            LOGGER.error("Error while returning connection!", he);
        }
    }

    @Override
    public int getMaxConnections() {
        return maxConnections;
    }

    @Override
    public void init() {
        if (databaseConfig != null && databaseConfig.getEntityManagerFactory().getProperties() != null) {
            Object prop = databaseConfig.getEntityManagerFactory().getProperties().getOrDefault(AvailableSettings.C3P0_MAX_SIZE, -1);
            maxConnections = prop instanceof Integer ? (Integer) prop : Integer.parseInt(prop.toString());
        }
    }

}
