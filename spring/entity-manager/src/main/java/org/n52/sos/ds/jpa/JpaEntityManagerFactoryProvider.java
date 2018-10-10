/*
 * Copyright (C) 2012-2018 52°North Initiative for Geospatial Open Source
 * Software GmbH
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * If the program is linked with libraries which are licensed under one of
 * the following licenses, the combination of the program with the linked
 * library is not considered a "derivative work" of the program:
 *
 *     - Apache License, version 2.0
 *     - Apache Software License, version 1.0
 *     - GNU Lesser General Public License, version 3
 *     - Mozilla Public License, versions 1.0, 1.1 and 2.0
 *     - Common Development and Distribution License (CDDL), version 1.0
 *
 * Therefore the distribution of the program linked with libraries licensed
 * under the aforementioned licenses, is permitted by the copyright holders
 * if the distribution is compliant with both the GNU General Public
 * License version 2 and the aforementioned licenses.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
 * Public License for more details.
 */
package org.n52.sos.ds.jpa;

import javax.inject.Inject;
import javax.persistence.EntityManagerFactory;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.AvailableSettings;
import org.n52.iceland.ds.ConnectionProviderException;
import org.n52.iceland.ds.DataConnectionProvider;
import org.n52.janmayen.lifecycle.Constructable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

public class JpaEntityManagerFactoryProvider implements DataConnectionProvider, Constructable {
    private static final Logger LOGGER = LoggerFactory.getLogger(JpaEntityManagerFactoryProvider.class);

    @Inject
    private EntityManagerFactory entityManagerFactory;

    private int maxConnections;

    @Bean
    @Primary
    public SessionFactory sessionFactory() {
        if (entityManagerFactory.unwrap(SessionFactory.class) == null) {
            throw new NullPointerException("factory is not a hibernate factory");
        }
        return entityManagerFactory.unwrap(SessionFactory.class);
    }

    @Override
    public Object getConnection()
            throws ConnectionProviderException {
        return sessionFactory().openSession();
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
        if (entityManagerFactory.getProperties() != null) {
            Object prop = entityManagerFactory.getProperties().getOrDefault(AvailableSettings.C3P0_MAX_SIZE, -1);
            maxConnections = prop instanceof Integer ? (Integer) prop : Integer.parseInt(prop.toString());
        }
    }

}
