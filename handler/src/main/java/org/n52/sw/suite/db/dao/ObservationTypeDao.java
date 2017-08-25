/*
 * Copyright (C) 2012-2017 52°North Initiative for Geospatial Open Source
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
package org.n52.sw.suite.db.dao;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.n52.series.db.DataModelUtil;
import org.n52.series.db.beans.ObservationTypeEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObservationTypeDao extends AbstractDao {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObservationTypeDao.class);

    public ObservationTypeDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }

    /**
     * Get observation type objects for observation types
     *
     * @param observationTypes
     *            Observation types
     * @param session
     *            Hibernate session
     * @return Observation type objects
     */
    @SuppressWarnings("unchecked")
    public List<ObservationTypeEntity> get(List<String> observationTypes) {
        Criteria criteria =
                getSession().createCriteria(ObservationTypeEntity.class).add(
                        Restrictions.in(ObservationTypeEntity.TYPE, observationTypes));
        LOGGER.debug("QUERY getObservationTypeObjects(observationTypes): {}", DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get observation type object for observation type
     *
     * @param observationType
     * @param session
     *            Hibernate session
     * @return Observation type object
     */
    public ObservationTypeEntity get(String observationType) {
        Criteria criteria =
                getSession().createCriteria(ObservationTypeEntity.class).add(
                        Restrictions.eq(ObservationTypeEntity.TYPE, observationType));
        LOGGER.debug("QUERY getObservationTypeObject(observationType): {}", DataModelUtil.getSqlString(criteria));
        return (ObservationTypeEntity) criteria.uniqueResult();
    }

    /**
     * Insert or/and get observation type object for observation type
     *
     * @param observationType
     *            Observation type
     * @param session
     *            Hibernate session
     * @return Observation type object
     */
    public ObservationTypeEntity getOrInsert(String observationType) {
        ObservationTypeEntity hObservationType = get(observationType);
        if (hObservationType == null) {
            hObservationType = new ObservationTypeEntity();
            hObservationType.setType(observationType);
            getSession().save(hObservationType);
            getSession().flush();
        }
        return hObservationType;
    }

    /**
     * Insert or/and get observation type objects for observation types
     *
     * @param observationTypes
     *            Observation types
     * @param session
     *            Hibernate session
     * @return Observation type objects
     */
    public List<ObservationTypeEntity> getOrInsert(Set<String> observationTypes) {
        List<ObservationTypeEntity> obsTypes = new LinkedList<>();
        for (String observationType : observationTypes) {
            obsTypes.add(getOrInsert(observationType));
        }
        return obsTypes;
    }
}
