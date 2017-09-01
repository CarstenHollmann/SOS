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
package org.n52.sw.db.dao;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.n52.series.db.beans.FeatureEntity;
import org.n52.series.db.beans.FeatureTypeEntity;
import org.n52.shetland.ogc.OGCConstants;
import org.n52.sw.db.util.HibernateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureTypeDao
        extends
        AbstractDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureTypeDao.class);

    public FeatureTypeDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }

    /**
     * Get all featureOfInterest types
     *
     * @return All featureOfInterest types
     */
    @SuppressWarnings("unchecked")
    public List<String> get() {
        Criteria criteria = getDefaultCriteria()
                        .add(Restrictions.ne(FeatureTypeEntity.TYPE, OGCConstants.UNKNOWN))
                        .setProjection(
                                Projections.distinct(Projections
                                        .property(FeatureTypeEntity.TYPE)));

        LOGGER.debug("QUERY get(): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get featureOfInterest type object for featureOfInterest type
     *
     * @param type
     *            FeatureOfInterest type
     * @return FeatureOfInterest type object
     */
    public FeatureTypeEntity get(String type) {
        Criteria criteria = getDefaultCriteria().add(
                        Restrictions.eq(FeatureTypeEntity.TYPE, type));
        LOGGER.debug("QUERY get(type): {}",
                HibernateHelper.getSqlString(criteria));
        return (FeatureTypeEntity) criteria.uniqueResult();
    }

    /**
     * Get featureOfInterest type objects for featureOfInterest types
     *
     * @param type
     *            FeatureOfInterest types
     * @return FeatureOfInterest type objects
     */
    @SuppressWarnings("unchecked")
    public List<FeatureTypeEntity> getFeatureTypeEntityObjects(Collection<String> type) {
        Criteria criteria = getDefaultCriteria().add(
                        Restrictions.in(FeatureTypeEntity.TYPE, type));
        LOGGER.debug("QUERY get(type): {}",
                HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get featureOfInterest type objects for featureOfInterest identifiers
     *
     * @param featureOfInterestIdentifiers
     *            FeatureOfInterest identifiers
     * @return FeatureOfInterest type objects
     */
    @SuppressWarnings("unchecked")
    public List<String> getForFeature(Collection<String> features) {
        Criteria criteria = getDefaultCriteria().add(
                        Restrictions.in(FeatureEntity.PROPERTY_DOMAIN_ID, features));
        criteria.createCriteria(FeatureEntity.PROPERTY_DOMAIN_ID).setProjection(
                Projections.distinct(Projections.property(FeatureTypeEntity.TYPE)));
        LOGGER.debug("QUERY getForFeature(features): {}",
                HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Insert and/or get featureOfInterest type object for featureOfInterest
     * type
     *
     * @param type
     *            FeatureOfInterest type
     * @return FeatureOfInterest type object
     */
    public FeatureTypeEntity getOrInsert(String type) {
        FeatureTypeEntity featureOfInterestType = get(type);
        if (featureOfInterestType == null) {
            featureOfInterestType = new FeatureTypeEntity();
            featureOfInterestType.setType(type);
            getSession().save(featureOfInterestType);
            getSession().flush();
        }
        return featureOfInterestType;
    }

    /**
     * Insert and/or get featureOfInterest type objects for featureOfInterest
     * types
     *
     * @param types
     *            FeatureOfInterest types
     * @return FeatureOfInterest type objects
     */
    public List<FeatureTypeEntity> getOrInsert(Collection<String> types) {
        final List<FeatureTypeEntity> featureTypes = new LinkedList<FeatureTypeEntity>();
        for (final String type : types) {
            featureTypes.add(getOrInsert(type));
        }
        return featureTypes;
    }

    private Criteria getDefaultCriteria() {
        return getSession().createCriteria(FeatureTypeEntity.class)
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }
}
