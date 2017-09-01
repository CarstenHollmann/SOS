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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Disjunction;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.spatial.criterion.SpatialProjections;
import org.n52.series.db.beans.AbstractFeatureEntity;
import org.n52.shetland.ogc.filter.SpatialFilter;
import org.n52.shetland.ogc.gml.AbstractFeature;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.SosConstants;
import org.n52.shetland.util.CollectionHelper;
import org.n52.sos.ds.hibernate.util.SpatialRestrictions;
import org.n52.sos.util.GeometryHandler;
import org.n52.sw.db.util.HibernateHelper;
import org.n52.sw.db.util.QueryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

public abstract class AbstractFeatureDao<T>
        extends
       org.n52.series.db.dao.FeatureDao
       implements
       Dao, QueryConstants {

    private static Logger LOGGER = LoggerFactory.getLogger(AbstractFeatureDao.class);
    private DaoFactory daoFactory;
    private GeometryHandler geometryhandler;

    public AbstractFeatureDao(DaoFactory daoFactory, Session session, GeometryHandler geometryhandler) {
        super(session);
        this.daoFactory = daoFactory;
        this.geometryhandler = geometryhandler;
    }

    @Override
    public DaoFactory getDaoFactory() {
        return daoFactory;
    }

    @Override
    public Session getSession() {
        return session;
    }

    public GeometryHandler getGeometryHandler() {
        return geometryhandler;
    }

    public abstract AbstractFeatureEntity insert(AbstractFeature samplingFeature) throws OwsExceptionReport;

    public T get(String identifier) {
        Criteria criteria = getDefaultCriteria(session)
                .add(Restrictions.eq(AbstractFeatureEntity.PROPERTY_DOMAIN_ID, identifier));
        LOGGER.debug("QUERY getFeature(identifier): {}", HibernateHelper.getSqlString(criteria));
        return (T) criteria.uniqueResult();
    }


    @SuppressWarnings("unchecked")
    public List<String> getIdentifiers(SpatialFilter filter) throws OwsExceptionReport {
        Criteria c = getDefaultCriteria(session)
                .setProjection(Projections.distinct(Projections.property(AbstractFeatureEntity.PROPERTY_DOMAIN_ID)));
        if (filter != null && (filter.getGeometry().getGeometry().isPresent() || filter.getGeometry().getEnvelope().isPresent())) {
                c.add(SpatialRestrictions.filter(AbstractFeatureEntity.PROPERTY_GEOMETRY_ENTITY_GEOMETRY, filter.getOperator(), filter.getGeometry().toGeometry()));
        }
        return c.list();
    }

    @SuppressWarnings("unchecked")
    public Geometry getExtent(Collection<String> identifiers) {
        Geometry geom = null;
        if (identifiers != null && !identifiers.isEmpty()) {
            int count = 1;
            for (List<String> ids : QueryHelper.getListsForIdentifiers(identifiers)) {
                Criteria c = getDefaultCriteria(session);
                addIdentifierRestriction(c, ids);
                c.setProjection(SpatialProjections.extent(AbstractFeatureEntity.PROPERTY_GEOMETRY_ENTITY_GEOMETRY));
                LOGGER.debug("QUERY getFeatureExtent(identifiers)({}): {}", count++, HibernateHelper.getSqlString(c));
                mergeGeometries(geom, c.list());
            }
        } else {
            Criteria c = getDefaultCriteria(session);
            c.setProjection(SpatialProjections.extent(AbstractFeatureEntity.PROPERTY_GEOMETRY_ENTITY_GEOMETRY));
            LOGGER.debug("QUERY getFeatureExtent(identifiers): {}", HibernateHelper.getSqlString(c));
            mergeGeometries(geom, c.list());
        }
        return geom;
    }

    /**
     * Get featureOfInterest objects for featureOfInterest identifiers
     *
     * @param identifiers
     *            FeatureOfInterest identifiers
     * @param session
     *            Hibernate session
     * @return FeatureOfInterest objects
     */
    @SuppressWarnings("unchecked")
    public List<T> get(Collection<String> identifiers) {
        if (identifiers != null && !identifiers.isEmpty()) {
            List<T> features = new ArrayList<>();
            int count = 1;
            for (List<String> ids : QueryHelper.getListsForIdentifiers(identifiers)) {
                Criteria c = getDefaultCriteria(session);
                addIdentifierRestriction(c, ids);
                LOGGER.debug("QUERY getFeatureOfInterestObjects(identifiers)({}): {}", count++, HibernateHelper.getSqlString(c));
                features.addAll(c.list());
            }
            return features;
        } else {
            Criteria c = getDefaultCriteria(session);
            LOGGER.debug("QUERY getFeatureOfInterestObjects(identifiers): {}", HibernateHelper.getSqlString(c));
            return c.list();
        }
    }

    protected T get(String identifier, Geometry geometry) throws OwsExceptionReport {
        if (!identifier.startsWith(SosConstants.GENERATED_IDENTIFIER_PREFIX)) {
            return (T) getDefaultCriteria(session)
                    .add(Restrictions.eq(AbstractFeatureEntity.PROPERTY_DOMAIN_ID, identifier)).uniqueResult();
        } else {
            return (T) getDefaultCriteria(session)
                    .add(SpatialRestrictions.eq(AbstractFeatureEntity.PROPERTY_GEOMETRY_ENTITY_GEOMETRY, getGeometryHandler()
                            .switchCoordinateAxisFromToDatasourceIfNeeded(geometry))).uniqueResult();
        }
    }

    @SuppressWarnings("unchecked")
    public List<T> get() {
        return getDefaultCriteria(session).list();
    }

    @SuppressWarnings("unchecked")
    public List<T> get(Collection<String> identifiers, Collection<SpatialFilter> filters) throws OwsExceptionReport {
        if (CollectionHelper.isNotEmpty(identifiers)) {
            return getChunks(identifiers, filters);
        } else {
            Criteria c = getDefaultCriteria(session);
            addSpatialFilters(c, filters);
            LOGGER.debug("QUERY getFeatures(identifiers)): {}", HibernateHelper.getSqlString(c));
            return c.list();
        }
    }

    @SuppressWarnings("unchecked")
    private List<T> getChunks(Collection<String> identifiers,
            Collection<SpatialFilter> filters) throws OwsExceptionReport {
        List<T> features = new ArrayList<>();
        int count = 1;
        for (List<String> ids : QueryHelper.getListsForIdentifiers(identifiers)) {
            Criteria c = getDefaultCriteria(getSession());
            addIdentifierRestriction(c, ids);
            addSpatialFilters(c, filters);
            LOGGER.debug("QUERY getFeatures(identifiers)({}): {}", count++, HibernateHelper.getSqlString(c));
            features.addAll(c.list());
        }
        return features;
    }

    private Criteria addIdentifierRestriction(Criteria c, Collection<String> identifiers) {
        if (CollectionHelper.isNotEmpty(identifiers)) {
            c.add(Restrictions.in(AbstractFeatureEntity.PROPERTY_DOMAIN_ID, identifiers));
        }
        return c;
    }

    private void addSpatialFilters(Criteria c, Collection<SpatialFilter> filters) throws OwsExceptionReport {
        if (CollectionHelper.isNotEmpty(filters)) {
            Disjunction disjunction = Restrictions.disjunction();
            for (SpatialFilter filter : filters) {
                if (filter != null && (filter.getGeometry().getGeometry().isPresent() || filter.getGeometry().getEnvelope().isPresent())) {
                    disjunction.add(SpatialRestrictions.filter(AbstractFeatureEntity.PROPERTY_GEOMETRY_ENTITY_GEOMETRY, filter.getOperator(), filter.getGeometry().toGeometry()));
                }
            }
            c.add(disjunction);
        }
    }

    private void mergeGeometries(Geometry geom, List<Object> list) {
        for (Object extent : list) {
            if (extent != null) {
                if (geom == null) {
                    geom =  (Geometry) extent;
                } else {
                    geom.union((Geometry) extent);
                }
            }
        }
    }

    public void updateFeatureOfInterest(AbstractFeatureEntity featureOfInterest, AbstractFeature abstractFeature) {
        addName(abstractFeature, featureOfInterest);
        session.saveOrUpdate(featureOfInterest);
    }

    protected Criteria getDefaultCriteria(Session session) {
        return session.createCriteria(AbstractFeatureEntity.class).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

}
