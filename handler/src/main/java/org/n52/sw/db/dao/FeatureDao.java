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

import static org.n52.janmayen.http.HTTPStatus.BAD_REQUEST;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.sql.JoinType;
import org.n52.iceland.exception.ows.concrete.NotYetSupportedException;
import org.n52.janmayen.function.Suppliers;
import org.n52.janmayen.http.HTTPStatus;
import org.n52.series.db.DataModelUtil;
import org.n52.series.db.beans.AbstractFeatureEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.FeatureEntity;
import org.n52.series.db.beans.FeatureTypeEntity;
import org.n52.series.db.beans.ObservationConstellationEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.UnitEntity;
import org.n52.series.db.beans.feature.SpecimenEntity;
import org.n52.shetland.ogc.OGCConstants;
import org.n52.shetland.ogc.UoM;
import org.n52.shetland.ogc.gml.AbstractFeature;
import org.n52.shetland.ogc.gml.FeatureWith.FeatureWithFeatureType;
import org.n52.shetland.ogc.gml.FeatureWith.FeatureWithGeometry;
import org.n52.shetland.ogc.gml.time.TimeInstant;
import org.n52.shetland.ogc.gml.time.TimePeriod;
import org.n52.shetland.ogc.om.features.samplingFeatures.AbstractSamplingFeature;
import org.n52.shetland.ogc.om.features.samplingFeatures.FeatureOfInterestVisitor;
import org.n52.shetland.ogc.om.features.samplingFeatures.SamplingFeature;
import org.n52.shetland.ogc.om.features.samplingFeatures.SfSpecimen;
import org.n52.shetland.ogc.om.series.wml.WmlMonitoringPoint;
import org.n52.shetland.ogc.om.values.Value;
import org.n52.shetland.ogc.ows.exception.CodedException;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.sos.ds.FeatureQueryHandler;
import org.n52.sos.util.GeometryHandler;
import org.n52.sw.db.util.NoopTransformerAdapter;
import org.n52.sw.db.util.QueryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Point;

public class FeatureDao
        extends
        AbstractFeatureDao<FeatureEntity> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureDao.class);
    private static final String SQL_QUERY_GET_FEATURE_OF_INTEREST_IDENTIFIER_FOR_OFFERING =
            "getFeatureOfInterestIdentifiersForOffering";
    private static final String SQL_QUERY_GET_FEATURE_OF_INTEREST_IDENTIFIER_FOR_OBSERVATION_CONSTELLATION =
            "getFeatureOfInterestIdentifiersForObservationConstellation";
    private FeatureQueryHandler featureQueryHandler;


    public FeatureDao(DaoFactory daoFactory, Session session, GeometryHandler geometryHandler, FeatureQueryHandler featureQueryHandler) {
        super(daoFactory, session, geometryHandler);
        this.featureQueryHandler = featureQueryHandler;
    }

    public FeatureQueryHandler getFeatureQueryHandler() {
        return featureQueryHandler;
    }

    @Override
    public AbstractFeatureEntity insert(AbstractFeature abstractFeature)
            throws OwsExceptionReport {

        FeatureOfInterestPersister persister = new FeatureOfInterestPersister(
                this,
                getSession(),
                getDaoFactory()
        );
        return abstractFeature.accept(persister);
    }


    /**
     * Get featureOfInterest identifiers for observation constellation
     *
     * @param oc
     *            Observation constellation
     * @return FeatureOfInterest identifiers for observation constellation
     * @throws CodedException
     */
    @SuppressWarnings("unchecked")
    public List<String> get(ObservationConstellationEntity oc) throws OwsExceptionReport {
        if (DataModelUtil.isNamedQuerySupported(
                SQL_QUERY_GET_FEATURE_OF_INTEREST_IDENTIFIER_FOR_OBSERVATION_CONSTELLATION, session)) {
            Query namedQuery =
                    session.getNamedQuery(SQL_QUERY_GET_FEATURE_OF_INTEREST_IDENTIFIER_FOR_OBSERVATION_CONSTELLATION);
            namedQuery.setParameter(PROCEDURE, oc.getProcedure().getIdentifier());
            namedQuery.setParameter(OBSERVABLE_PROPERTY, oc.getObservableProperty().getIdentifier());
            namedQuery.setParameter(OFFERING, oc.getOffering().getIdentifier());
            LOGGER.debug(
                    "QUERY getFeatureOfInterestIdentifiersForObservationConstellation(observationConstellation) with NamedQuery: {}",
                    SQL_QUERY_GET_FEATURE_OF_INTEREST_IDENTIFIER_FOR_OBSERVATION_CONSTELLATION);
            return namedQuery.list();
        } else {
            AbstractSeriesDao seriesDao = getDaoFactory().getSeriesDao(getSession());
            Criteria criteria = seriesDao.getDefaultSeriesCriteria();
            criteria.add(Restrictions.eq(DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION, oc));
            criteria.createCriteria(DatasetEntity.PROPERTY_FEATURE)
                        .setProjection(Projections.distinct(Projections.property(FeatureEntity.PROPERTY_DOMAIN_ID)));
            LOGGER.debug(
                    "QUERY getFeatureOfInterestIdentifiersForObservationConstellation(observationConstellation): {}",
                    DataModelUtil.getSqlString(criteria));
            return criteria.list();
        }
    }

    /**
     * Get featureOfInterest identifiers for an offering identifier
     *
     * @param offering
     *            Offering identifier
     * @return FeatureOfInterest identifiers for offering
     * @throws CodedException
     */
    @SuppressWarnings({ "unchecked" })
    public List<String> getForOffering(String offering, Session session)
            throws OwsExceptionReport {
        if (DataModelUtil.isNamedQuerySupported(SQL_QUERY_GET_FEATURE_OF_INTEREST_IDENTIFIER_FOR_OFFERING,
                session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_FEATURE_OF_INTEREST_IDENTIFIER_FOR_OFFERING);
            namedQuery.setParameter(OFFERING, offering);
            LOGGER.debug("QUERY getFeatureOfInterestIdentifiersForOffering(offeringIdentifiers) with NamedQuery: {}",
                    SQL_QUERY_GET_FEATURE_OF_INTEREST_IDENTIFIER_FOR_OFFERING);
            return namedQuery.list();
        } else {
            AbstractSeriesDao seriesDao = getDaoFactory().getSeriesDao(getSession());
            Criteria criteria = seriesDao.getDefaultSeriesCriteria();
            criteria.createCriteria(DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION)
                    .createCriteria(ObservationConstellationEntity.OFFERING)
                    .add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering));
            criteria.createCriteria(DatasetEntity.PROPERTY_FEATURE)
                    .setProjection(Projections.distinct(Projections.property(FeatureEntity.PROPERTY_DOMAIN_ID)));

            LOGGER.debug("QUERY getForOffering(offeringIdentifiers): {}",
                    DataModelUtil.getSqlString(criteria));
            return criteria.list();
        }
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
    public List<FeatureEntity> get(Collection<String> identifiers) {
        if (identifiers == null || identifiers.isEmpty()) {
            return Collections.emptyList();
        }
        Criteria criteria = getDefaultCriteria()
                .add(QueryHelper.getCriterionForObjects(FeatureEntity.PROPERTY_DOMAIN_ID, identifiers));
        LOGGER.debug("QUERY getFeatureOfInterestObject(identifiers): {}", DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Load FOI identifiers and parent ids for use in the cache. Just loading
     * the ids allows us to not load the geometry columns, XML, etc.
     *
     * @return Map keyed by FOI identifiers, with value collections of parent
     *         FOI identifiers if supported
     */
    public Map<String,Collection<String>> getParentMap() {
        Criteria criteria = getDefaultCriteria()
                .createAlias(FeatureEntity.PROPERTY_PARENTS, "pfoi", JoinType.LEFT_OUTER_JOIN)
                .setProjection(Projections.projectionList()
                        .add(Projections.property(FeatureEntity.PROPERTY_DOMAIN_ID))
                        .add(Projections.property("pfoi." + FeatureEntity.PROPERTY_DOMAIN_ID)));
        // return as List<Object[]> even if there's only one column for
        // consistency
        criteria.setResultTransformer(NoopTransformerAdapter.INSTANCE);

        LOGGER.debug("QUERY getFeatureOfInterestIdentifiersWithParents(): {}", DataModelUtil.getSqlString(criteria));
        @SuppressWarnings("unchecked")
        List<Object[]> results = criteria.list();
        Map<String, Collection<String>> foiMap = Maps.newHashMap();
        results.forEach(result -> {
            String featureIdentifier = (String) result[0];
            String parentFeatureIdentifier = (String) result[1];
            if (parentFeatureIdentifier != null) {
                foiMap.computeIfAbsent(featureIdentifier, Suppliers.asFunction(ArrayList::new))
                        .add(parentFeatureIdentifier);
            } else {
                foiMap.put(featureIdentifier, null);
            }
        });
        return foiMap;
    }

    /**
     * Insert and/or get featureOfInterest object for identifier
     *
     * @param identifier
     *            FeatureOfInterest identifier
     * @param url
     *            FeatureOfInterest URL, if defined as link
     * @return FeatureOfInterest object
     */
    public FeatureEntity getOrInsert(String identifier, String url) {
        FeatureEntity feature = get(identifier);
        if (feature == null) {
            feature = new FeatureEntity();
            feature.setIdentifier(identifier);
            if (url != null && !url.isEmpty()) {
                feature.setUrl(url);
            }
            FeatureTypeEntity type = getDaoFactory().getFeatureTypeDao(getSession()).getOrInsert(OGCConstants.UNKNOWN);
            feature.setFeatureType(type);
            getSession().save(feature);
        } else if (feature.getUrl() != null && !feature.getUrl().isEmpty() && url != null && !url.isEmpty()) {
            feature.setUrl(url);
            getSession().saveOrUpdate(feature);
        }
        // don't flush here because we may be batching
        return feature;
    }

    /**
     * Insert featureOfInterest relationship
     *
     * @param parentFeature
     *            Parent featureOfInterest
     * @param childFeature
     *            Child featureOfInterest
     */
    public void insertRelationship(AbstractFeatureEntity parentFeature, AbstractFeatureEntity childFeature, Session session) {
        parentFeature.getChildren().add(childFeature);
        session.saveOrUpdate(parentFeature);
        // don't flush here because we may be batching
    }

    /**
     * Insert featureOfInterest/related feature relations if relatedFeatures
     * exists for offering.
     *
     * @param featureOfInterest
     *            FeatureOfInerest
     * @param offering
     *            Offering
     */
    public void checkOrInsertRelatedFeatureRelation(AbstractFeatureEntity featureOfInterest, OfferingEntity offering, Session session) {
        getDaoFactory().getRelatedFeatureDao(getSession())
                .get(offering.getIdentifier())
                .stream()
                .filter(relatedFeature -> !featureOfInterest.getIdentifier().equals(relatedFeature.getFeature().getIdentifier()))
                .forEachOrdered(relatedFeature -> insertRelationship(relatedFeature.getFeature(), featureOfInterest, session));

    }

    /**
     * Insert featureOfInterest if it is supported
     *
     * @param featureOfInterest
     *            SOS featureOfInterest to insert
     * @return FeatureOfInterest object
     * @throws NoApplicableCodeException
     *             If SOS feature type is not supported (with status
     *             {@link HTTPStatus}.BAD_REQUEST
     */
    public FeatureEntity checkOrInsert(AbstractFeature featureOfInterest, Session session) throws OwsExceptionReport {
        if (featureOfInterest instanceof AbstractSamplingFeature) {
            AbstractSamplingFeature sf = (AbstractSamplingFeature) featureOfInterest;
            String featureIdentifier = getFeatureQueryHandler().insertFeature(sf, getSession());
            return getOrInsert(featureIdentifier, sf.getUrl());
        } else {
            Object type = featureOfInterest != null ? featureOfInterest.getClass().getName() : featureOfInterest;
            throw new NoApplicableCodeException()
                    .withMessage("The used feature type '%s' is not supported.", type)
                    .setStatus(BAD_REQUEST);
        }
    }

    public void updateFeatureOfInterestGeometry(AbstractFeatureEntity featureOfInterest, Geometry geom, Session session) {
        if (featureOfInterest.isSetGeometry()) {
            if (geom instanceof Point) {
                List<Coordinate> coords = Lists.newArrayList();
                if (featureOfInterest.getGeometry() instanceof Point) {
                    coords.add(featureOfInterest.getGeometry().getCoordinate());
                } else if (featureOfInterest.getGeometry() instanceof LineString) {
                    coords.addAll(Lists.newArrayList(featureOfInterest.getGeometry().getCoordinates()));
                }
                if (!coords.isEmpty()) {
                    coords.add(geom.getCoordinate());
                    Geometry newGeometry =
                            new GeometryFactory().createLineString(coords.toArray(new Coordinate[coords.size()]));
                    newGeometry.setSRID(featureOfInterest.getGeometry().getSRID());
                    featureOfInterest.setGeometry(newGeometry);
                }
            }
        } else {
            featureOfInterest.setGeometry(geom);
        }
        session.saveOrUpdate(featureOfInterest);
    }

    protected Criteria getDefaultCriteria() {
        return getSession().createCriteria(FeatureEntity.class).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    private DetachedCriteria getDetachedCriteriaSeries()
            throws OwsExceptionReport {
        final DetachedCriteria detachedCriteria =
                DetachedCriteria.forClass(getDaoFactory().getSeriesDao(getSession()).getSeriesClass());
        detachedCriteria.add(Restrictions.eq(DatasetEntity.PROPERTY_DELETED, false))
                .add(Restrictions.eq(DatasetEntity.PROPERTY_PUBLISHED, true));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(DatasetEntity.PROPERTY_FEATURE)));
        return detachedCriteria;
    }

    private DetachedCriteria getDetachedCriteriaSeriesForOffering(String offering) throws OwsExceptionReport {
        final DetachedCriteria detachedCriteria = getDetachedCriteriaSeries();
        DetachedCriteria ocCriteria = detachedCriteria.createCriteria(DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION);
        ocCriteria.createCriteria(ObservationConstellationEntity.OFFERING)
        .add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering));
        return detachedCriteria;
    }

    public static class FeatureOfInterestPersister implements FeatureOfInterestVisitor<AbstractFeatureEntity> {

            private FeatureDao dao;
            private Session session;
            private DaoFactory daoFactory;

            public FeatureOfInterestPersister(FeatureDao dao, Session session, DaoFactory daoFactory) {
               this.dao = dao;
               this.session = session;
               this.daoFactory = daoFactory;
            }

            @Override
            public AbstractFeatureEntity visit(SamplingFeature value) throws OwsExceptionReport {
                AbstractFeatureEntity feature = getFeatureOfInterest(value);
                if (feature == null) {
                    return persist(new FeatureEntity(), value, true);
                }
                return persist(feature, value, false);
            }

            @Override
            public AbstractFeatureEntity visit(SfSpecimen value) throws OwsExceptionReport {
                AbstractFeatureEntity feature = getFeatureOfInterest(value);
                if (feature == null) {
                    SpecimenEntity specimen = new SpecimenEntity();
                    specimen.setMaterialClass(value.getMaterialClass().getHref());
                    if (value.getSamplingTime() instanceof TimeInstant) {
                        TimeInstant time = (TimeInstant) value.getSamplingTime();
                        specimen.setSamplingTimeStart(time.getValue().toDate());
                        specimen.setSamplingTimeEnd(time.getValue().toDate());
                    } else if (value.getSamplingTime() instanceof TimePeriod) {
                        TimePeriod time = (TimePeriod) value.getSamplingTime();
                        specimen.setSamplingTimeStart(time.getStart().toDate());
                        specimen.setSamplingTimeEnd(time.getEnd().toDate());
                    }
                    if (value.isSetSamplingMethod()) {
                        specimen.setSamplingMethod(value.getSamplingMethod().getReference().getHref().toString());
                    }
                    if (value.isSetSize()) {
                        specimen.setSize(value.getSize().getValue());
                        specimen.setSizeUnit(getUnit(value.getSize()));
                    }
                    if (value.isSetCurrentLocation()) {
                        specimen.setCurrentLocation(value.getCurrentLocation().getReference().getHref().toString());
                    }
                    if (value.isSetSpecimenType()) {
                        specimen.setSpecimenType(value.getSpecimenType().getHref());
                    }
                    return persist(specimen, value, true);
                }
                return persist(feature, value, false);
            }

            @Override
            public AbstractFeatureEntity visit(WmlMonitoringPoint monitoringPoint) throws OwsExceptionReport {
               throw new NotYetSupportedException(WmlMonitoringPoint.class.getSimpleName());
            }

            private AbstractFeatureEntity persist(AbstractFeatureEntity feature, AbstractFeature abstractFeature, boolean add) throws OwsExceptionReport {
                if (add) {
                    dao.addDomainIdNameDescription(abstractFeature, feature);
                    if (abstractFeature instanceof FeatureWithGeometry) {
                        if (((FeatureWithGeometry) abstractFeature).isSetGeometry()) {
                            feature.setGeometry(((FeatureWithGeometry) abstractFeature).getGeometry());
                        }
                    }
                    if (abstractFeature.isSetXml()) {
                        feature.setDescriptionXml(abstractFeature.getXml());
                    }
                    if (abstractFeature instanceof FeatureWithFeatureType
                            && ((FeatureWithFeatureType) abstractFeature).isSetFeatureType()) {
                        feature.setFeatureType(daoFactory.getFeatureTypeDao(session).getOrInsert(
                                ((FeatureWithFeatureType) abstractFeature).getFeatureType()));
                    }
                    if (abstractFeature instanceof AbstractSamplingFeature) {
                        AbstractSamplingFeature samplingFeature = (AbstractSamplingFeature) abstractFeature;
                        if (samplingFeature.isSetSampledFeatures()) {
                            Set<AbstractFeatureEntity> parents =
                                    Sets.newHashSetWithExpectedSize(samplingFeature.getSampledFeatures().size());
                            for (AbstractFeature sampledFeature : samplingFeature.getSampledFeatures()) {
                                if (!OGCConstants.UNKNOWN.equals(sampledFeature.getIdentifierCodeWithAuthority().getValue())) {
                                    if (sampledFeature instanceof AbstractSamplingFeature) {
                                        parents.add(dao.insert((AbstractSamplingFeature) sampledFeature));
                                    } else {
                                        parents.add(dao.insert(
                                                new SamplingFeature(sampledFeature.getIdentifierCodeWithAuthority())));
                                    }
                                }
                            }
                            feature.setParents(parents);
                        }
                    }
                    session.saveOrUpdate(feature);
                    session.flush();
                    session.refresh(feature);
                }
                if (abstractFeature instanceof AbstractSamplingFeature && ((AbstractSamplingFeature) abstractFeature).isSetParameter()) {
                    Map<UoM, UnitEntity> unitCache = Maps.newHashMap();
                    feature.setParameters(daoFactory.getParameterDao(session).insert(((AbstractSamplingFeature) abstractFeature).getParameters(),
                            feature.getId(), unitCache));
                }
                return feature;
            }

            private UnitEntity getUnit(Value<?> value) {
                return value.isSetUnit() ? daoFactory.getUnitDao(session).getOrInsert(value.getUnitObject()) : null;
            }

            private AbstractFeatureEntity getFeatureOfInterest(AbstractSamplingFeature value) throws OwsExceptionReport {
                final String newId = value.getIdentifierCodeWithAuthority().getValue();
                Geometry geom = null;
                if (value instanceof FeatureWithGeometry) {
                    geom = ((FeatureWithGeometry) value).getGeometry();

                }
                return dao.get(newId, geom);
            }
        }

}
