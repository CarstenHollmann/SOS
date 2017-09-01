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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.h2.engine.Procedure;
import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.Subqueries;
import org.hibernate.sql.JoinType;
import org.hibernate.transform.ResultTransformer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.n52.series.db.DataModelUtil;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.FeatureEntity;
import org.n52.series.db.beans.ObservationConstellationEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProcedureDescriptionFormatEntity;
import org.n52.series.db.beans.ProcedureEntity;
import org.n52.series.db.beans.ValidProcedureTimeEntity;
import org.n52.series.db.dao.DatasetDao;
import org.n52.shetland.ogc.gml.AbstractFeature;
import org.n52.shetland.ogc.gml.time.Time;
import org.n52.shetland.ogc.ows.exception.CodedException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sensorML.AbstractSensorML;
import org.n52.shetland.ogc.sos.SosProcedureDescription;
import org.n52.shetland.util.CollectionHelper;
import org.n52.shetland.util.DateTimeHelper;
import org.n52.sos.exception.ows.concrete.UnsupportedOperatorException;
import org.n52.sos.exception.ows.concrete.UnsupportedTimeException;
import org.n52.sos.exception.ows.concrete.UnsupportedValueReferenceException;
import org.n52.sw.suite.db.util.EntitiyHelper;
import org.n52.sw.suite.db.util.HibernateHelper;
import org.n52.sw.suite.db.util.NoopTransformerAdapter;
import org.n52.sw.suite.db.util.ProcedureTimeExtrema;
import org.n52.sw.suite.db.util.QueryHelper;
import org.n52.sw.suite.db.util.TimeExtrema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ProcedureDao
        extends
        org.n52.series.db.dao.ProcedureDao
        implements
        Dao, TimeCreator, QueryConstants {

    private static Logger LOGGER = LoggerFactory.getLogger(ProcedureDao.class);

    private static String SQL_QUERY_GET_PROCEDURES_FOR_ALL_FEATURES_OF_INTEREST =
            "getProceduresForAllFeaturesOfInterest";
    private static String SQL_QUERY_GET_PROCEDURES_FOR_FEATURE_OF_INTEREST = "getProceduresForFeatureOfInterest";
    private static String SQL_QUERY_GET_PROCEDURE_TIME_EXTREMA = "getProcedureTimeExtrema";
    private static String SQL_QUERY_GET_ALL_PROCEDURE_TIME_EXTREMA = "getAllProcedureTimeExtrema";
    private static String SQL_QUERY_GET_MIN_DATE_FOR_PROCEDURE = "getMinDate4Procedure";
    private static String SQL_QUERY_GET_MAX_DATE_FOR_PROCEDURE = "getMaxDate4Procedure";
    private ProcedureTimeTransformer transformer = new ProcedureTimeTransformer();

    private DaoFactory daoFactory;

    public ProcedureDao(DaoFactory daoFactory, Session session) {
        super(session);
        this.daoFactory = daoFactory;
    }

    @Override
    public DaoFactory getDaoFactory() {
        return daoFactory;
    }

    @Override
    public Session getSession() {
        return session;
    }

    /**
     * Get all procedure objects
     *
     * @return ProcedureEntity objects
     */
    @SuppressWarnings("unchecked")
    public List<ProcedureEntity> get() {
        Criteria criteria = getDefaultCriteria();
        LOGGER.debug("QUERY getProcedureObjects(): {}", DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get map keyed by undeleted procedure identifiers with collections of
     * parent procedures (if supported) as values
     *
     * @return Map keyed by procedure identifier with values of parent procedure
     *         identifier collections
     */
    public Map<String, Collection<String>> getParentMap() {
        Criteria criteria = getDefaultCriteria();
        ProjectionList projectionList = Projections.projectionList();
        projectionList.add(Projections.property(ProcedureEntity.PROPERTY_DOMAIN_ID));
        criteria.createAlias(ProcedureEntity.PROPERTY_PARENTS, "pp", JoinType.LEFT_OUTER_JOIN);
        projectionList.add(Projections.property("pp." + ProcedureEntity.PROPERTY_DOMAIN_ID));
        criteria.setProjection(projectionList);
        // return as List<Object[]> even if there's only one column for consistency
        criteria.setResultTransformer(NoopTransformerAdapter.INSTANCE);

        LOGGER.debug("QUERY getProcedureIdentifiers(): {}", DataModelUtil.getSqlString(criteria));
        @SuppressWarnings("unchecked")
        List<Object[]> results = criteria.list();
        Map<String, Collection<String>> map = Maps.newHashMap();
        for (Object[] result : results) {
            String procedureIdentifier = (String) result[0];
            String parentProcedureIdentifier = null;
                parentProcedureIdentifier = (String) result[1];
            if (parentProcedureIdentifier == null) {
                map.put(procedureIdentifier, null);
            } else {
                CollectionHelper.addToCollectionMap(procedureIdentifier, parentProcedureIdentifier, map);
            }
        }
        return map;
    }

    /**
     * Get ProcedureEntity object for procedure identifier
     *
     * @param identifier
     *            ProcedureEntity identifier
     * @return ProcedureEntity object
     */
    public ProcedureEntity get(String identifier) {
        Criteria criteria = getDefaultCriteria().createCriteria(ProcedureEntity.PROPERTY_VALID_PROCEDURE_TIME)
                .add(Restrictions.isNull(ValidProcedureTimeEntity.END_TIME));
        LOGGER.debug("QUERY getProcedureForIdentifier(identifier): {}", DataModelUtil.getSqlString(criteria));
        ProcedureEntity proc = (ProcedureEntity) criteria.uniqueResult();
        if (proc != null) {
            return proc;
        }
        criteria = getDefaultCriteria().add(Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, identifier));
        LOGGER.debug("QUERY getProcedureForIdentifier(identifier): {}", DataModelUtil.getSqlString(criteria));
        return (ProcedureEntity) criteria.uniqueResult();

    }

    /**
     * Get ProcedureEntity object for procedure identifier inclusive deleted procedure
     *
     * @param identifier
     *            ProcedureEntity identifier
     * @return ProcedureEntity object
     */
    public ProcedureEntity getIncludeDeleted(String identifier) {
        Criteria criteria =
                session.createCriteria(ProcedureEntity.class).add(Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, identifier));
        LOGGER.debug("QUERY getProcedureForIdentifierIncludeDeleted(identifier): {}",
                DataModelUtil.getSqlString(criteria));
        return (ProcedureEntity) criteria.uniqueResult();
    }

    /**
     * Get ProcedureEntity object for procedure identifier
     *
     * @param identifier
     *            ProcedureEntity identifier
     * @param session
     *            Hibernate session
     * @return ProcedureEntity object
     */
    public ProcedureEntity get(String identifier, Time time) {
        Criteria criteria = getDefaultCriteria().add(Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, identifier));
        LOGGER.debug("QUERY getProcedureForIdentifier(identifier): {}", DataModelUtil.getSqlString(criteria));
        return (ProcedureEntity) criteria.uniqueResult();
    }

    /**
     * Get transactional procedure object for procedure identifier and
     * procedureDescriptionFormat
     *
     * @param identifier
     *            ProcedureEntity identifier
     * @param procedureDescriptionFormat
     *            ProcedureDescriptionFormat identifier
     * @return Procedure object
     * @throws UnsupportedOperatorException
     * @throws UnsupportedValueReferenceException
     * @throws UnsupportedTimeException
     */
    public ProcedureEntity get(String identifier, String procedureDescriptionFormat,
            Time validTime)
                    throws UnsupportedTimeException, UnsupportedValueReferenceException, UnsupportedOperatorException {
        Criteria criteria =
                getDefaultCriteria().add(Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, identifier));
        Criteria createValidProcedureTime = criteria.createCriteria(ProcedureEntity.PROPERTY_VALID_PROCEDURE_TIME);
        Criterion validTimeCriterion = QueryHelper.getValidTimeCriterion(validTime);
        if (validTime == null || validTimeCriterion == null) {
            createValidProcedureTime.add(Restrictions.isNull(ValidProcedureTimeEntity.END_TIME));
        } else {
            createValidProcedureTime.add(validTimeCriterion);
        }
        createValidProcedureTime.createCriteria(ValidProcedureTimeEntity.PROCEDURE_DESCRIPTION_FORMAT).add(
                Restrictions.eq(ProcedureDescriptionFormatEntity.PROCEDURE_DESCRIPTION_FORMAT, procedureDescriptionFormat));
        LOGGER.debug("QUERY getTProcedureForIdentifier(identifier): {}", DataModelUtil.getSqlString(criteria));
        return (ProcedureEntity) criteria.uniqueResult();
    }

    /**
     * Get transactional procedure object for procedure identifier and
     * procedureDescriptionFormats
     *
     * @param identifier
     *            ProcedureEntity identifier
     * @param procedureDescriptionFormats
     *            ProcedureDescriptionFormat identifiers
     * @return Procedure object
     */
    public ProcedureEntity get(String identifier, Set<String> procedureDescriptionFormats) {
        Criteria criteria =
                getDefaultCriteria().add(Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, identifier));
        criteria.createCriteria(ProcedureEntity.PROPERTY_VALID_PROCEDURE_TIME)
                .add(Restrictions.in(ValidProcedureTimeEntity.PROCEDURE_DESCRIPTION_FORMAT, procedureDescriptionFormats));
        LOGGER.debug("QUERY getTProcedureForIdentifier(identifier): {}", DataModelUtil.getSqlString(criteria));
        return (ProcedureEntity) criteria.uniqueResult();
    }

    /**
     * Get procedure for identifier, possible procedureDescriptionFormats and
     * valid time
     *
     * @param identifier
     *            Identifier of the procedure
     * @param possibleProcedureDescriptionFormats
     *            Possible procedureDescriptionFormats
     * @param validTime
     *            Valid time of the procedure
     * @return ProcedureEntity entity that match the parameters
     * @throws UnsupportedTimeException
     *             If the time is not supported
     * @throws UnsupportedValueReferenceException
     *             If the valueReference is not supported
     * @throws UnsupportedOperatorException
     *             If the temporal operator is not supported
     */
    public ProcedureEntity get(String identifier, Set<String> possibleProcedureDescriptionFormats,
            Time validTime)
                    throws UnsupportedTimeException, UnsupportedValueReferenceException, UnsupportedOperatorException {
        Criteria criteria =
                getDefaultCriteria().add(Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, identifier));
        Criteria createValidProcedureTime = criteria.createCriteria(ProcedureEntity.PROPERTY_VALID_PROCEDURE_TIME);
        Criterion validTimeCriterion = QueryHelper.getValidTimeCriterion(validTime);
        if (validTime == null || validTimeCriterion == null) {
            createValidProcedureTime.add(Restrictions.isNull(ValidProcedureTimeEntity.END_TIME));
        } else {
            createValidProcedureTime.add(validTimeCriterion);
        }
        createValidProcedureTime.createCriteria(ValidProcedureTimeEntity.PROCEDURE_DESCRIPTION_FORMAT).add(Restrictions
                .in(ProcedureDescriptionFormatEntity.PROCEDURE_DESCRIPTION_FORMAT, possibleProcedureDescriptionFormats));
        LOGGER.debug(
                "QUERY getTProcedureForIdentifier(identifier, possibleProcedureDescriptionFormats, validTime): {}",
                DataModelUtil.getSqlString(criteria));
        return (ProcedureEntity) criteria.uniqueResult();
    }

    /**
     * Get ProcedureEntity objects for procedure identifiers
     *
     * @param identifiers
     *            ProcedureEntity identifiers
     * @param session
     *            Hibernate session
     * @return ProcedureEntity objects
     */
    @SuppressWarnings("unchecked")
    public List<ProcedureEntity> get(Collection<String> identifiers) {
        if (identifiers == null || identifiers.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        Criteria criteria = getDefaultCriteria().add(Restrictions.in(ProcedureEntity.PROPERTY_DOMAIN_ID, identifiers));
        LOGGER.debug("QUERY getProceduresForIdentifiers(identifiers): {}", DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get procedure identifiers for all FOIs
     *
     * @param session
     *            Hibernate session
     *
     * @return Map of foi identifier to procedure identifier collection
     * @throws HibernateException
     */
    public Map<String, Collection<String>> getForAllFeaturesOfInterest(Session session) {
        List<Object[]> results = getFeatureProcedureResult(session);
        Map<String, Collection<String>> foiProcMap = Maps.newHashMap();
        if (CollectionHelper.isNotEmpty(results)) {
            for (Object[] result : results) {
                String foi = (String) result[0];
                String proc = (String) result[1];
                Collection<String> foiProcs = foiProcMap.get(foi);
                if (foiProcs == null) {
                    foiProcs = Lists.newArrayList();
                    foiProcMap.put(foi, foiProcs);
                }
                foiProcs.add(proc);
            }
        }
        return foiProcMap;
    }

    /**
     * Get FOIs for all procedure identifiers
     *
     * @param session
     *            Hibernate session
     *
     * @return Map of procedure identifier to foi identifier collection
     */
    public Map<String, Collection<String>> getForAllProcedures(Session session) {
        List<Object[]> results = getFeatureProcedureResult(session);
        Map<String, Collection<String>> foiProcMap = Maps.newHashMap();
        if (CollectionHelper.isNotEmpty(results)) {
            for (Object[] result : results) {
                String foi = (String) result[0];
                String proc = (String) result[1];
                Collection<String> procFois = foiProcMap.get(proc);
                if (procFois == null) {
                    procFois = Lists.newArrayList();
                    foiProcMap.put(proc, procFois);
                }
                procFois.add(foi);
            }
        }
        return foiProcMap;
    }

    @SuppressWarnings("unchecked")
    private List<Object[]> getFeatureProcedureResult(Session session) {
        List<Object[]> results;
        if (DataModelUtil.isNamedQuerySupported(SQL_QUERY_GET_PROCEDURES_FOR_ALL_FEATURES_OF_INTEREST, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_PROCEDURES_FOR_ALL_FEATURES_OF_INTEREST);
            LOGGER.debug("QUERY getProceduresForAllFeaturesOfInterest(feature) with NamedQuery: {}",
                    SQL_QUERY_GET_PROCEDURES_FOR_ALL_FEATURES_OF_INTEREST);
            results = namedQuery.list();
        } else {
            Criteria c = null;
                c = session.createCriteria(new EntitiyHelper().getSeriesEntityClass())
                        .createAlias(DatasetEntity.PROPERTY_FEATURE, "f")
                        .createAlias(DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION, "o")
                        .createAlias(ObservationConstellationEntity.PROCEDURE, "p")
                        .add(Restrictions.eq(DatasetEntity.PROPERTY_DELETED, false))
                        .setProjection(Projections.distinct(Projections.projectionList()
                                .add(Projections.property("f." + FeatureEntity.PROPERTY_DOMAIN_ID))
                                .add(Projections.property("o." + "p." + ProcedureEntity.PROPERTY_DOMAIN_ID))));
            LOGGER.debug("QUERY getProceduresForAllFeaturesOfInterest(feature): {}", DataModelUtil.getSqlString(c));
            results = c.list();
        }
        return results;
    }

    /**
     * Get procedure identifiers for FOI
     *
     * @param session
     *            Hibernate session
     * @param feature
     *            FOI object
     *
     * @return Related procedure identifiers
     * @throws CodedException
     */
    @SuppressWarnings("unchecked")
    public List<String> getForFeatureOfInterest(FeatureEntity feature)
            throws OwsExceptionReport {
        if (DataModelUtil.isNamedQuerySupported(SQL_QUERY_GET_PROCEDURES_FOR_FEATURE_OF_INTEREST, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_PROCEDURES_FOR_FEATURE_OF_INTEREST);
            namedQuery.setParameter(FEATURE, feature.getIdentifier());
            LOGGER.debug("QUERY getProceduresForFeatureOfInterest(feature) with NamedQuery: {}",
                    SQL_QUERY_GET_PROCEDURES_FOR_FEATURE_OF_INTEREST);
            return namedQuery.list();
        } else {
            Criteria c = null;
                c = getDefaultCriteria();
                c.add(Subqueries.propertyIn(ProcedureEntity.PROPERTY_ID,
                        getDetachedCriteriaProceduresForFeatureOfInterestFromSeries(feature)));
                c.setProjection(Projections.distinct(Projections.property(ProcedureEntity.PROPERTY_DOMAIN_ID)));
            LOGGER.debug("QUERY getProceduresForFeatureOfInterest(feature): {}", DataModelUtil.getSqlString(c));
            return (List<String>) c.list();
        }
    }

    /**
     * Get procedure identifiers for offering identifier
     *
     * @param offeringIdentifier
     *            Offering identifier
     * @param session
     *            Hibernate session
     * @return ProcedureEntity identifiers
     * @throws CodedException
     *             If an error occurs
     */
    @SuppressWarnings("unchecked")
    public List<String> getForOffering(String offeringIdentifier)
            throws OwsExceptionReport {
        boolean obsConstSupported = HibernateHelper.isEntitySupported(ObservationConstellationEntity.class);
        Criteria c = null;

        if (obsConstSupported) {
            c = getDefaultCriteria();
            c.add(Subqueries.propertyIn(ProcedureEntity.PROPERTY_ID,
                    getDetachedCriteriaProceduresForOfferingFromObservationConstellation(offeringIdentifier)));
            c.setProjection(Projections.distinct(Projections.property(ProcedureEntity.PROPERTY_DOMAIN_ID)));
        } else {
            AbstractObservationDAO observationDAO = getDaoFactory().getObservationDAO(getSession());
            c = observationDAO.getDefaultObservationInfoCriteria();
                Criteria seriesCriteria = c.createCriteria(ContextualReferencedSeriesObservation.SERIES);
                seriesCriteria.createCriteria(Series.PROCEDURE)
                        .setProjection(Projections.distinct(Projections.property(ProcedureEntity.PROPERTY_DOMAIN_ID)));
            getDaoFactory().getOfferingDao(getSession()).addOfferingRestricionForObservation(c, offeringIdentifier);
        }
        LOGGER.debug(
                "QUERY getProcedureIdentifiersForOffering(offeringIdentifier) using ObservationContellation entitiy ({}): {}",
                obsConstSupported, DataModelUtil.getSqlString(c));
        return c.list();
    }

    private Criteria getDefaultCriteria() {
        return getSession().createCriteria(ProcedureEntity.class).add(Restrictions.eq(ProcedureEntity.DELETED, false))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    private Criteria getDefaultIncludeDeleted() {
        return getSession().createCriteria(ProcedureEntity.class).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    /**
     * Get procedure identifiers for observable property identifier
     *
     * @param observablePropertyIdentifier
     *            Observable property identifier
     * @param session
     *            Hibernate session
     * @return ProcedureEntity identifiers
     * @throws CodedException
     */
    @SuppressWarnings("unchecked")
    public Collection<String> getForObservableProperty(String observablePropertyIdentifier,
            Session session) throws OwsExceptionReport {
            Criteria c = getDefaultCriteria();
            c.setProjection(Projections.distinct(Projections.property(ProcedureEntity.PROPERTY_DOMAIN_ID)));
            c.add(Subqueries.propertyIn(ProcedureEntity.PROPERTY_ID,
                    getDetachedCriteriaProceduresForObservablePropertyFromObservationConstellation(
                            observablePropertyIdentifier)));
        LOGGER.debug(
                "QUERY getProcedureIdentifiersForObservableProperty(observablePropertyIdentifier): {}",
                DataModelUtil.getSqlString(c));
        return c.list();
    }

    public boolean isProcedureTimeExtremaNamedQuerySupported() {
        return DataModelUtil.isNamedQuerySupported(SQL_QUERY_GET_PROCEDURE_TIME_EXTREMA, getSession());
    }

    public TimeExtrema getProcedureTimeExtremaFromNamedQuery(String procedureIdentifier) {
        Object[] result = null;
        if (isProcedureTimeExtremaNamedQuerySupported()) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_PROCEDURE_TIME_EXTREMA);
            namedQuery.setParameter(PROCEDURE, procedureIdentifier);
            LOGGER.debug("QUERY getProcedureTimeExtrema({}) with NamedQuery '{}': {}", procedureIdentifier,
                    SQL_QUERY_GET_PROCEDURE_TIME_EXTREMA, namedQuery.getQueryString());
            result = (Object[]) namedQuery.uniqueResult();
        }
        return parseProcedureTimeExtremaResult(result);
    }

    public boolean isAllProcedureTimeExtremaNamedQuerySupported(Session session) {
        return DataModelUtil.isNamedQuerySupported(SQL_QUERY_GET_ALL_PROCEDURE_TIME_EXTREMA, session);
    }

    private TimeExtrema parseProcedureTimeExtremaResult(Object[] result) {
        TimeExtrema pte = new TimeExtrema();
        if (result != null) {
            pte.setMinPhenomenonTime(DateTimeHelper.makeDateTime(result[1]));
            DateTime maxPhenStart = DateTimeHelper.makeDateTime(result[2]);
            DateTime maxPhenEnd = DateTimeHelper.makeDateTime(result[3]);
            pte.setMaxPhenomenonTime(DateTimeHelper.max(maxPhenStart, maxPhenEnd));
        }
        return pte;
    }

    /**
     * Query procedure time extrema for the provided procedure identifier
     *
     * @param session
     * @param procedureIdentifier
     * @return ProcedureTimeExtrema
     * @throws CodedException
     */
    public TimeExtrema getProcedureTimeExtrema(String procedureIdentifier)
            throws OwsExceptionReport {
        Object[] result;
        if (isProcedureTimeExtremaNamedQuerySupported()) {
            return getProcedureTimeExtremaFromNamedQuery( procedureIdentifier);
        }
        DataDao observationDAO = getDaoFactory().getObservationDao(getSession());
        Criteria criteria = observationDAO.getDefaultObservationInfoCriteria();
        criteria.createAlias(ContextualReferencedSeriesObservation.SERIES, "s");
        criteria.createAlias("s." + DatasetEntity.PROCEDURE, "p");
        criteria.add(Restrictions.eq("p." + ProcedureEntity.PROPERTY_DOMAIN_ID, procedureIdentifier));
        ProjectionList projectionList = Projections.projectionList();
        projectionList.add(Projections.groupProperty("p." + ProcedureEntity.PROPERTY_DOMAIN_ID));
        projectionList.add(Projections.min(DataEntity.PROPERTY_PHENOMENON_TIME_START));
        projectionList.add(Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_START));
        projectionList.add(Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_END));
        criteria.setProjection(projectionList);

        LOGGER.debug("QUERY getProcedureTimeExtrema(procedureIdentifier): {}", DataModelUtil.getSqlString(criteria));
        result = (Object[]) criteria.uniqueResult();

        return parseProcedureTimeExtremaResult(result);
    }

    @SuppressWarnings("unchecked")
    public Map<String, TimeExtrema> getProcedureTimeExtrema() throws OwsExceptionReport {
        List<ProcedureTimeExtrema> results = null;
        if (isAllProcedureTimeExtremaNamedQuerySupported(getSession())) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_ALL_PROCEDURE_TIME_EXTREMA);
            LOGGER.debug("QUERY getProcedureTimeExtrema() with NamedQuery '{}': {}",
                    SQL_QUERY_GET_ALL_PROCEDURE_TIME_EXTREMA, namedQuery.getQueryString());
            namedQuery.setResultTransformer(transformer);
            results = namedQuery.list();
        } else {
            DatasetDao seriesDAO = getDaoFactory().getSeriesDao(getSession());
            Criteria c = seriesDAO.getDefaultSeriesCriteria();
            c.createAlias(DatasetEntity.PROCEDURE, "p");
            c.setProjection(Projections.projectionList()
                    .add(Projections.groupProperty("p." + ProcedureEntity.PROPERTY_DOMAIN_ID))
                    .add(Projections.min(DatasetEntity.PROPERTY_FIRST_VALUE_AT)).add(Projections.max(DatasetEntity.PROPERTY_LAST_VALUE_AT)));
            LOGGER.debug("QUERY getProcedureTimeExtrema(procedureIdentifier): {}",
                    DataModelUtil.getSqlString(c));
            c.setResultTransformer(transformer);
            results = c.list();
        }
        Map<String, TimeExtrema> procedureTimeExtrema = Maps.newHashMap();
        if (CollectionHelper.isNotEmpty(results)) {
            for (ProcedureTimeExtrema pte : results) {
                if (pte.isSetProcedure()) {
                    procedureTimeExtrema.put(pte.getProcedure(), pte);
                }
            }
        }
        return procedureTimeExtrema;
    }

    private boolean checkHasNoProcedureTimeResult(List<ProcedureTimeExtrema> results) {
        if (CollectionHelper.isNotEmpty(results)) {
            int noTimeCount = 0;
            for (ProcedureTimeExtrema procedureTimeExtrema : results) {
                if (!procedureTimeExtrema.isSetPhenomenonTimes()) {
                    noTimeCount++;
                }
            }
            if (results.size() > 0 && noTimeCount == results.size()) {
                return true;
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * Get min time from observations for procedure
     *
     * @param procedure
     *            ProcedureEntity identifier
     * @param session
     *            Hibernate session
     * @return min time for procedure
     * @throws CodedException
     */
    public DateTime getMinDate4Procedure(String procedure) throws OwsExceptionReport {
        Object min = null;
        if (DataModelUtil.isNamedQuerySupported(SQL_QUERY_GET_MIN_DATE_FOR_PROCEDURE, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_MIN_DATE_FOR_PROCEDURE);
            namedQuery.setParameter(PROCEDURE, procedure);
            LOGGER.debug("QUERY getMinDate4Procedure(procedure) with NamedQuery: {}",
                    SQL_QUERY_GET_MIN_DATE_FOR_PROCEDURE);
            min = namedQuery.uniqueResult();
        } else {
            DataDao observationDAO = getDaoFactory().getObservationDao(getSession());
            Criteria criteria = observationDAO.getDefaultObservationInfoCriteria(session);
            addProcedureRestrictionForSeries(criteria, procedure);
            addMinMaxProjection(criteria, MinMax.MIN, DataEntity.PROPERTY_PHENOMENON_TIME_START);
            LOGGER.debug("QUERY getMinDate4Procedure(procedure): {}", DataModelUtil.getSqlString(criteria));
            min = criteria.uniqueResult();
        }
        if (min != null) {
            return new DateTime(min, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Get max time from observations for procedure
     *
     * @param procedure
     *            ProcedureEntity identifier
     * @param session
     *            Hibernate session
     * @return max time for procedure
     * @throws CodedException
     */
    public DateTime getMaxDate4Procedure(String procedure) throws OwsExceptionReport {
        Object maxStart = null;
        Object maxEnd = null;
        if (DataModelUtil.isNamedQuerySupported(SQL_QUERY_GET_MAX_DATE_FOR_PROCEDURE, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_MAX_DATE_FOR_PROCEDURE);
            namedQuery.setParameter(PROCEDURE, procedure);
            LOGGER.debug("QUERY getMaxDate4Procedure(procedure) with NamedQuery: {}",
                    SQL_QUERY_GET_MAX_DATE_FOR_PROCEDURE);
            maxStart = namedQuery.uniqueResult();
            maxEnd = maxStart;
        } else {
            DataDao observationDAO = getDaoFactory().getObservationDao(getSession());
            Criteria cstart = observationDAO.getDefaultObservationInfoCriteria();
            Criteria cend = observationDAO.getDefaultObservationInfoCriteria();
            addProcedureRestrictionForSeries(cstart, procedure);
            addProcedureRestrictionForSeries(cend, procedure);
            addMinMaxProjection(cstart, MinMax.MAX, DataEntity.PROPERTY_PHENOMENON_TIME_START);
            addMinMaxProjection(cend, MinMax.MAX, DataEntity.PROPERTY_PHENOMENON_TIME_END);
            LOGGER.debug("QUERY getMaxDate4Procedure(procedure) start: {}", DataModelUtil.getSqlString(cstart));
            LOGGER.debug("QUERY getMaxDate4Procedure(procedure) end: {}", DataModelUtil.getSqlString(cend));
            if (DataModelUtil.getSqlString(cstart).endsWith(DataModelUtil.getSqlString(cend))) {
                maxStart = cstart.uniqueResult();
                maxEnd = maxStart;
                LOGGER.debug("Max time start and end query are identically, only one query is executed!");
            } else {
                maxStart = cstart.uniqueResult();
                maxEnd = cend.uniqueResult();
            }
        }
        if (maxStart == null && maxEnd == null) {
            return null;
        } else {
            DateTime start = new DateTime(maxStart, DateTimeZone.UTC);
            if (maxEnd != null) {
                DateTime end = new DateTime(maxEnd, DateTimeZone.UTC);
                if (end.isAfter(start)) {
                    return end;
                }
            }
            return start;
        }
    }

    /**
     * Insert and get procedure object
     *
     * @param identifier
     *            ProcedureEntity identifier
     * @param procedureDescriptionFormat
     *            ProcedureEntity description format object
     * @param procedureDescription
     *            {@link SosProcedureDescription} to insert
     * @param isType
     * @param session
     *            Hibernate session
     * @return ProcedureEntity object
     */
    public ProcedureEntity getOrInsertProcedure(String identifier, ProcedureDescriptionFormatEntity procedureDescriptionFormat,
            SosProcedureDescription<?> procedureDescription, boolean isType) {
        ProcedureEntity procedure = getIncludeDeleted(identifier);
        if (procedure == null) {
            ProcedureEntity procedureEntity = new ProcedureEntity();
            procedureEntity.setProcedureDescriptionFormat(procedureDescriptionFormat);
            procedureEntity.setIdentifier(identifier);
            if (procedureDescription.getProcedureDescription() instanceof AbstractFeature) {
                AbstractFeature af = (AbstractFeature) procedureDescription.getProcedureDescription();
                if (af.isSetName()) {
                    procedureEntity.setName(af.getFirstName().getValue());
                }
                if (af.isSetDescription()) {
                    procedureEntity.setDescription(af.getDescription());
                }
            }
            if (procedureDescription.isSetParentProcedure()) {
                ProcedureEntity parent = get(procedureDescription.getParentProcedure().getHref());
                if (parent != null) {
                    procedureEntity.setParents(Sets.newHashSet(parent));
                }
            }
            if (procedureDescription.getTypeOf() != null && !procedureEntity.isSetTypeOf()) {
                ProcedureEntity typeOfProc = get(procedureDescription.getTypeOf().getTitle());
                if (typeOfProc != null) {
                    procedureEntity.setTypeOf(typeOfProc);
                }
            }
            procedureEntity.setType(isType);
            procedureEntity.setAggregation(procedureDescription.isAggregation());
            if (procedureDescription.getProcedureDescription() instanceof AbstractSensorML) {
                AbstractSensorML sml = (AbstractSensorML) procedureDescription.getProcedureDescription();
                if (sml.isSetMobile()) {
                    procedureEntity.setMobile(sml.getMobile());
                }
                if (sml.isSetInsitu()) {
                    procedureEntity.setInsitu(sml.getInsitu());
                }
            }
            procedure = procedureEntity;
        }
        procedure.setDeleted(false);
        session.saveOrUpdate(procedure);
        session.flush();
        session.refresh(procedure);
        return procedure;
    }

    /**
     * Get Hibernate Detached Criteria for class Series and featureOfInterest
     * identifier
     *
     * @param featureOfInterest
     *            FeatureOfInterest identifier parameter
     * @param session
     *            Hibernate session
     * @return Hiberante Detached Criteria with ProcedureEntity entities
     * @throws CodedException
     */
    private DetachedCriteria getDetachedCriteriaProceduresForFeatureOfInterestFromSeries(
            FeatureEntity featureOfInterest) throws OwsExceptionReport {
        DetachedCriteria detachedCriteria =
                DetachedCriteria.forClass(getDaoFactory().getSeriesDao(getSession()).getClass());
        detachedCriteria.add(Restrictions.eq(DatasetEntity.PROPERTY_DELETED, false));
        detachedCriteria.add(Restrictions.eq(DatasetEntity.PROPERTY_FEATURE, featureOfInterest));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(Series.PROCEDURE)));
        return detachedCriteria;
    }

    /**
     * Get Hibernate Detached Criteria for class ObservationConstellation and
     * observableProperty identifier
     *
     * @param observablePropertyIdentifier
     *            ObservableProperty identifier parameter
     * @param session
     *            Hibernate session
     * @return Hiberante Detached Criteria with ProcedureEntity entities
     */
    private DetachedCriteria getDetachedCriteriaProceduresForObservablePropertyFromObservationConstellation(
            String observablePropertyIdentifier) {
        DetachedCriteria detachedCriteria = DetachedCriteria.forClass(ObservationConstellationEntity.class);
        detachedCriteria.add(Restrictions.eq(ObservationConstellationEntity.DELETED, false));
        detachedCriteria.createCriteria(ObservationConstellationEntity.OBSERVABLE_PROPERTY)
                .add(Restrictions.eq(PhenomenonEntity.PROPERTY_DOMAIN_ID, observablePropertyIdentifier));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(ObservationConstellationEntity.PROCEDURE)));
        return detachedCriteria;
    }

    /**
     * Get Hibernate Detached Criteria for class Series and observableProperty
     * identifier
     *
     * @param observablePropertyIdentifier
     *            ObservableProperty identifier parameter
     * @param session
     *            Hibernate session
     * @return Hiberante Detached Criteria with ProcedureEntity entities
     * @throws CodedException
     */
    private DetachedCriteria getDetachedCriteriaProceduresForObservablePropertyFromSeries(
            String observablePropertyIdentifier) throws OwsExceptionReport {
        DetachedCriteria detachedCriteria =
                DetachedCriteria.forClass(getDaoFactory().getSeriesDao(getSession()).getClass());
        detachedCriteria.add(Restrictions.eq(DatasetEntity.PROPERTY_DELETED, false));
        detachedCriteria.createCriteria(Series.OBSERVABLE_PROPERTY)
                .add(Restrictions.eq(PhenomenonEntity.PROPERTY_DOMAIN_ID, observablePropertyIdentifier));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(Series.PROCEDURE)));
        return detachedCriteria;
    }

    /**
     * Get Hibernate Detached Criteria for class ObservationConstellation and
     * offering identifier
     *
     * @param offeringIdentifier
     *            Offering identifier parameter
     * @param session
     *            Hibernate session
     * @return Detached Criteria with ProcedureEntity entities
     */
    private DetachedCriteria getDetachedCriteriaProceduresForOfferingFromObservationConstellation(
            String offeringIdentifier) {
        DetachedCriteria detachedCriteria = DetachedCriteria.forClass(ObservationConstellationEntity.class);
        detachedCriteria.add(Restrictions.eq(ObservationConstellationEntity.DELETED, false));
        detachedCriteria.createCriteria(ObservationConstellationEntity.OFFERING)
                .add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offeringIdentifier));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(ObservationConstellationEntity.PROCEDURE)));
        return detachedCriteria;
    }

    /**
     * Add procedure identifier restriction to Hibernate Criteria for series
     *
     * @param criteria
     *            Hibernate Criteria for series to add restriction
     * @param procedure
     *            ProcedureEntity identifier
     */
    private void addProcedureRestrictionForSeries(Criteria criteria, String procedure) {
        Criteria seriesCriteria = criteria.createCriteria(ContextualReferencedSeriesObservation.SERIES);
        seriesCriteria.createCriteria(ContextualReferencedSeriesObservation.PROCEDURE).add(
                Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, procedure));
    }

    /**
     * Add procedure identifier restriction to Hibernate Criteria
     *
     * @param criteria
     *            Hibernate Criteria to add restriction
     * @param procedure
     *            ProcedureEntity identifier
     */
    private void addProcedureRestrictionForObservation(Criteria criteria, String procedure) {
        criteria.createCriteria(ContextualReferencedObservation.PROCEDURE).add(Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, procedure));
    }

    @SuppressWarnings("unchecked")
    protected Set<String> getObservationIdentifiers(String procedureIdentifier) {
            Criteria criteria =
                    session.createCriteria(new EntitiyHelper().getObservationInfoEntityClass())
                            .setProjection(
                                    Projections.distinct(Projections.property(ContextualReferencedSeriesObservation.PROPERTY_DOMAIN_ID)))
                            .add(Restrictions.isNotNull(ContextualReferencedSeriesObservation.PROPERTY_DOMAIN_ID))
                            .add(Restrictions.eq(ContextualReferencedSeriesObservation.DELETED, false));
            Criteria seriesCriteria = criteria.createCriteria(ContextualReferencedSeriesObservation.SERIES);
            Criteria ocCriteria = seriesCriteria.createCriteria(DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION);
            ocCriteria.createCriteria(ObservationConstellationEntity.PROCEDURE)
                    .add(Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, procedureIdentifier));
            LOGGER.debug("QUERY getObservationIdentifiers(procedureIdentifier): {}",
                    DataModelUtil.getSqlString(criteria));
            return Sets.newHashSet(criteria.list());
    }

    public Map<String, String> getProcedureFormatMap(Session session) {
        if (HibernateHelper.isEntitySupported(ProcedureEntity.class)) {
            // get the latest validProcedureTimes' procedureDescriptionFormats
            return daoFactory.getValidProcedureTimeDao(getSession()).getTProcedureFormatMap(session);
        } else {
            Criteria criteria = getDefaultCriteria();
            criteria.createAlias(ProcedureEntity.PROPERTY_PROCEDURE_DESCRIPTION_FORMAT, "pdf");
            criteria.setProjection(Projections.projectionList().add(Projections.property(ProcedureEntity.PROPERTY_DOMAIN_ID))
                    .add(Projections.property("pdf." + ProcedureDescriptionFormatEntity.PROCEDURE_DESCRIPTION_FORMAT)));
            criteria.addOrder(Order.asc(ProcedureEntity.PROPERTY_DOMAIN_ID));
            LOGGER.debug("QUERY getProcedureFormatMap(): {}", DataModelUtil.getSqlString(criteria));
            @SuppressWarnings("unchecked")
            List<Object[]> results = criteria.list();
            Map<String, String> procedureFormatMap = Maps.newTreeMap();
            for (Object[] result : results) {
                String procedureIdentifier = (String) result[0];
                String format = (String) result[1];
                procedureFormatMap.put(procedureIdentifier, format);
            }
            return procedureFormatMap;
        }
    }

     private DetachedCriteria getDetachedCriteriaSeries(Session session) throws OwsExceptionReport {
         DetachedCriteria detachedCriteria = DetachedCriteria.forClass(getDaoFactory().getSeriesDao(getSession()).getSeriesClass());
         detachedCriteria.add(Restrictions.eq(DatasetEntity.PROPERTY_DELETED, false))
         .add(Restrictions.eq(DatasetEntity.PROPERTY_PUBLISHED, true));
         DetachedCriteria ocCriteria = detachedCriteria.createCriteria(DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION);
         ocCriteria.setProjection(Projections.distinct(Projections.property("oc." + ObservationConstellationEntity.PROCEDURE)));
//         detachedCriteria.createCriteria(DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION, "oc");
//         detachedCriteria.setProjection(Projections.distinct(Projections.property("oc." + ObservationConstellationEntity.PROCEDURE)));
         return detachedCriteria;
     }

    /**
     * ProcedureEntity time extrema {@link ResultTransformer}
     *
     * @author <a href="mailto:c.hollmann@52north.org">Carsten Hollmann</a>
     * @since 4.4.0
     *
     */
    private class ProcedureTimeTransformer implements ResultTransformer {
        private static final long serialVersionUID = -373512929481519459L;

        @Override
        public ProcedureTimeExtrema transformTuple(Object[] tuple, String[] aliases) {
            ProcedureTimeExtrema procedureTimeExtrema = new ProcedureTimeExtrema();
            if (tuple != null) {
                procedureTimeExtrema.setProcedure(tuple[0].toString());
                procedureTimeExtrema.setMinPhenomenonTime(DateTimeHelper.makeDateTime(tuple[1]));
                if (tuple.length == 4) {
                    DateTime maxPhenStart = DateTimeHelper.makeDateTime(tuple[2]);
                    DateTime maxPhenEnd = DateTimeHelper.makeDateTime(tuple[3]);
                    procedureTimeExtrema.setMaxPhenomenonTime(DateTimeHelper.max(maxPhenStart, maxPhenEnd));
                } else {
                    procedureTimeExtrema.setMaxPhenomenonTime(DateTimeHelper.makeDateTime(tuple[2]));
                }
            }
            return procedureTimeExtrema;
        }

        @Override
        @SuppressWarnings({ "rawtypes"})
        public List transformList(List collection) {
            return collection;
        }
    }

    public ProcedureEntity updateProcedure(ProcedureEntity procedure, SosProcedureDescription procedureDescription) {
        if (procedureDescription.getProcedureDescription() instanceof AbstractFeature) {
            AbstractFeature af = (AbstractFeature) procedureDescription.getProcedureDescription();
            if (af.isSetName()) {
                if (!procedure.isSetName() || (procedure.isSetName() && !af.getName().equals(procedure.getName()))) {
                    procedure.setName(af.getFirstName().getValue());
                }
                if (af.isSetDescription() && !af.getDescription().equals(procedure.getDescription())) {
                    procedure.setDescription(af.getDescription());
                }
            }
            getSession().saveOrUpdate(procedure);
            getSession().flush();
            getSession().refresh(procedure);
        }
        return procedure;

    }

}
