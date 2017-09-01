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

import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.Subqueries;
import org.hibernate.sql.JoinType;
import org.hibernate.transform.ResultTransformer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.FeatureTypeEntity;
import org.n52.series.db.beans.ObservationConstellationEntity;
import org.n52.series.db.beans.ObservationTypeEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProcedureEntity;
import org.n52.series.db.beans.RelatedFeatureEntity;
import org.n52.shetland.ogc.gml.time.TimePeriod;
import org.n52.shetland.ogc.ows.exception.CodedException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.SosOffering;
import org.n52.shetland.util.CollectionHelper;
import org.n52.shetland.util.DateTimeHelper;
import org.n52.sw.db.util.HibernateHelper;
import org.n52.sw.db.util.NoopTransformerAdapter;
import org.n52.sw.db.util.OfferingTimeExtrema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class OfferingDao
        extends
        org.n52.series.db.dao.OfferingDao
        implements
        Dao, TimeCreator, QueryConstants {

    private static Logger LOGGER = LoggerFactory.getLogger(OfferingDao.class);
    private static final String SQL_QUERY_OFFERING_TIME_EXTREMA = "getOfferingTimeExtrema";
    private static final String SQL_QUERY_GET_MIN_DATE_FOR_OFFERING = "getMinDate4Offering";
    private static final String SQL_QUERY_GET_MAX_DATE_FOR_OFFERING = "getMaxDate4Offering";
    private static final String SQL_QUERY_GET_MIN_RESULT_TIME_FOR_OFFERING = "getMinResultTime4Offering";
    private static final String SQL_QUERY_GET_MAX_RESULT_TIME_FOR_OFFERING = "getMaxResultTime4Offering";
    private final OfferingeTimeTransformer transformer = new OfferingeTimeTransformer();

    private DaoFactory daoFactory;

    public OfferingDao(DaoFactory daoFactory, Session session) {
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
     * Get transactional offering object for identifier
     *
     * @param identifier
     *            OfferingEntity identifier
     * @return Transactional offering object
     */
    public OfferingEntity get(String identifier) {
        Criteria criteria =
                getDefaultCriteria().add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, identifier));
        LOGGER.debug("QUERY get(): {}", HibernateHelper.getSqlString(criteria));
        return (OfferingEntity) criteria.uniqueResult();
    }

    /**
     * Get all offering objects
     *
     * @return OfferingEntity objects
     */
    @SuppressWarnings("unchecked")
    public List<OfferingEntity> get() {
        Criteria criteria = session.createCriteria(OfferingEntity.class);
        LOGGER.debug("QUERY get(): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    public Map<String, Collection<String>> getParentMap(Session session) {
        Criteria criteria = getDefaultCriteria();
        ProjectionList projectionList = Projections.projectionList();
        projectionList.add(Projections.property(OfferingEntity.PROPERTY_DOMAIN_ID));
        criteria.createAlias(OfferingEntity.PROPERTY_PARENTS, "po", JoinType.LEFT_OUTER_JOIN);
        projectionList.add(Projections.property("po." + OfferingEntity.PROPERTY_DOMAIN_ID));
        criteria.setProjection(projectionList);
        // return as List<Object[]> even if there's only one column for
        // consistency
        criteria.setResultTransformer(NoopTransformerAdapter.INSTANCE);

        LOGGER.debug("QUERY getParentMap(): {}", HibernateHelper.getSqlString(criteria));
        @SuppressWarnings("unchecked")
        List<Object[]> results = criteria.list();
        Map<String, Collection<String>> map = Maps.newHashMap();
        for (Object[] result : results) {
            String offeringIdentifier = (String) result[0];
            String parentOfferingIdentifier = null;
            parentOfferingIdentifier = (String) result[1];
            if (parentOfferingIdentifier == null) {
                map.put(offeringIdentifier, null);
            } else {
                CollectionHelper.addToCollectionMap(offeringIdentifier, parentOfferingIdentifier, map);
            }
        }
        return map;
    }

    /**
     * Get offering objects for cache update
     *
     * @param identifiers
     *            Optional collection of offering identifiers to fetch. If null,
     *            all offerings are returned.
     * @return OfferingEntity objects
     */
    @SuppressWarnings("unchecked")
    public List<OfferingEntity> get(Collection<String> identifiers) {
        Criteria criteria = getDefaultCriteria();
        if (CollectionHelper.isNotEmpty(identifiers)) {
            criteria.add(Restrictions.in(OfferingEntity.PROPERTY_DOMAIN_ID, identifiers));
        }
        LOGGER.debug("QUERY get(): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get offering identifiers for procedure identifier
     *
     * @param procedureIdentifier
     *            Procedure identifier
     * @return OfferingEntity identifiers
     * @throws OwsExceptionReport
     */
    @SuppressWarnings("unchecked")
    public List<String> getForProcedure(String procedureIdentifier)
            throws OwsExceptionReport {
        Criteria c;
            c = getDefaultCriteria();
            c.add(Subqueries.propertyIn(OfferingEntity.PROPERTY_ID,
                    getDetachedCriteriaOfferingForProcedureFromObservationConstellation(procedureIdentifier)));
            c.setProjection(Projections.distinct(Projections.property(OfferingEntity.PROPERTY_DOMAIN_ID)));
        LOGGER.debug(
                "QUERY getOfferingIdentifiersForProcedure(procedureIdentifier): {}",
                HibernateHelper.getSqlString(c));
        return c.list();
    }

    /**
     * Get offering identifiers for observable property identifier
     *
     * @param observablePropertyIdentifier
     *            Observable property identifier
     * @return OfferingEntity identifiers
     * @throws CodedException
     */
    @SuppressWarnings("unchecked")
    public Collection<String> getObservableProperty(String observablePropertyIdentifier) throws OwsExceptionReport {
        Criteria c;
            c = getDefaultCriteria();
            c.add(Subqueries.propertyIn(OfferingEntity.PROPERTY_ID,
                    getDetachedCriteriaOfferingForObservablePropertyFromObservationConstellation(
                            observablePropertyIdentifier)));
            c.setProjection(Projections.distinct(Projections.property(OfferingEntity.PROPERTY_DOMAIN_ID)));
        LOGGER.debug(
                "QUERY getObservableProperty(observablePropertyIdentifier): {}",
                HibernateHelper.getSqlString(c));
        return c.list();
    }

    /**
     * Get offering time extrema
     *
     * @param identifiers
     *            Optional collection of offering identifiers to fetch. If null,
     *            all offerings are returned.
     * @return Map of offering time extrema, keyed by offering identifier
     * @throws CodedException
     */
    @SuppressWarnings("unchecked")
    public Map<String, OfferingTimeExtrema> getTimeExtrema(Collection<String> identifiers) throws OwsExceptionReport {
        List<OfferingTimeExtrema> results = null;
        if (HibernateHelper.isNamedQuerySupported(SQL_QUERY_OFFERING_TIME_EXTREMA, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_OFFERING_TIME_EXTREMA);
            if (CollectionHelper.isNotEmpty(identifiers)) {
                namedQuery.setParameterList("identifiers", identifiers);
            }
            LOGGER.debug("QUERY getOfferingTimeExtrema() with NamedQuery: {}", SQL_QUERY_OFFERING_TIME_EXTREMA);
            namedQuery.setResultTransformer(transformer);
            results = namedQuery.list();
        } else {
            Criteria criteria = getDefaultCriteria().setProjection(
                            Projections.projectionList().add(Projections.groupProperty(OfferingEntity.PROPERTY_DOMAIN_ID))
                                    .add(Projections.min(OfferingEntity.PROPERTY_PHENOMENON_TIME_START))
                                    .add(Projections.max(OfferingEntity.PROPERTY_PHENOMENON_TIME_END))
                                    .add(Projections.min(OfferingEntity.PROPERTY_RESULT_TIME_START))
                                    .add(Projections.max(OfferingEntity.PROPERTY_RESULT_TIME_END)));
            if (CollectionHelper.isNotEmpty(identifiers)) {
                criteria.add(Restrictions.in(OfferingEntity.PROPERTY_DOMAIN_ID, identifiers));
            }
            LOGGER.debug("QUERY getTimeExtrema(): {}", HibernateHelper.getSqlString(criteria));
            criteria.setResultTransformer(transformer);
            results = criteria.list();
        }

        Map<String, OfferingTimeExtrema> map = Maps.newHashMap();
        for (OfferingTimeExtrema result : results) {
            if (result.isSetOffering()) {
                map.put(result.getOffering(), result);
            }
        }
        return map;
    }

    /**
     * Get min time from observations for offering
     *
     * @param offering
     *            OfferingEntity identifier
     * @return min time for offering
     * @throws CodedException
     */
    public DateTime getMinDate(String offering) throws OwsExceptionReport {
        Object min;
        if (HibernateHelper.isNamedQuerySupported(SQL_QUERY_GET_MIN_DATE_FOR_OFFERING, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_MIN_DATE_FOR_OFFERING);
            namedQuery.setParameter(OFFERING, offering);
            LOGGER.debug("QUERY getMinDate(offering) with NamedQuery: {}",
                    SQL_QUERY_GET_MIN_DATE_FOR_OFFERING);
            min = namedQuery.uniqueResult();
        } else {
            Criteria criteria = getDefaultCriteria()
                    .add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering))
                    .setProjection(Projections.min(OfferingEntity.PROPERTY_PHENOMENON_TIME_START));
            LOGGER.debug("QUERY Series-getMinDate(offering): {}", HibernateHelper.getSqlString(criteria));
            min = criteria.uniqueResult();
        }
        if (min != null) {
            return new DateTime(min, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Get max time from observations for offering
     *
     * @param offering
     *            OfferingEntity identifier
     * @return max time for offering
     * @throws CodedException
     */
    public DateTime getMaxDate(String offering)
            throws OwsExceptionReport {
        Object max;
        if (HibernateHelper.isNamedQuerySupported(SQL_QUERY_GET_MAX_DATE_FOR_OFFERING, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_MAX_DATE_FOR_OFFERING);
            namedQuery.setParameter(OFFERING, offering);
            LOGGER.debug("QUERY getMaxDate(offering) with NamedQuery: {}", SQL_QUERY_GET_MAX_DATE_FOR_OFFERING);
            max = namedQuery.uniqueResult();
        } else {
            Criteria criteria = getDefaultCriteria().add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering))
                    .setProjection(Projections.min(OfferingEntity.PROPERTY_PHENOMENON_TIME_START));
            LOGGER.debug("QUERY Series-getMinDate(offering): {}", HibernateHelper.getSqlString(criteria));
            max = criteria.uniqueResult();
        }
        if (max != null) {
            return new DateTime(max, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Get min result time from observations for offering
     *
     * @param offering
     *            OfferingEntity identifier
     *
     * @return min result time for offering
     * @throws CodedException
     */
    public DateTime getMinResultTime(String offering) throws OwsExceptionReport {
        Object min;
        if (HibernateHelper.isNamedQuerySupported(SQL_QUERY_GET_MIN_RESULT_TIME_FOR_OFFERING, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_MIN_RESULT_TIME_FOR_OFFERING);
            namedQuery.setParameter(OFFERING, offering);
            LOGGER.debug("QUERY getMinResultTime(offering) with NamedQuery: {}",
                    SQL_QUERY_GET_MIN_RESULT_TIME_FOR_OFFERING);
            min = namedQuery.uniqueResult();
        } else {
            Criteria criteria = getDefaultCriteria()
                    .add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering))
                    .setProjection(Projections.min(OfferingEntity.PROPERTY_RESULT_TIME_START));
            LOGGER.debug("QUERY getMinResultTime(offering): {}", HibernateHelper.getSqlString(criteria));
            min = criteria.uniqueResult();
        }
        if (min != null) {
            return new DateTime(min, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Get max result time from observations for offering
     *
     * @param offering
     *            OfferingEntity identifier
     *
     * @return max result time for offering
     * @throws CodedException
     */
    public DateTime getMaxResultTime(String offering) throws OwsExceptionReport {
        Object max;
        if (HibernateHelper.isNamedQuerySupported(SQL_QUERY_GET_MAX_RESULT_TIME_FOR_OFFERING, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_GET_MAX_RESULT_TIME_FOR_OFFERING);
            namedQuery.setParameter(OFFERING, offering);
            LOGGER.debug("QUERY getMaxResultTime(offering) with NamedQuery: {}",
                    SQL_QUERY_GET_MAX_RESULT_TIME_FOR_OFFERING);
            max = namedQuery.uniqueResult();
        } else {
            Criteria criteria = getDefaultCriteria().add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering))
                    .setProjection(Projections.min(OfferingEntity.PROPERTY_RESULT_TIME_END));
            LOGGER.debug("QUERY getMaxResultTime(offering): {}", HibernateHelper.getSqlString(criteria));
            max = criteria.uniqueResult();
        }
        if (max != null) {
            return new DateTime(max, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Get temporal bounding box for each offering
     *
     * @param session
     *            Hibernate session
     * @return a Map containing the temporal bounding box for each offering
     * @throws CodedException
     */
    public Map<String, TimePeriod> getTemporalBoundingBoxes()
            throws OwsExceptionReport {
        Criteria criteria = getDefaultCriteria().setProjection(
                Projections.projectionList()
                        .add(Projections.min(OfferingEntity.PROPERTY_PHENOMENON_TIME_START))
                        .add(Projections.max(OfferingEntity.PROPERTY_PHENOMENON_TIME_END))
                        .add(Projections.groupProperty(OfferingEntity.PROPERTY_DOMAIN_ID)));
        LOGGER.debug("QUERY getTemporalBoundingBoxes(): {}", HibernateHelper.getSqlString(criteria));
        final List<?> temporalBoundingBoxes = criteria.list();
        if (!temporalBoundingBoxes.isEmpty()) {
            final HashMap<String, TimePeriod> temporalBBoxMap = new HashMap<>(temporalBoundingBoxes.size());
            for (Object recordObj : temporalBoundingBoxes) {
                if (recordObj instanceof Object[]) {
                    final Object[] record = (Object[]) recordObj;
                    final TimePeriod value =
                            createTimePeriod((Timestamp) record[0], (Timestamp) record[1]);
                    temporalBBoxMap.put((String) record[2], value);
                }
            }
            LOGGER.debug(temporalBoundingBoxes.toString());
            return temporalBBoxMap;
        }
        return new HashMap<>(0);
    }

    /**
     * Insert or update and get offering
     *
     * @param assignedOffering
     *            SosOfferingEntity to insert, update or get
     * @param relatedFeatures
     *            Related feature objects
     * @param observationTypes
     *            Allowed observation type objects
     * @param featureOfInterestTypes
     *            Allowed featureOfInterest type objects
     * @param session
     *            Hibernate session
     * @return OfferingEntity object
     */
    public OfferingEntity getAndUpdateOrInsert(SosOffering assignedOffering, List<RelatedFeatureEntity> relatedFeatures,
            List<ObservationTypeEntity> observationTypes, List<FeatureTypeEntity> featureOfInterestTypes) {
        OfferingEntity offering = get(assignedOffering.getIdentifier());
        if (offering == null) {
            offering = new OfferingEntity();
            offering.setIdentifier(assignedOffering.getIdentifier());
            if (assignedOffering.isSetName()) {
                offering.setName(assignedOffering.getFirstName().getValue());
            } else {
                offering.setName("OfferingEntity for the procedure " + assignedOffering.getIdentifier());
            }
            if (assignedOffering.isSetDescription()) {
                offering.setDescription(assignedOffering.getDescription());
            }
        }
        if (!relatedFeatures.isEmpty()) {
            offering.setRelatedFeatures(new HashSet<>(relatedFeatures));
        } else {
            offering.setRelatedFeatures(new HashSet<RelatedFeatureEntity>(0));
        }
        if (!observationTypes.isEmpty()) {
            offering.setObservationTypes(new HashSet<>(observationTypes));
        } else {
            offering.setObservationTypes(new HashSet<ObservationTypeEntity>(0));
        }
        if (!featureOfInterestTypes.isEmpty()) {
            offering.setFeatureTypes(new HashSet<>(featureOfInterestTypes));
        } else {
            offering.setFeatureTypes(new HashSet<FeatureTypeEntity>(0));
        }
        getSession().saveOrUpdate(offering);
        getSession().flush();
        getSession().refresh(offering);
        return offering;
    }

    /**
     * Get Hibernate Detached Criteria for class ObservationConstellation and
     * observableProperty identifier
     *
     * @param observablePropertyIdentifier
     *            ObservableProperty identifier parameter
     * @param session
     *            Hibernate session
     * @return Detached Criteria with OfferingEntity entities as result
     */
    private DetachedCriteria getDetachedCriteriaOfferingForObservablePropertyFromObservationConstellation(
            String observablePropertyIdentifier) {
        final DetachedCriteria detachedCriteria = DetachedCriteria.forClass(ObservationConstellationEntity.class);
        detachedCriteria.add(Restrictions.eq(ObservationConstellation.DELETED, false));
        detachedCriteria.createCriteria(ObservationConstellationEntity.OBSERVABLE_PROPERTY)
                .add(Restrictions.eq(PhenomenonEntity.PROPERTY_DOMAIN_ID, observablePropertyIdentifier));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(ObservationConstellationEntity.OFFERING)));
        return detachedCriteria;
    }

    /**
     * Get Hibernate Detached Criteria for class ObservationConstellation and
     * procedure identifier
     *
     * @param procedureIdentifier
     *            Procedure identifier parameter
     * @param session
     *            Hibernate session
     * @return Detached Criteria with OfferingEntity entities as result
     */
    private DetachedCriteria getDetachedCriteriaOfferingForProcedureFromObservationConstellation(
            String procedureIdentifier) {
        final DetachedCriteria detachedCriteria = DetachedCriteria.forClass(ObservationConstellationEntity.class);
        detachedCriteria.add(Restrictions.eq(ObservationConstellationEntity.DELETED, false));
        detachedCriteria.createCriteria(ObservationConstellationEntity.PROCEDURE)
                .add(Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, procedureIdentifier));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(ObservationConstellationEntity.OFFERING)));
        return detachedCriteria;
    }

    /**
     * Query allowed FeatureOfInterestTypes for offering
     *
     * @param offeringIdentifier
     *            OfferingEntity identifier
     * @param session
     *            Hibernate session
     * @return Allowed FeatureOfInterestTypes
     */
    public List<String> getAllowedFeatureTypes(String offeringIdentifier) {
        if (HibernateHelper.isEntitySupported(OfferingEntity.class)) {
            Criteria criteria = getDefaultCriteria()
                    .add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offeringIdentifier));
            LOGGER.debug("QUERY getAllowedFeatureTypes(offering): {}",
                    HibernateHelper.getSqlString(criteria));
            OfferingEntity offering = (OfferingEntity) criteria.uniqueResult();
            if (offering != null) {
                List<String> list = Lists.newArrayList();
                for (FeatureTypeEntity featureOfInterestType : offering.getFeatureTypes()) {
                    list.add(featureOfInterestType.getType());
                }
                return list;
            }
        }
        return Lists.newArrayList();
    }


    /**
     * Add offering identifier restriction to Hibernate Criteria
     *
     * @param criteria
     *            Hibernate Criteria to add restriction
     * @param offering
     *            OfferingEntity identifier
     */
    public void addOfferingRestricionForSeries(Criteria c, String offering) {
        addOfferingRestrictionFor(c, offering, DatasetEntity.OFFERING);
    }

    private void addOfferingRestrictionFor(Criteria c, String offering, String associationPath) {
        c.createCriteria(associationPath).add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering));
    }

    private DetachedCriteria getDetachedCriteriaSeries(Session session) throws OwsExceptionReport {
        final DetachedCriteria detachedCriteria =
                DetachedCriteria.forClass(getDaoFactory().getSeriesDAO(getSession()).getSeriesClass());
        detachedCriteria.add(Restrictions.eq(DatasetEntity.PROPERTY_DELETED, false)).add(Restrictions.eq(DatasetEntity.PROPERTY_PUBLISHED, true));
        detachedCriteria.setProjection(Projections.distinct(Projections.property(Series.OFFERING)));
        return detachedCriteria;
    }

    protected Criteria getDefaultCriteria() {
        return getSession().createCriteria(OfferingEntity.class).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    /**
     * OfferingEntity time extrema {@link ResultTransformer}
     *
     * @author <a href="mailto:c.hollmann@52north.org">Carsten Hollmann</a>
     * @since 4.4.0
     *
     */
    private class OfferingeTimeTransformer implements ResultTransformer {
        private static final long serialVersionUID = -373512929481519459L;

        @Override
        public OfferingTimeExtrema transformTuple(Object[] tuple, String[] aliases) {
            OfferingTimeExtrema offeringTimeExtrema = new OfferingTimeExtrema();
            if (tuple != null) {
                offeringTimeExtrema.setOffering(tuple[0].toString());
                offeringTimeExtrema.setMinPhenomenonTime(DateTimeHelper.makeDateTime(tuple[1]));
                offeringTimeExtrema.setMaxPhenomenonTime(DateTimeHelper.makeDateTime(tuple[2]));
                offeringTimeExtrema.setMinResultTime(DateTimeHelper.makeDateTime(tuple[3]));
                offeringTimeExtrema.setMaxResultTime(DateTimeHelper.makeDateTime(tuple[4]));
            }
            return offeringTimeExtrema;
        }

        @Override
        @SuppressWarnings({ "rawtypes" })
        public List transformList(List collection) {
            return collection;
        }
    }

}
