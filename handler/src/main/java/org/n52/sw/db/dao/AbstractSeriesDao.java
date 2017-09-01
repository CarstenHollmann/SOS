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
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.sql.JoinType;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.FeatureEntity;
import org.n52.series.db.beans.HibernateRelations;
import org.n52.series.db.beans.ObservationConstellationEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProcedureEntity;
import org.n52.series.db.dao.DatasetDao;
import org.n52.shetland.ogc.ows.exception.CodedException;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.request.GetObservationByIdRequest;
import org.n52.shetland.ogc.sos.request.GetObservationRequest;
import org.n52.shetland.util.CollectionHelper;
import org.n52.shetland.util.DateTimeHelper;
import org.n52.shetland.util.StringHelper;
import org.n52.sw.db.util.HibernateHelper;
import org.n52.sw.db.util.TimeExtrema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public abstract class AbstractSeriesDao
        extends
        DatasetDao<DatasetEntity>
        implements
        Dao,
        QueryConstants,
        TimeCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSeriesDao.class);
    private DaoFactory daoFactory;
    private boolean includeChildObservableProperties;

    public AbstractSeriesDao(DaoFactory daoFactory, Session session, boolean includeChildObservableProperties) {
        super(session);
        this.daoFactory = daoFactory;
        this.includeChildObservableProperties = includeChildObservableProperties;
    }

    @Override
    public DaoFactory getDaoFactory() {
        return daoFactory;
    }

    @Override
    public Session getSession() {
        return session;
    }

    protected boolean isIncludeChildObservableProperties() {
        return includeChildObservableProperties;
    }

    public abstract Class<?> getSeriesClass();

    /**
     * Get series for GetObservation request and featuresOfInterest
     *
     * @param request
     *            GetObservation request to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     *
     * @return Series that fit
     *
     * @throws OwsExceptionReport
     */
    public abstract List<DatasetEntity> get(GetObservationRequest request, Collection<String> features)
            throws OwsExceptionReport;

    /**
     * Get series for GetObservationByIdRequest request
     *
     * @param request
     *            GetObservationByIdRequest request to get series for
     * @return Series that fit
     * @throws CodedException
     */
    public abstract List<DatasetEntity> get(GetObservationByIdRequest request)
            throws OwsExceptionReport;

    /**
     * Query series for observedProiperty and featuresOfInterest
     *
     * @param observedProperty
     *            ObservedProperty to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     *
     * @return Series list
     */
    public abstract List<DatasetEntity> get(String observedProperty, Collection<String> features);

    /**
     * Query series for observedProiperty and featuresOfInterest
     *
     * @param procedure
     *            Procedure to get series for
     * @param observedProperty
     *            ObservedProperty to get series for
     * @param offering
     *            offering to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     * @return Series list
     */
    public abstract List<DatasetEntity> get(String procedure, String observedProperty, String offering,
            Collection<String> featureIdentifiers);

    /**
     * Create series for parameter
     *
     * @param procedures
     *            Procedures to get series for
     * @param observedProperties
     *            ObservedProperties to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     * @param session
     *            Hibernate session
     *
     * @return Series that fit
     */
    public abstract List<DatasetEntity> get(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features);

    /**
     * Create series for parameter
     *
     * @param procedures
     *            Procedures to get series for
     * @param observedProperties
     *            ObservedProperties to get series for
     * @param features
     *            FeaturesOfInterest to get series for
     * @param offerings
     *            Offerings to get series for
     * @param session
     *            Hibernate session
     * @return Series that fit
     */
    public abstract List<DatasetEntity> get(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> featuresOfInterest, Collection<String> offerings)
            throws OwsExceptionReport;

    /**
     * Get series for procedure, observableProperty and featureOfInterest
     *
     * @param procedure
     *            Procedure identifier parameter
     * @param observableProperty
     *            ObservableProperty identifier parameter
     * @param featureOfInterest
     *            FeatureOfInterest identifier parameter
     * @param session
     *            Hibernate session
     *
     * @return Matching series
     */
    public abstract DatasetEntity get(String procedure, String observableProperty, String featureOfInterest);


    protected abstract void addSpecificRestrictions(Criteria c, GetObservationRequest request)
            throws OwsExceptionReport;

    protected abstract DatasetEntity getImpl(ObservationContext ctx) throws OwsExceptionReport;
//    {
//        try {
//            return (DatasetEntity) getSeriesClass().newInstance();
//        } catch (InstantiationException | IllegalAccessException e) {
//            throw new NoApplicableCodeException().causedBy(e).withMessage("Error while creating an instance of %s",
//                    getSeriesClass().getCanonicalName());
//        }
//    }

    /**
     * Insert or update and get series for procedure, observable property and
     * featureOfInterest
     *
     * @param identifiers
     *            identifiers object
     * @param session
     *            Hibernate session
     *
     * @return Series object
     *
     * @throws OwsExceptionReport
     */
    public DatasetEntity getOrInsert(ObservationContext ctx)
            throws OwsExceptionReport {
        Criteria criteria = getDefaultAllCriteria();
        ctx.addIdentifierRestrictionsToCritera(criteria);
        LOGGER.debug("QUERY getOrInsertSeries(feature, observableProperty, procedure): {}",
                HibernateHelper.getSqlString(criteria));
        DatasetEntity series = (DatasetEntity) criteria.uniqueResult();
        if (series == null) {
            series = getImpl(ctx);
            series.setCategory(ctx.getCategory());
            series.setFeature(ctx.getFeatureOfInterest());
            // FIXME: hiddenChild?
            series.setObservationConstellation(getDaoFactory().getObservationConstellationDao(getSession())
                    .checkOrInsert(ctx.getProcedure(), ctx.getPhenomenon(), ctx.getOffering(), false));
            series.setDeleted(false);
            series.setPublished(true);
            series.setHiddenChild(ctx.isHiddenChild());
            getSession().saveOrUpdate(series);
            getSession().flush();
            getSession().refresh(series);
        } else if (series.isDeleted()) {
            series.setDeleted(false);
            getSession().saveOrUpdate(series);
            getSession().flush();
            getSession().refresh(series);
        }
        return series;
    }

    public List<DatasetEntity> get() {
        return getDefaultSeriesCriteria().list();
    }

    public Criteria getCriteria(GetObservationRequest request, Collection<String> features)
            throws OwsExceptionReport {
        final Criteria c = createCriteriaFor(request.getProcedures(), request.getObservedProperties(), features,
                request.getOfferings());
        addSpecificRestrictions(c, request);
        LOGGER.debug("QUERY getSeries(request, features): {}", HibernateHelper.getSqlString(c));
        return c;
    }

    public Criteria getCriteria(GetObservationByIdRequest request) {
        final Criteria c = getDefaultSeriesCriteria();
        c.add(Restrictions.in(DatasetEntity.PROPERTY_DOMAIN_ID, request.getObservationIdentifier()));
        LOGGER.debug("QUERY getCriteria(request): {}", HibernateHelper.getSqlString(c));
        return c;
    }

    public Criteria getCriteria(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features) {
        final Criteria c = createCriteriaFor(procedures, observedProperties, features);
        LOGGER.debug("QUERY getSeries(procedures, observableProperteies, features): {}",
                HibernateHelper.getSqlString(c));
        return c;
    }

    public Criteria getCriteria(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features, Collection<String> offerings) {
        final Criteria c = createCriteriaFor(procedures, observedProperties, features, offerings);
        LOGGER.debug("QUERY getSeries(proceedures, observableProperteies, features, offerings): {}",
                HibernateHelper.getSqlString(c));
        return c;
    }

    public Criteria getCriteria(String observedProperty, Collection<String> features) {
        final Criteria c = getDefaultSeriesCriteria();
        if (CollectionHelper.isNotEmpty(features)) {
            addFeatureOfInterestToCriteria(c, features);
        }
        if (!Strings.isNullOrEmpty(observedProperty)) {
            addObservablePropertyToCriteria(c, observedProperty);
        }
        return c;
    }

    public Criteria getCriteria(String procedure, String observedProperty, String offering,
            Collection<String> features) {
        final Criteria c = getDefaultSeriesCriteria();
        if (CollectionHelper.isNotEmpty(features)) {
            addFeatureOfInterestToCriteria(c, features);
        }
        if (!Strings.isNullOrEmpty(observedProperty)) {
            addObservablePropertyToCriteria(c, observedProperty);
        }
        if (!Strings.isNullOrEmpty(offering)) {
            addOfferingToCriteria(c, offering);
        }
        if (!Strings.isNullOrEmpty(procedure)) {
            addProcedureToCriteria(c, procedure);
        }
        return c;
    }

    public Criteria getCriteriaFor(String procedure, String observableProperty, String featureOfInterest) {
        final Criteria c = createCriteriaFor(procedure, observableProperty, featureOfInterest);
        LOGGER.debug("QUERY getSeriesFor(procedure, observableProperty, featureOfInterest): {}",
                HibernateHelper.getSqlString(c));
        return c;
    }

    /**
     * Add featureOfInterest restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param feature
     *            FeatureOfInterest identifier to add
     */
    public void addFeatureOfInterestToCriteria(Criteria c, String feature) {
        c.createCriteria(DatasetEntity.PROPERTY_FEATURE).add(Restrictions.eq(FeatureEntity.PROPERTY_DOMAIN_ID, feature));

    }

    /**
     * Add featureOfInterest restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param feature
     *            FeatureOfInterest to add
     */
    public void addFeatureOfInterestToCriteria(Criteria c, FeatureEntity feature) {
        c.add(Restrictions.eq(DatasetEntity.PROPERTY_FEATURE, feature));

    }

    /**
     * Add featuresOfInterest restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param features
     *            FeatureOfInterest identifiers to add
     */
    public void addFeatureOfInterestToCriteria(Criteria c, Collection<String> features) {
        c.createCriteria(DatasetEntity.PROPERTY_FEATURE).add(Restrictions.in(FeatureEntity.PROPERTY_DOMAIN_ID, features));

    }

    /**
     * Add observedProperty restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param observedProperty
     *            ObservableProperty identifier to add
     */
    public void addObservablePropertyToCriteria(Criteria c, String observedProperty) {
        addObservationConstellationCriteria(c).createCriteria(ObservationConstellationEntity.OBSERVABLE_PROPERTY)
                .add(Restrictions.eq(PhenomenonEntity.PROPERTY_DOMAIN_ID, observedProperty));
    }

    /**
     * Add observedProperty restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param observedProperty
     *            ObservableProperty to add
     */
    public void addObservablePropertyToCriteria(Criteria c, PhenomenonEntity observedProperty) {
        addObservationConstellationCriteria(c).add(Restrictions.eq(ObservationConstellationEntity.OBSERVABLE_PROPERTY, observedProperty));
    }

    /**
     * Add observedProperties restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param observedProperties
     *            ObservableProperty identifiers to add
     */
    public void addObservablePropertyToCriteria(Criteria c, Collection<String> observedProperties) {
        addObservationConstellationCriteria(c).createCriteria(ObservationConstellationEntity.OBSERVABLE_PROPERTY)
                .add(Restrictions.in(PhenomenonEntity.PROPERTY_DOMAIN_ID, observedProperties));
    }

    /**
     * Add procedure restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param procedure
     *            Procedure identifier to add
     */
    public void addProcedureToCriteria(Criteria c, String procedure) {
        addObservationConstellationCriteria(c).createCriteria(ObservationConstellationEntity.PROCEDURE)
                .add(Restrictions.eq(ProcedureEntity.PROPERTY_DOMAIN_ID, procedure));
    }

    /**
     * Add procedure restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param procedure
     *            Procedure to add
     */
    public void addProcedureToCriteria(Criteria c, ProcedureEntity procedure) {
        addObservationConstellationCriteria(c).add(Restrictions.eq(ObservationConstellationEntity.PROCEDURE, procedure));
    }

    /**
     * Add procedures restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param procedures
     *            Procedure identifiers to add
     */
    public void addProcedureToCriteria(Criteria c, Collection<String> procedures) {
        addObservationConstellationCriteria(c).createCriteria(ObservationConstellationEntity.PROCEDURE)
                .add(Restrictions.in(ProcedureEntity.PROPERTY_DOMAIN_ID, procedures));

    }

    /**
     * Add offering restriction to Hibernate Criteria with LEFT-OUTER-JOIN
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param offerings
     *            Offering identifiers to add
     * @throws OwsExceptionReport
     */
    public void addOfferingToCriteria(Criteria c, Collection<String> offerings) {
        addObservationConstellationCriteria(c).createCriteria(ObservationConstellationEntity.OFFERING)
        .add(Restrictions.in(OfferingEntity.PROPERTY_DOMAIN_ID, offerings));

    }

    public void addOfferingToCriteria(Criteria c, String offering) {
        addObservationConstellationCriteria(c).createCriteria(ObservationConstellationEntity.OFFERING)
            .add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering));
    }

    public void addOfferingToCriteria(Criteria c, OfferingEntity offering) {
        addObservationConstellationCriteria(c)
                .add(Restrictions.eq(ObservationConstellationEntity.OFFERING, offering));
    }

    private Criteria addObservationConstellationCriteria(Criteria c) {
        return c.createCriteria(DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION);
    }

    /**
     * Get default Hibernate Criteria for querying series, deleted flag ==
     * <code>false</code>
     *
     * @return Default criteria
     */
    public Criteria getDefaultSeriesCriteria() {
        Criteria c = getDefaultAllCriteria().add(Restrictions.eq(DatasetEntity.PROPERTY_DELETED, false))
                .add(Restrictions.eq(DatasetEntity.PROPERTY_PUBLISHED, true));
        if (!isIncludeChildObservableProperties()) {
            c.add(Restrictions.eq(DatasetEntity.HIDDEN_CHILD, false));
        }
        return c;
    }

    /**
     * Get default Hibernate Criteria for querying all series
     *
     * @return Default criteria
     */
    public Criteria getDefaultAllCriteria() {
        return session.createCriteria(getSeriesClass()).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    /**
     * Update Series for procedure by setting deleted flag and return changed
     * series
     *
     * @param procedure
     *            Procedure for which the series should be changed
     * @param deleteFlag
     *            New deleted flag value
     * @param session
     *            Hibernate session
     *
     * @return Updated Series
     */
    @SuppressWarnings("unchecked")
    public List<DatasetEntity> updateSeriesSetAsDeletedForProcedureAndGetSeries(String procedure, boolean deleteFlag,
            Session session) {
        Criteria criteria = getDefaultAllCriteria();
        addProcedureToCriteria(criteria, procedure);
        List<DatasetEntity> hSeries = criteria.list();
        for (DatasetEntity series : hSeries) {
            series.setDeleted(deleteFlag);
            session.saveOrUpdate(series);
            session.flush();
        }
        return hSeries;
    }

    /**
     * Update series values which will be used by the Timeseries API. Can be
     * later used by the SOS.
     *
     * @param series
     *            Series object
     * @param hObservation
     *            Observation object
     * @param session
     *            Hibernate session
     */
    public void updateSeriesWithFirstLatestValues(DatasetEntity series, DataEntity<?> hObservation) {
        if (!series.isSetFirstValueAt() || (series.isSetFirstValueAt()
                && series.getFirstValueAt().after(hObservation.getPhenomenonTimeStart()))) {
            series.setFirstValueAt(hObservation.getPhenomenonTimeStart());
        }
        if (!series.isSetLastValueAt() || (series.isSetLastValueAt()
                && series.getLastValueAt().before(hObservation.getPhenomenonTimeEnd()))) {
            series.setLastValueAt(hObservation.getPhenomenonTimeEnd());
        }
        if (hObservation instanceof HibernateRelations.HasUnit) {
            if (!series.hasUnit() && ((HibernateRelations.HasUnit)hObservation).isSetUnit()) {
                // TODO check if both unit are equal. If not throw exception?
                series.setUnit(((HibernateRelations.HasUnit)hObservation).getUnit());
            }
        }
        getSession().saveOrUpdate(series);
        getSession().flush();
    }

    /**
     * Check {@link Series} if the deleted observation time stamp corresponds to
     * the first/last series time stamp
     *
     * @param series
     *            Series to update
     * @param observation
     *            Deleted observation
     * @param session
     *            Hibernate session
     */
    public void updateSeriesAfterObservationDeletion(DatasetEntity series, DataEntity<?> observation,
            Session session) {
        DataDao seriesObservationDAO = getDaoFactory().getSeriesDao(getSession());
        if (series.isSetFirstValueAt()&& series.getFirstValueAt().equals(observation.getPhenomenonTimeStart())) {
            DataEntity<?> firstObservation = seriesObservationDAO.getFirstObservationFor(series, session);
            if (firstObservation != null) {
                series.setFirstValueAt(firstObservation.getPhenomenonTimeStart());
            } else {
                series.setFirstValueAt(null);
            }
        }
        if (series.isSetLastValueAt() && series.getLastValueAt().equals(observation.getPhenomenonTimeEnd())) {
            DataEntity<?> latestObservation = seriesObservationDAO.getLastObservationFor(series, session);
            if (latestObservation != null) {
                series.setLastValueAt(latestObservation.getPhenomenonTimeEnd());
            } else {
                series.setLastValueAt(null);
            }
        }
        if (!series.isSetFirstValueAt()) {
            series.setUnit(null);
        }
        session.saveOrUpdate(series);
    }

    public TimeExtrema getProcedureTimeExtrema(Session session, String procedure) {
        Criteria c = getDefaultSeriesCriteria();
        addProcedureToCriteria(c, procedure);
        ProjectionList projectionList = Projections.projectionList();
        projectionList.add(Projections.min(DatasetEntity.PROPERTY_FIRST_VALUE_AT));
        projectionList.add(Projections.max(DatasetEntity.PROPERTY_LAST_VALUE_AT));
        c.setProjection(projectionList);
        LOGGER.debug("QUERY getProcedureTimeExtrema(procedureIdentifier): {}", HibernateHelper.getSqlString(c));
        Object[] result = (Object[]) c.uniqueResult();

        TimeExtrema pte = new TimeExtrema();
        if (result != null) {
            pte.setMinPhenomenonTime(DateTimeHelper.makeDateTime(result[0]));
            pte.setMaxPhenomenonTime(DateTimeHelper.makeDateTime(result[1]));
        }
        return pte;
    }

    /**
     * Create series query criteria for parameter
     *
     * @param procedures
     *            Procedures to get series for
     * @param observedProperties
     *            ObservedProperties to get series for
     * @param features
     *            FeatureOfInterest to get series for
     * @param session
     *            Hibernate session
     *
     * @return Criteria to query series
     */
    private Criteria createCriteriaFor(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features) {
        final Criteria c = getDefaultSeriesCriteria();
        if (CollectionHelper.isNotEmpty(features)) {
            addFeatureOfInterestToCriteria(c, features);
        }
        if (CollectionHelper.isNotEmpty(observedProperties)) {
            addObservablePropertyToCriteria(c, observedProperties);
        }
        if (CollectionHelper.isNotEmpty(procedures)) {
            addProcedureToCriteria(c, procedures);
        }
        return c;
    }

    private Criteria createCriteriaFor(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features, Collection<String> offerings) {
        final Criteria c = createCriteriaFor(procedures, observedProperties, features);
        if (CollectionHelper.isNotEmpty(offerings)) {
            addOfferingToCriteria(c, offerings);
        }
        return c;
    }

    /**
     * Get series query Hibernate Criteria for procedure, observableProperty and
     * featureOfInterest
     *
     * @param procedure
     *            Procedure to get series for
     * @param observedProperty
     *            ObservedProperty to get series for
     * @param feature
     *            FeatureOfInterest to get series for
     * @param session
     *            Hibernate session
     *
     * @return Criteria to query series
     */
    private Criteria createCriteriaFor(String procedure, String observedProperty, String feature) {
        final Criteria c = getDefaultSeriesCriteria();
        if (Strings.isNullOrEmpty(feature)) {
            addFeatureOfInterestToCriteria(c, feature);
        }
        if (Strings.isNullOrEmpty(observedProperty)) {
            addObservablePropertyToCriteria(c, observedProperty);
        }
        if (Strings.isNullOrEmpty(procedure)) {
            addProcedureToCriteria(c, procedure);
        }
        return c;
    }

}
