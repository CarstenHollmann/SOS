package org.n52.sw.db.dao;

import static org.hibernate.criterion.Restrictions.eq;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.ScrollMode;
import org.hibernate.ScrollableResults;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.ProjectionList;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.spatial.criterion.SpatialProjections;
import org.hibernate.sql.JoinType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.n52.iceland.service.ServiceConfiguration;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.ObservationConstellationEntity;
import org.n52.shetland.ogc.gml.time.IndeterminateValue;
import org.n52.shetland.ogc.om.OmObservation;
import org.n52.shetland.ogc.ows.exception.CodedException;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.ExtendedIndeterminateTime;
import org.n52.shetland.ogc.sos.request.AbstractObservationRequest;
import org.n52.shetland.ogc.sos.request.GetObservationByIdRequest;
import org.n52.shetland.ogc.sos.request.GetObservationRequest;
import org.n52.shetland.util.CollectionHelper;
import org.n52.shetland.util.DateTimeHelper;
import org.n52.shetland.util.StringHelper;
import org.n52.sos.ds.hibernate.entities.FeatureOfInterest;
import org.n52.sos.ds.hibernate.entities.ObservableProperty;
import org.n52.sos.ds.hibernate.entities.Offering;
import org.n52.sos.ds.hibernate.entities.Procedure;
import org.n52.sos.ds.hibernate.entities.observation.series.Series;
import org.n52.sos.ds.hibernate.entities.observation.series.SeriesObservation;
import org.n52.sos.ds.hibernate.entities.observation.series.TemporalReferencedSeriesObservation;
import org.n52.sos.ds.hibernate.entities.values.ObservationValueTime;
import org.n52.sos.ogc.sos.SosConstants.SosIndeterminateTime;
import org.n52.sos.util.GeometryHandler;
import org.n52.sw.db.util.HibernateHelper;
import org.n52.sw.db.util.ObservationTimeExtrema;
import org.n52.sw.db.util.QueryHelper;
import org.n52.sw.db.util.ScrollableIterable;
import org.n52.sw.db.util.TimeExtrema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public abstract class AbstractSeriesObservationDao extends AbstractObservationDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSeriesObservationDao.class);

    public AbstractSeriesObservationDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }

    @Override
    protected void addObservationContextToObservation(ObservationContext ctx,
        AbstractSeriesDao seriesDAO = getDaoFactory().getSeriesDao(getSession());
       DatasetEntity series = seriesDAO.getOrInsert(ctx);
       observation.addDataset(series);
        seriesDAO.updateSeriesWithFirstLatestValues(series, observation,);
    }

    @Override
    public Criteria getObservationInfoCriteriaForFeatureOfInterestAndProcedure(String feature, String procedure) {
        Criteria criteria = getDefaultObservationInfoCriteria();
        Criteria seriesCriteria = criteria.createCriteria(ContextualReferencedDataEntity.SERIES);
        seriesCriteria.createCriteria(DatasetEntity.PROPERTY_FEATURE).add(eq(FeatureOfInterest.IDENTIFIER, feature));
        seriesCriteria.createCriteria(Series.PROCEDURE).add(eq(Procedure.IDENTIFIER, procedure));

        if (!isIncludeChildObservableProperties()) {
            seriesCriteria.createCriteria(DataEntity.VALUE).createCriteria(DataEntity.OBS_ID);
        }

        return criteria;
    }

    @Override
    public Criteria getObservationInfoCriteriaForFeatureOfInterestAndOffering(String feature, String offering) {
        Criteria criteria = getDefaultObservationInfoCriteria();
        Criteria seriesCriteria = criteria.createCriteria(ContextualReferencedDataEntity.SERIES);
        seriesCriteria.createCriteria(Series.FEATURE_OF_INTEREST).add(eq(FeatureOfInterest.IDENTIFIER, feature));
        criteria.createCriteria(DataEntity.OFFERINGS).add(eq(Offering.IDENTIFIER, offering));
        return criteria;
    }

    @Override
    public Criteria getObservationCriteriaForProcedure(String procedure) throws OwsExceptionReport {
        AbstractSeriesDAO seriesDAO = getDaoFactory().getSeriesDao(getSession());
        Criteria criteria = getDefaultDataEntityCriteria();
        Criteria seriesCriteria = getDefaultDataEntityCriteria(criteria);
        seriesDAO.addProcedureToCriteria(seriesCriteria, procedure);
        return criteria;
    }

    private Criteria getDefaultDataEntityCriteria(Criteria criteria) {
        Criteria seriesCriteria = criteria.createCriteria(DataEntity.SERIES);
        seriesCriteria.add(Restrictions.eq(Series.PUBLISHED, true));
        return seriesCriteria;
    }

    @Override
    public Criteria getObservationCriteriaForObservableProperty(String observableProperty)
            throws OwsExceptionReport {
        AbstractSeriesDAO seriesDAO = getDaoFactory().getSeriesDAO();
        Criteria criteria = getDefaultDataEntityCriteria();
        Criteria seriesCriteria = getDefaultDataEntityCriteria(criteria);
        seriesDAO.addObservablePropertyToCriteria(seriesCriteria, observableProperty);
        return criteria;
    }

    @Override
    public Criteria getObservationCriteriaForFeatureOfInterest(String featureOfInterest)
            throws OwsExceptionReport {
        AbstractSeriesDAO seriesDAO = getDaoFactory().getSeriesDAO();
        Criteria criteria = getDefaultDataEntityCriteria();
        Criteria seriesCriteria = getDefaultDataEntityCriteria(criteria);
        seriesDAO.addFeatureOfInterestToCriteria(seriesCriteria, featureOfInterest);
        return criteria;
    }

    @Override
    public Criteria getObservationCriteriaFor(String procedure, String observableProperty)
            throws OwsExceptionReport {
        AbstractSeriesDAO seriesDAO = getDaoFactory().getSeriesDAO();
        Criteria criteria = getDefaultDataEntityCriteria();
        Criteria seriesCriteria = getDefaultDataEntityCriteria(criteria);
        seriesDAO.addProcedureToCriteria(seriesCriteria, procedure);
        seriesDAO.addObservablePropertyToCriteria(seriesCriteria, observableProperty);
        return criteria;
    }

    @Override
    public Criteria getObservationCriteriaFor(String procedure, String observableProperty, String featureOfInterest) throws OwsExceptionReport {
        Criteria criteria = getDefaultDataEntityCriteria();
        addRestrictionsToCriteria(criteria, procedure, observableProperty, featureOfInterest);
        return criteria;
    }

    public ScrollableResults getObservations(Set<String> procedure, Set<String> observableProperty,
            Set<String> featureOfInterest, Set<String> offering, Criterion filterCriterion) {
        Criteria c = getDefaultDataEntityCriteria();
        String seriesAliasPrefix = createSeriesAliasAndRestrictions(c);
        if (CollectionHelper.isNotEmpty(procedure)) {
            c.createCriteria(seriesAliasPrefix + Series.PROCEDURE).add(Restrictions.in(Procedure.IDENTIFIER, procedure));
        }

        if (CollectionHelper.isNotEmpty(observableProperty)) {
            c.createCriteria(seriesAliasPrefix + Series.OBSERVABLE_PROPERTY).add(Restrictions.in(ObservableProperty.IDENTIFIER,
                    observableProperty));
        }

        if (CollectionHelper.isNotEmpty(featureOfInterest)) {
            c.createCriteria(seriesAliasPrefix + Series.FEATURE_OF_INTEREST).add(Restrictions.in(FeatureOfInterest.IDENTIFIER, featureOfInterest));
        }

        if (CollectionHelper.isNotEmpty(offering)) {
            c.createCriteria(DataEntity.OFFERINGS).add(Restrictions.in(Offering.IDENTIFIER, offering));
        }
        String logArgs = "request, features, offerings";
        if (filterCriterion != null) {
            logArgs += ", filterCriterion";
            c.add(filterCriterion);
        }
        LOGGER.debug("QUERY getObservations({}): {}", logArgs, HibernateHelper.getSqlString(c));

        return c.scroll(ScrollMode.FORWARD_ONLY);
    }

    @Override
    public Criteria getTemoralReferencedObservationCriteriaFor(OmObservation observation, ObservationConstellationEntity oc) throws OwsExceptionReport {
        Criteria criteria = getDefaultObservationTimeCriteria();
        Criteria seriesCriteria = addRestrictionsToCriteria(criteria, oc.getProcedure().getIdentifier(),
                oc.getObservableProperty().getIdentifier(),
                observation.getObservationConstellation().getFeatureOfInterestIdentifier(),
                oc.getOffering().getIdentifier());
        addAdditionalObservationIdentification(seriesCriteria, observation);
        return criteria;
    }

    /**
     * Add restirction to {@link Criteria
     *
     * @param criteria
     *            Main {@link Criteria}
     * @param procedure
     *            The procedure restriction
     * @param observableProperty
     *            The observableProperty restriction
     * @param featureOfInterest
     *            The featureOfInterest restriction
     * @return The created series {@link Criteria}
     * @throws OwsExceptionReport
     *             If an erro occurs
     */
    private Criteria addRestrictionsToCriteria(Criteria criteria, String procedure, String observableProperty,
            String featureOfInterest) throws OwsExceptionReport {
        AbstractSeriesDao seriesDAO = getDaoFactory().getSeriesDao(getSession());
        Criteria seriesCriteria = getDefaultDataEntityCriteria(criteria);
        seriesDAO.addFeatureOfInterestToCriteria(seriesCriteria, featureOfInterest);
        seriesDAO.addProcedureToCriteria(seriesCriteria, procedure);
        seriesDAO.addObservablePropertyToCriteria(seriesCriteria, observableProperty);
        return seriesCriteria;
    }

    private Criteria addRestrictionsToCriteria(Criteria criteria, String procedure, String observableProperty,
            String featureOfInterest, String offering) throws OwsExceptionReport {
        AbstractSeriesDao seriesDAO = getDaoFactory().getSeriesDao(getSession());
        Criteria seriesCriteria = criteria.createCriteria(DataEntity.PROPERTY_DATASETS);
        seriesDAO.addFeatureOfInterestToCriteria(seriesCriteria, featureOfInterest);
        seriesDAO.addProcedureToCriteria(seriesCriteria, procedure);
        seriesDAO.addObservablePropertyToCriteria(seriesCriteria, observableProperty);
        seriesDAO.addOfferingToCriteria(seriesCriteria, offering);
        return seriesCriteria;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<String> getObservationIdentifiers(String procedureIdentifier) {
        Criteria criteria =
                getDefaultObservationInfoCriteria()
                        .setProjection(Projections.distinct(Projections.property(ContextualReferencedDataEntity.IDENTIFIER)))
                        .add(Restrictions.isNotNull(ContextualReferencedDataEntity.IDENTIFIER))
                        .add(Restrictions.eq(ContextualReferencedDataEntity.DELETED, false));
        Criteria seriesCriteria = getDefaultDataEntityCriteria(criteria);
        seriesCriteria.createCriteria(Series.PROCEDURE)
                .add(Restrictions.eq(Procedure.IDENTIFIER, procedureIdentifier));
        LOGGER.debug("QUERY getObservationIdentifiers(procedureIdentifier): {}",
                HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }


    @SuppressWarnings("unchecked")
    @Override
    public List<Geometry> getSamplingGeometries(String feature) throws OwsExceptionReport {
        Criteria criteria = getDefaultObservationTimeCriteria(session).createAlias(DataEntity.SERIES, "s");
        criteria.createCriteria("s." + Series.FEATURE_OF_INTEREST).add(eq(FeatureOfInterest.IDENTIFIER, feature));
        criteria.addOrder(Order.asc(DataEntity.PHENOMENON_TIME_START));
        if (HibernateHelper.isColumnSupported(getObservationFactory().contextualReferencedClass(), DataEntity.SAMPLING_GEOMETRY)) {
            criteria.add(Restrictions.isNotNull(DataEntity.SAMPLING_GEOMETRY));
            criteria.setProjection(Projections.property(DataEntity.SAMPLING_GEOMETRY));
            LOGGER.debug("QUERY getSamplingGeometries(feature): {}", HibernateHelper.getSqlString(criteria));
            return criteria.list();
        } else if (HibernateHelper.isColumnSupported(getObservationFactory().contextualReferencedClass(), DataEntity.LONGITUDE)
                && HibernateHelper.isColumnSupported(getObservationFactory().contextualReferencedClass(), DataEntity.LATITUDE)) {
            criteria.add(Restrictions.and(Restrictions.isNotNull(DataEntity.LATITUDE),
                    Restrictions.isNotNull(DataEntity.LONGITUDE)));
            List<Geometry> samplingGeometries = Lists.newArrayList();
            LOGGER.debug("QUERY getSamplingGeometries(feature): {}", HibernateHelper.getSqlString(criteria));
            for (DataEntity element : (List<DataEntity>)criteria.list()) {
                samplingGeometries.add(new HibernateGeometryCreator().createGeometry(element));
            }
            return samplingGeometries;
        }
        return Collections.emptyList();
    }

    @Override
    public Long getSamplingGeometriesCount(String feature) throws OwsExceptionReport {
        Criteria criteria = getDefaultObservationTimeCriteria(session).createAlias(DataEntity.SERIES, "s");
        criteria.createCriteria("s." + Series.FEATURE_OF_INTEREST).add(eq(FeatureOfInterest.IDENTIFIER, feature));
        criteria.setProjection(Projections.count(DataEntity.OBS_ID));
        if (GeometryHandler.getInstance().isSpatialDatasource()) {
            criteria.add(Restrictions.isNotNull(DataEntity.SAMPLING_GEOMETRY));
            LOGGER.debug("QUERY getSamplingGeometriesCount(feature): {}", HibernateHelper.getSqlString(criteria));
            return (Long)criteria.uniqueResult();
        } else {
            criteria.add(Restrictions.and(Restrictions.isNotNull(DataEntity.LATITUDE),
                    Restrictions.isNotNull(DataEntity.LONGITUDE)));
            LOGGER.debug("QUERY getSamplingGeometriesCount(feature): {}", HibernateHelper.getSqlString(criteria));
            return (Long)criteria.uniqueResult();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Envelope getBboxFromSamplingGeometries(String feature) throws OwsExceptionReport {
        Criteria criteria = getDefaultObservationTimeCriteria().createAlias(DataEntity.SERIES, "s");
        criteria.createCriteria("s." + Series.FEATURE_OF_INTEREST).add(eq(FeatureOfInterest.IDENTIFIER, feature));
        if (GeometryHandler.getInstance().isSpatialDatasource()) {
            criteria.add(Restrictions.isNotNull(DataEntity.SAMPLING_GEOMETRY));
            Dialect dialect = ((SessionFactoryImplementor) session.getSessionFactory()).getDialect();
            if (HibernateHelper.supportsFunction(dialect, HibernateConstants.FUNC_EXTENT)) {
                criteria.setProjection(SpatialProjections.extent(DataEntity.SAMPLING_GEOMETRY));
                LOGGER.debug("QUERY getBboxFromSamplingGeometries(feature): {}",
                        HibernateHelper.getSqlString(criteria));
                return (Envelope) criteria.uniqueResult();
            }
        } else if (HibernateHelper.isColumnSupported(getObservationFactory().observationClass(), DataEntity.SAMPLING_GEOMETRY)) {
            criteria.add(Restrictions.isNotNull(DataEntity.SAMPLING_GEOMETRY));
            criteria.setProjection(Projections.property(DataEntity.SAMPLING_GEOMETRY));
            LOGGER.debug("QUERY getBboxFromSamplingGeometries(feature): {}",
                    HibernateHelper.getSqlString(criteria));
            Envelope envelope = new Envelope();
            for (Geometry geom : (List<Geometry>) criteria.list()) {
                envelope.expandToInclude(geom.getEnvelopeInternal());
            }
            return envelope;
        } else if (HibernateHelper.isColumnSupported(getObservationFactory().observationClass(), DataEntity.LATITUDE)
                && HibernateHelper.isColumnSupported(getObservationFactory().observationClass(), DataEntity.LONGITUDE)) {
            criteria.add(Restrictions.and(Restrictions.isNotNull(DataEntity.LATITUDE),
                    Restrictions.isNotNull(DataEntity.LONGITUDE)));
            criteria.setProjection(Projections.projectionList().add(Projections.min(DataEntity.LATITUDE))
                    .add(Projections.min(DataEntity.LONGITUDE))
                    .add(Projections.max(DataEntity.LATITUDE))
                    .add(Projections.max(DataEntity.LONGITUDE)));

            LOGGER.debug("QUERY getBboxFromSamplingGeometries(feature): {}", HibernateHelper.getSqlString(criteria));
            MinMaxLatLon minMaxLatLon = new MinMaxLatLon((Object[]) criteria.uniqueResult());
            Envelope envelope = new Envelope(minMaxLatLon.getMinLon(), minMaxLatLon.getMaxLon(),
                    minMaxLatLon.getMinLat(), minMaxLatLon.getMaxLat());
            return envelope;
        }
        return null;
    }

    /**
     * Create series observation query criteria for series and offerings
     *
     * @param clazz
     *            Class to query
     * @param series
     *           DatasetEntity to get values for
     * @param offerings
     *            Offerings to get values for
     * @param session
     *            Hibernate session
     * @return Criteria to query series observations
     */
    protected Criteria createCriteriaFor(Class<?> clazz, DatasetEntity series, List<String> offerings) {
        final Criteria criteria = createCriteriaFor(clazz, series);
        if (CollectionHelper.isNotEmpty(offerings)) {
            criteria.createCriteria(DataEntity.OFFERINGS).add(Restrictions.in(Offering.IDENTIFIER, offerings));
        }
        return criteria;
    }

    /**
     * Create series observation query criteria for series
     *
     * @param clazz
     *            to query
     * @param series
     *           DatasetEntity to get values for
     * @param session
     *            Hibernate session
     * @return Criteria to query series observations
     */
    protected Criteria createCriteriaFor(Class<?> clazz, DatasetEntity series) {
        final Criteria criteria = getDefaultDataEntityCriteria();
        criteria.createCriteria(DataEntity.PROPERTY_DATASETS)
                .add(Restrictions.eq(DatasetEntity.PROPERTY_ID, series.getSeriesId()))
                .add(Restrictions.eq(DatasetEntity.PROPERTY_PUBLISHED, true));
        return criteria;
    }

    /**
     * Get the result times for this series, offerings and filters
     *
     * @param series
     *            Time series to get result times for
     * @param offerings
     *            Offerings to restrict matching result times
     * @param filter
     *            Temporal filter to restrict matching result times
     * @param session
     *            Hibernate session
     * @return Matching result times
     */
    @SuppressWarnings("unchecked")
    public List<Date> getResultTimesForDataEntity(DatasetEntity series, List<String> offerings, Criterion filter,
            Session session) {
        Criteria criteria = createCriteriaFor(getObservationFactory().observationClass(), series);
        if (CollectionHelper.isNotEmpty(offerings)) {
            criteria.createCriteria(DataEntity.OFFERINGS)
                    .add(Restrictions.in(Offering.IDENTIFIER, offerings));
        }
        if (filter != null) {
            criteria.add(filter);
        }
        criteria.setProjection(Projections.distinct(Projections.property(DataEntity.PROPERTY_RESULT_TIME)));
        criteria.addOrder(Order.asc(DataEntity.PROPERTY_RESULT_TIME));
        LOGGER.debug("QUERY getResultTimesForDataEntity({}): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Create criteria to query min/max time for series from series observation
     *
     * @param series
     *           DatasetEntity to get values for
     * @param offerings
     * @param session
     *            Hibernate session
     * @return Criteria to get min/max time values for series
     */
    public Criteria getMinMaxTimeCriteriaForDataEntity(DatasetEntity series, Collection<String> offerings,
            Session session) {
        Criteria criteria = createCriteriaFor(getObservationFactory().observationClass(), series);
        if (CollectionHelper.isNotEmpty(offerings)) {
            criteria.createCriteria(DataEntity.OFFERINGS).add(
                    Restrictions.in(Offering.IDENTIFIER, offerings));
        }
        criteria.setProjection(Projections.projectionList()
                .add(Projections.min(DataEntity.PROPERTY_PHENOMENON_TIME_START))
                .add(Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_END)));
        return criteria;
    }

    /**
     * Create criteria to query min/max time of each offering for series from series observation
     *
     * @param series
     *           DatasetEntity to get values for
     * @param offerings
     * @param session
     *            Hibernate session
     * @return Criteria to get min/max time values for series
     */
    public Criteria getOfferingMinMaxTimeCriteriaForDataEntity(DatasetEntity series, Collection<String> offerings,
            Session session) {
        Criteria criteria = createCriteriaFor(getObservationFactory().observationClass(), series);
        if (CollectionHelper.isNotEmpty(offerings)) {
            criteria.createCriteria(DataEntity.OFFERINGS, "off").add(
                    Restrictions.in(Offering.IDENTIFIER, offerings));
        } else {
            criteria.createAlias(DataEntity.OFFERINGS, "off");
        }
        criteria.setProjection(Projections.projectionList()
                        .add(Projections.groupProperty("off." + Offering.IDENTIFIER))
                        .add(Projections.min(DataEntity.PROPERTY_PHENOMENON_TIME_START))
                        .add(Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_END)));
        return criteria;
    }

    public ScrollableResults getSeriesNotMatchingSeries(Set<Long> seriesIDs, GetObservationRequest request,
            Set<String> features, Criterion temporalFilterCriterion) throws OwsExceptionReport {
        Criteria c = getDataEntityCriteriaFor(request, features, temporalFilterCriterion, null);
        c.createCriteria(DataEntity.PROPERTY_DATASETS)
            .add(Restrictions.not(Restrictions.in(DatasetEntity.PROPERTY_ID, seriesIDs)));
        c.setProjection(Projections.property(DataEntity.PROPERTY_DATASETS));
        return c.setReadOnly(true).scroll(ScrollMode.FORWARD_ONLY);
    }

    public ScrollableResults getSeriesNotMatchingSeries(Set<Long> seriesIDs, GetObservationRequest request,
            Set<String> features) throws OwsExceptionReport {
        return getSeriesNotMatchingSeries(seriesIDs, request, features, null);
    }

    /**
     * Create series observations {@link Criteria} for GetObservation request,
     * features, and filter criterion (typically a temporal filter) or an
     * indeterminate time (first/latest). This method is private and accepts all
     * possible arguments for request-based getDataEntityFor. Other
     * public methods overload this method with sensible combinations of
     * arguments.
     *
     * @param request
     *            GetObservation request
     * @param features
     *              Collection of feature identifiers resolved from the request
     * @param filterCriterion
     *            Criterion to apply to criteria query (typically a temporal
     *            filter)
     * @param sosIndeterminateTime
     *            Indeterminate time to use in a temporal filter (first/latest)
     * @param session
     * @returnDatasetEntity observations {@link Criteria}
     * @throws OwsExceptionReport
     */
    protected Criteria getDataEntityCriteriaFor(GetObservationRequest request, Collection<String> features,
                Criterion filterCriterion, IndeterminateValue sosIndeterminateTime) throws OwsExceptionReport {
            final Criteria observationCriteria = getDefaultDataEntityCriteria();

        Criteria seriesCriteria = observationCriteria.createCriteria(DataEntity.PROPERTY_DATASETS);

        checkAndAddSpatialFilteringProfileCriterion(observationCriteria, request);
        addSpecificRestrictions(seriesCriteria, request);
        if (CollectionHelper.isNotEmpty(request.getProcedures())) {
            seriesCriteria.createCriteria(Series.PROCEDURE)
                    .add(Restrictions.in(Procedure.IDENTIFIER, request.getProcedures()));
        }

        if (CollectionHelper.isNotEmpty(request.getObservedProperties())) {
            seriesCriteria.createCriteria(Series.OBSERVABLE_PROPERTY)
                    .add(Restrictions.in(ObservableProperty.IDENTIFIER, request.getObservedProperties()));
        }

        if (CollectionHelper.isNotEmpty(features)) {
            seriesCriteria.createCriteria(Series.FEATURE_OF_INTEREST)
                    .add(Restrictions.in(FeatureOfInterest.IDENTIFIER, features));
        }

        if (CollectionHelper.isNotEmpty(request.getOfferings())) {
            observationCriteria.createCriteria(DataEntity.OFFERINGS)
                    .add(Restrictions.in(Offering.IDENTIFIER, request.getOfferings()));
        }

        String logArgs = "request, features, offerings";
        if (filterCriterion != null) {
            logArgs += ", filterCriterion";
            observationCriteria.add(filterCriterion);
        }
        if (sosIndeterminateTime != null) {
            logArgs += ", sosIndeterminateTime";
            addIndeterminateTimeRestriction(observationCriteria, sosIndeterminateTime);
        }
        if (request.isSetFesFilterExtension()) {
            new ExtensionFesFilterCriteriaAdder(observationCriteria, request.getFesFilterExtensions()).add();
        }
        LOGGER.debug("QUERY getDataEntityFor({}): {}", logArgs,
                HibernateHelper.getSqlString(observationCriteria));
        return observationCriteria;
    }


    private String createSeriesAliasAndRestrictions(Criteria c) {
        String alias = "s";
        String aliasWithDot = alias + ".";
        c.createAlias(DataEntity.PROPERTY_DATASETS, alias);
        c.add(Restrictions.eq(aliasWithDot + DatasetEntity.PROPERTY_DELETED, false));
        c.add(Restrictions.eq(aliasWithDot + DatasetEntity.PROPERTY_PUBLISHED, true));
        return aliasWithDot;
    }

    /**
     * Query series observations {@link ScrollableResults} for GetObservation
     * request and features
     *
     * @param request
     *              GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param session
     *            Hibernate session
     * @return {@link ScrollableResults} ofDatasetEntity observations that fit
     * @throws OwsExceptionReport
     */
    public ScrollableResults getStreamingDataEntitysFor(GetObservationRequest request,
            Collection<String> features) throws OwsExceptionReport {
        return getStreamingDataEntitysFor(request, features, null, null);
    }

    /**
     * Query series observations {@link ScrollableResults} for GetObservation
     * request, features, and a filter criterion (typically a temporal filter)
     *
     * @param request
     *              GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param filterCriterion
     *            Criterion to apply to criteria query (typically a temporal
     *            filter)
     * @param session
     *            Hibernate session
     * @return {@link ScrollableResults} ofDatasetEntity observations that fit
     * @throws OwsExceptionReport
     */
    public ScrollableResults getStreamingDataEntitysFor(GetObservationRequest request,
            Collection<String> features, Criterion filterCriterion) throws OwsExceptionReport {
        return getStreamingDataEntitysFor(request, features, filterCriterion, null);
    }

    /**
     * Query series observations for GetObservation request, features, and
     * filter criterion (typically a temporal filter) or an indeterminate time
     * (first/latest). This method is private and accepts all possible arguments
     * for request-based getDataEntityFor. Other public methods overload
     * this method with sensible combinations of arguments.
     *
     * @param request
     *            GetObservation request
     * @param features
     *              Collection of feature identifiers resolved from the request
     * @param filterCriterion
     *            Criterion to apply to criteria query (typically a temporal
     *            filter)
     * @param sosIndeterminateTime
     *            Indeterminate time to use in a temporal filter (first/latest)
     * @param session
     * @return {@link ScrollableResults} ofDatasetEntity observations that fits
     * @throws OwsExceptionReport
     */
    protected ScrollableResults getStreamingDataEntitysFor(GetObservationRequest request, Collection<String> features,
            Criterion filterCriterion, IndeterminateValue sosIndeterminateTime) throws OwsExceptionReport {
        return getDataEntityCriteriaFor(request, features, filterCriterion, sosIndeterminateTime).setReadOnly(true).scroll(ScrollMode.FORWARD_ONLY);
    }

    /**
     * Update series observation by setting deleted flag
     *
     * @param series
     *           DatasetEntity for which the observations should be updated
     * @param deleteFlag
     *            New deleted flag value
     * @param session
     *            Hibernate Session
     */
    public void updateObservationSetAsDeletedForSeries(List<DatasetEntity> series, boolean deleteFlag) {
        if (CollectionHelper.isNotEmpty(series)) {
            Criteria criteria = getDefaultDataEntityCriteria();
            criteria.add(Restrictions.in(DataEntity.PROPERTY_DATASETS, series));
            ScrollableIterable<DataEntity<?>> scroll = ScrollableIterable.fromCriteria(criteria);
            updateObservation(scroll, deleteFlag);
        }
    }

    /**
     * Query the min time from series observations for series
     *
     * @param series
     *           DatasetEntity to get values for
     * @param session
     *            Hibernate session
     * @return Min time from series observations
     */
    public DateTime getMinDataEntityTime(DatasetEntity series) {
        Criteria criteria = createCriteriaFor(getObservationFactory().observationClass(), series);
        criteria.setProjection(Projections.min(DataEntity.PROPERTY_PHENOMENON_TIME_START));
        Object min = criteria.uniqueResult();
        if (min != null) {
            return new DateTime(min, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Query the max time from series observations for series
     *
     * @param series
     *           DatasetEntity to get values for
     * @param session
     *            Hibernate session
     * @return Max time from series observations
     */
    public DateTime getMaxDataEntityTime(DatasetEntity series) {
        Criteria criteria = createCriteriaFor(getObservationFactory().observationClass(), series);
        criteria.setProjection(Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_END));
        Object max = criteria.uniqueResult();
        if (max != null) {
            return new DateTime(max, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Query series observation for series and offerings
     *
     * @param series
     *           DatasetEntity to get values for
     * @returnDatasetEntity observations that fit
     */
    public abstract List<DataEntity<?>> get(DatasetEntity series);

    /**
     * Query series obserations for series, temporal filter, and offerings
     *
     * @param series
     *           DatasetEntity to get values for
     * @param offerings
     *            Offerings to get values for
     * @param filterCriterion
     * @returnDatasetEntity observations that fit
     */

    /**
     * Query first/latest series obserations for series (and offerings)
     *
     * @param series
     *           DatasetEntity to get values for
     * @param sosIndeterminateTime
     * @returnDatasetEntity observations that fit
     */
    public abstract List<DataEntity<?>> get(DatasetEntity series, IndeterminateValue sosIndeterminateTime);

    /**
     * Query series observations for GetObservation request and features
     *
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @returnDatasetEntity observations that fit
     * @throws OwsExceptionReport
     */
    public abstract List<DataEntity<?>> get(GetObservationRequest request, Collection<String> features) throws OwsExceptionReport;

    /**
     * Query series observations for GetObservation request, features, and a
     * filter criterion (typically a temporal filter)
     *
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param filterCriterion
     *            Criterion to apply to criteria query (typically a temporal
     *            filter)
     * @returnDatasetEntity observations that fit
     * @throws OwsExceptionReport
     */
    public abstract List<DataEntity<?>> get(GetObservationRequest request, Collection<String> features, Criterion filterCriterion) throws OwsExceptionReport;

    /**
     * Query series observations for GetObservation request, features, and an
     * indeterminate time (first/latest)
     *
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param sosIndeterminateTime
     *            Indeterminate time to use in a temporal filter (first/latest)
     * @returnDatasetEntity observations that fit
     * @throws OwsExceptionReport
     */
    public abstract List<DataEntity<?>> get(GetObservationRequest request, Collection<String> features, IndeterminateValue sosIndeterminateTime) throws OwsExceptionReport;

    /**
     * Query series observations for GetObservation request, features, and
     * filter criterion (typically a temporal filter) or an indeterminate time
     * (first/latest). This method is private and accepts all possible arguments
     * for request-based getDataEntityFor. Other public methods overload
     * this method with sensible combinations of arguments.
     *
     * @param request
     *            GetObservation request
     * @param features
     *            Collection of feature identifiers resolved from the request
     * @param filterCriterion
     *            Criterion to apply to criteria query (typically a temporal
     *            filter)
     * @param sosIndeterminateTime
     *            Indeterminate time to use in a temporal filter (first/latest)
     * @param session
     * @returnDatasetEntity observations that fit
     * @throws OwsExceptionReport
     */
    protected abstract List<? extends DataEntity<?>> get(GetObservationRequest request, Collection<String> features, Criterion filterCriterion, IndeterminateValue sosIndeterminateTime) throws OwsExceptionReport;


    public abstract List<DataEntity<?>> get(DatasetEntity series, GetObservationRequest request, IndeterminateValue sosIndeterminateTime) throws OwsExceptionReport;

    protected abstract void addSpecificRestrictions(Criteria c, GetObservationRequest request) throws OwsExceptionReport;

    protected Criteria getDataEntityCriteriaFor(DatasetEntity series, GetObservationRequest request,
            IndeterminateValue sosIndeterminateTime) throws OwsExceptionReport {
        final Criteria c =
                getDefaultDataEntityCriteria().add(
                        Restrictions.eq(DataEntity.PROPERTY_DATASETS, series));
        checkAndAddSpatialFilteringProfileCriterion(c, request);

        if (request.isSetOffering()) {
            c.createCriteria(DataEntity.OFFERINGS).add(
                    Restrictions.in(Offering.IDENTIFIER, request.getOfferings()));
        }
        String logArgs = "request, features, offerings";
        logArgs += ", sosIndeterminateTime";
        if (series.isSetFirstTimeStamp() && sosIndeterminateTime.equals(ExtendedIndeterminateTime.FIRST)) {
            addIndeterminateTimeRestriction(c, sosIndeterminateTime, series.getFirstTimeStamp());
        } else if (series.isSetLastTimeStamp() && sosIndeterminateTime.equals(ExtendedIndeterminateTime.LATEST)) {
            addIndeterminateTimeRestriction(c, sosIndeterminateTime, series.getLastTimeStamp());
        } else {
            addIndeterminateTimeRestriction(c, sosIndeterminateTime);
        }
        LOGGER.debug("QUERY getDataEntityFor({}): {}", logArgs, HibernateHelper.getSqlString(c));
        return c;

    }

    protected Criteria getDataEntityCriteriaForIndeterminateTimeFilter(DatasetEntity series,
            List<String> offerings, IndeterminateValue sosIndeterminateTime) {
        final Criteria criteria = createCriteriaFor(getObservationFactory().observationClass(), series, offerings);
        criteria.addOrder(getOrder(sosIndeterminateTime)).setMaxResults(1);
        LOGGER.debug("QUERY getDataEntityForExtendedIndeterminateTimeFilter(series, offerings,(first,latest)): {}",
                HibernateHelper.getSqlString(criteria));
        return criteria;
    }

    protected Criteria getDataEntityCriteriaFor(DatasetEntity series, List<String> offerings,
            Criterion filterCriterion) {
        final Criteria criteria = createCriteriaFor(getObservationFactory().observationClass(), series, offerings);
        criteria.add(filterCriterion);
        LOGGER.debug("QUERY getDataEntityFor(series, offerings, temporalFilter): {}",
                HibernateHelper.getSqlString(criteria));
        return criteria;
    }

    protected Criteria getDataEntityCriteriaFor(DatasetEntity series, List<String> offerings,
            Session session) {
        final Criteria criteria = createCriteriaFor(DataEntity.class, series, offerings);
        LOGGER.debug("QUERY getDataEntityFor(series, offerings): {}", HibernateHelper.getSqlString(criteria));
        return criteria;
    }

    @Override
    public String addProcedureAlias(Criteria criteria) {
        criteria.createAlias(DataEntity.PROPERTY_DATASETS, "ds");
        criteria.createAlias("ds." + DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION, "oc");
        criteria.createAlias("oc." + ObservationConstellationEntity.PROCEDURE, "p");
        return "p.";
    }


    /**
     * Get the first not deleted observation for the {@link Series}
     *
     * @param series
     *           DatasetEntity to get observation for
     * @param session
     *            Hibernate session
     * @return First not deleted observation
     */
    public DataEntity<?> getFirstObservationFor(DatasetEntity series) {
        Criteria c = getDefaultDataEntityCriteria(criteria)
        c.add(Restrictions.eq(DataEntity.PROPERTY_DATASETS, series));
        c.addOrder(Order.asc(DataEntity.PROPERTY_PHENOMENON_TIME_START));
        c.setMaxResults(1);
         LOGGER.debug("QUERY getFirstObservationFor(series): {}",
                    HibernateHelper.getSqlString(c));
        return (DataEntity)c.uniqueResult();
    }

    /**
     * Get the last not deleted observation for the {@link Series}
     *
     * @param series
     *           DatasetEntity to get observation for
     * @param session
     *            Hibernate session
     * @return Last not deleted observation
     */
    public DataEntity<?> getLastObservationFor(DatasetEntity series) {
        Criteria c = getDefaultDataEntityCriteria();
        c.add(Restrictions.eq(DataEntity.PROPERTY_DATASETS, series));
        c.addOrder(Order.desc(DataEntity.PROPERTY_PHENOMENON_TIME_END));
        c.setMaxResults(1);
         LOGGER.debug("QUERY getLastObservationFor(series): {}",
                    HibernateHelper.getSqlString(c));
        return (DataEntity)c.uniqueResult();
    }
    @SuppressWarnings("unchecked")
    public List<String> getOfferingsForSeries(DatasetEntity series) {
        Criteria criteria = createCriteriaFor(getObservationFactory().observationClass(), series);
        criteria.createAlias(DataEntity.OFFERINGS, "off");
        criteria.setProjection(Projections.distinct(Projections.property("off." + Offering.IDENTIFIER)));
        LOGGER.debug("QUERY getOfferingsForSeries(series): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }
    
    /**
     * Query the minimum {@link ObservationValueTime} for parameter
     * @param request
     *            {@link GetObservationRequest}
     * @param procedure
     *            Datasource procedure id
     * @param observableProperty
     *            Datasource procedure id
     * @param featureOfInterest
     *            Datasource procedure id
     * @param temporalFilterCriterion
     *            Temporal filter {@link Criterion}
     * @param session
     *            Hibernate Session
     * @return Resulting minimum {@link ObservationValueTime}
     * @throws OwsExceptionReport If an error occurs when executing the query
     */
    public ObservationValueTime getMinValueFor(GetObservationRequest request, long procedure, long observableProperty,
            long featureOfInterest, Criterion temporalFilterCriterion) throws OwsExceptionReport {
        return (ObservationValueTime) getValueCriteriaFor(request, procedure, observableProperty, featureOfInterest,
                temporalFilterCriterion, SosIndeterminateTime.first).uniqueResult();
    }

    /**
     * Query the maximum {@link ObservationValueTime} for parameter
     * @param request
     *            {@link GetObservationRequest}
     * @param procedure
     *            Datasource procedure id
     * @param observableProperty
     *            Datasource procedure id
     * @param featureOfInterest
     *            Datasource procedure id
     * @param temporalFilterCriterion
     *            Temporal filter {@link Criterion}
     * @param session
     *            Hibernate Session
     * @return Resulting maximum {@link ObservationValueTime}
     * @throws OwsExceptionReport If an error occurs when executing the query
     */
    public ObservationValueTime getMaxValueFor(GetObservationRequest request, long procedure, long observableProperty,
            long featureOfInterest, Criterion temporalFilterCriterion) throws OwsExceptionReport {
        return (ObservationValueTime) getValueCriteriaFor(request, procedure, observableProperty, featureOfInterest,
                temporalFilterCriterion, SosIndeterminateTime.latest).uniqueResult();
    }

    /**
     * Query the minimum {@link ObservationValueTime} for parameter
     * @param request
     *            {@link GetObservationRequest}
     * @param procedure
     *            Datasource procedure id
     * @param observableProperty
     *            Datasource procedure id
     * @param featureOfInterest
     *            Datasource procedure id
     * @param session
     *            Hibernate Session
     * @return Resulting minimum {@link ObservationValueTime}
     * @throws OwsExceptionReport If an error occurs when executing the query
     */
    public ObservationValueTime getMinValueFor(GetObservationRequest request, long procedure, long observableProperty,
            long featureOfInterest) throws OwsExceptionReport {
        return (ObservationValueTime) getValueCriteriaFor(request, procedure, observableProperty, featureOfInterest, null,
                SosIndeterminateTime.first).uniqueResult();
    }

    /**
     * Query the maximum {@link ObservationValueTime} for parameter
     * @param request
     *            {@link GetObservationRequest}
     * @param procedure
     *            Datasource procedure id
     * @param observableProperty
     *            Datasource procedure id
     * @param featureOfInterest
     *            Datasource procedure id
     * @param session
     *            Hibernate Session
     * @return Resulting maximum {@link ObservationValueTime}
     * @throws OwsExceptionReport If an error occurs when executing the query
     */
    public ObservationValueTime getMaxValueFor(GetObservationRequest request, long procedure, long observableProperty,
            long featureOfInterest) throws OwsExceptionReport {
        return (ObservationValueTime) getValueCriteriaFor(request, procedure, observableProperty, featureOfInterest, null,
                SosIndeterminateTime.latest).uniqueResult();
    }

    /**
     * Create {@link Criteria} for parameter
     * @param request
     *            {@link GetObservationRequest}
     * @param procedure
     *            Datasource procedure id
     * @param observableProperty
     *            Datasource procedure id
     * @param featureOfInterest
     *            Datasource procedure id
     * @param temporalFilterCriterion
     *            Temporal filter {@link Criterion}
     * @param sosIndeterminateTime first/latest indicator
     * @param session
     *            Hibernate Session
     * @return Resulting {@link Criteria}
     * @throws OwsExceptionReport  If an error occurs when adding Spatial Filtering Profile
     *             restrictions
     */
    private Criteria getValueCriteriaFor(GetObservationRequest request, long procedure, long observableProperty,
            long featureOfInterest, Criterion temporalFilterCriterion, ExtendedIndeterminateTime sosIndeterminateTime) throws OwsExceptionReport {
        final Criteria c =
                getDefaultDataEntityCriteria(getObservationFactory().observationClass()).createAlias(ObservationValueTime.PROCEDURE, "p")
                        .createAlias(ObservationValueTime.FEATURE_OF_INTEREST, "f")
                        .createAlias(ObservationValueTime.OBSERVABLE_PROPERTY, "o");

        checkAndAddSpatialFilteringProfileCriterion(c, request);

        c.add(Restrictions.eq("p." + Procedure.ID, observableProperty));
        c.add(Restrictions.eq("o." + ObservableProperty.ID, observableProperty));
        c.add(Restrictions.eq("f." + FeatureOfInterest.ID, featureOfInterest));

        if (CollectionHelper.isNotEmpty(request.getOfferings())) {
            c.createCriteria(ObservationValueTime.OFFERINGS).add(Restrictions.in(Offering.IDENTIFIER, request.getOfferings()));
        }

        String logArgs = "request, series, offerings";
        if (temporalFilterCriterion != null) {
            logArgs += ", filterCriterion";
            c.add(temporalFilterCriterion);
        }
        if (sosIndeterminateTime != null) {
            logArgs += ", sosIndeterminateTime";
            addIndeterminateTimeRestriction(c, sosIndeterminateTime);
        }
        LOGGER.debug("QUERY getObservationFor({}): {}", logArgs, HibernateHelper.getSqlString(c));
        return c;
    }
    

    /**
     * Get the concrete {@link TemporalReferencedSeriesObservation} class.
     *
     * @return The concrete {@link TemporalReferencedSeriesObservation} class
     */
    protected abstract Class<?> getSeriesValueTimeClass();

    /**
     * Get {@link ObservationTimeExtrema} for a {@link Series} with temporal
     * filter.
     *
     * @param request
     *            {@link AbstractObservationRequest} request
     * @param series
     *            {@link Series} to get time extrema for
     * @param temporalFilterCriterion
     *            Temporal filter
     * @param session
     *            Hibernate session
     * @return Time extrema for {@link Series}
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    public ObservationTimeExtrema getTimeExtremaForSeries(AbstractObservationRequest request, long series,
            Criterion temporalFilterCriterion, Session session) throws OwsExceptionReport {
        Criteria c = getSeriesValueCriteriaFor(request, series, temporalFilterCriterion, null, session);
        addMinMaxTimeProjection(c);
        LOGGER.debug("QUERY getTimeExtremaForSeries(request, series, temporalFilter): {}",
                HibernateHelper.getSqlString(c));
        return parseMinMaxTime((Object[]) c.uniqueResult());

    }

    /**
     * Get {@link ObservationTimeExtrema} for a {@link Series} with temporal
     * filter.
     *
     * @param request
     *            {@link AbstractObservationRequest} request
     * @param series
     *            {@link Set} of {@link Series} to get time extrema for
     * @param temporalFilterCriterion
     *            Temporal filter
     * @param session
     *            Hibernate session
     * @return Time extrema for {@link Series}
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    public ObservationTimeExtrema getTimeExtremaForSeries(AbstractObservationRequest request, Set<Long> series,
            Criterion temporalFilterCriterion, Session session) throws OwsExceptionReport {
        Criteria c = getSeriesValueCriteriaFor(request, series, temporalFilterCriterion, null, session);
        addMinMaxTimeProjection(c);
        LOGGER.debug("QUERY getTimeExtremaForSeries(request, series, temporalFilter): {}",
                HibernateHelper.getSqlString(c));
        return parseMinMaxTime((Object[]) c.uniqueResult());

    }

    /**
     * Get {@link ObservationTimeExtrema} for a {@link Series}.
     *
     * @param request
     *            {@link AbstractObservationRequest} request
     * @param series
     *            {@link Series} to get time extrema for
     * @param session
     *            Hibernate session
     * @return Time extrema for {@link Series}
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    public ObservationTimeExtrema getTimeExtremaForSeries(AbstractObservationRequest request, long series, Session session)
            throws OwsExceptionReport {
        return getTimeExtremaForSeries(request, series, null, session);
    }

    /**
     * Query the minimum {@link TemporalReferencedSeriesObservation} for parameter
     *
     * @param request
     *            {@link AbstractObservationRequest}
     * @param series
     *            Datasource series id
     * @param temporalFilterCriterion
     *            Temporal filter {@link Criterion}
     * @param session
     *            Hibernate Session
     * @return Resulting minimum {@link TemporalReferencedSeriesObservation}
     * @throws OwsExceptionReport
     *             If an error occurs when executing the query
     */
    public TemporalReferencedSeriesObservation getMinSeriesValueFor(AbstractObservationRequest request, long series,
            Criterion temporalFilterCriterion, Session session) throws OwsExceptionReport {
        return (TemporalReferencedSeriesObservation) getSeriesValueCriteriaFor(request, series, temporalFilterCriterion,
                ExtendedIndeterminateTime.FIRST, session).uniqueResult();
    }

    /**
     * Query the maximum {@link TemporalReferencedSeriesObservation} for parameter
     *
     * @param request
     *            {@link AbstractObservationRequest}
     * @param series
     *            Datasource series id
     * @param temporalFilterCriterion
     *            Temporal filter {@link Criterion}
     * @param session
     *            Hibernate Session
     * @return Resulting maximum {@link TemporalReferencedSeriesObservation}
     * @throws OwsExceptionReport
     *             If an error occurs when executing the query
     */
    public TemporalReferencedSeriesObservation getMaxSeriesValueFor(AbstractObservationRequest request, long series,
            Criterion temporalFilterCriterion, Session session) throws OwsExceptionReport {
        return (TemporalReferencedSeriesObservation) getSeriesValueCriteriaFor(request, series, temporalFilterCriterion,
                ExtendedIndeterminateTime.LATEST, session).uniqueResult();
    }

    /**
     * Query the minimum {@link TemporalReferencedSeriesObservation} for parameter
     *
     * @param request
     *            {@link AbstractObservationRequest}
     * @param series
     *            Datasource series id
     * @param session
     *            Hibernate Session
     * @return Resulting minimum {@link TemporalReferencedSeriesObservation}
     * @throws OwsExceptionReport
     *             If an error occurs when executing the query
     */
    public TemporalReferencedSeriesObservation getMinSeriesValueFor(AbstractObservationRequest request, long series, Session session)
            throws OwsExceptionReport {
        return (TemporalReferencedSeriesObservation) getSeriesValueCriteriaFor(request, series, null, ExtendedIndeterminateTime.FIRST, session)
                .uniqueResult();
    }

    /**
     * Query the maximum {@link TemporalReferencedSeriesObservation} for parameter
     *
     * @param request
     *            {@link AbstractObservationRequest}
     * @param series
     *            Datasource series id
     * @param session
     *            Hibernate Session
     * @return Resulting maximum {@link TemporalReferencedSeriesObservation}
     * @throws OwsExceptionReport
     *             If an error occurs when executing the query
     */
    public TemporalReferencedSeriesObservation getMaxSeriesValueFor(AbstractObservationRequest request, long series, Session session)
            throws OwsExceptionReport {
        return (TemporalReferencedSeriesObservation) getSeriesValueCriteriaFor(request, series, null, ExtendedIndeterminateTime.LATEST, session)
                .uniqueResult();
    }

    @Override
    public ObservationTimeExtrema getTimeExtremaForSeries(Collection<Series> series, Criterion temporalFilterCriterion,
            Session session) throws OwsExceptionReport {
        Criteria c = getSeriesValueCriteriaFor(series, temporalFilterCriterion, null, session);
        addPhenomenonTimeProjection(c);
        LOGGER.debug("QUERY getTimeExtremaForSeries(series, temporalFilter): {}",
                HibernateHelper.getSqlString(c));
        return parseMinMaxPhenomenonTime((Object[]) c.uniqueResult());
    }

    @Override
    public ObservationTimeExtrema getTimeExtremaForSeriesIds(Collection<Long> series, Criterion temporalFilterCriterion,
            Session session) throws OwsExceptionReport {
        Criteria c = getSeriesValueCriteriaForSeriesIds(series, temporalFilterCriterion, null, session);
        addPhenomenonTimeProjection(c);
        LOGGER.debug("QUERY getTimeExtremaForSeriesIds(series, temporalFilter): {}",
                HibernateHelper.getSqlString(c));
        return parseMinMaxPhenomenonTime((Object[]) c.uniqueResult());
    }

    private ObservationTimeExtrema parseMinMaxPhenomenonTime(Object[] result) {
        ObservationTimeExtrema ote = new ObservationTimeExtrema();
        if (result != null) {
            ote.setMinPhenomenonTime(DateTimeHelper.makeDateTime(result[0]));
            ote.setMaxPhenomenonTime(DateTimeHelper.makeDateTime(result[1]));
        }
        return ote;
    }

    private void addPhenomenonTimeProjection(Criteria c) {
        ProjectionList projectionList = Projections.projectionList();
        projectionList.add(Projections.min(TemporalReferencedSeriesObservation.PHENOMENON_TIME_START));
        projectionList.add(Projections.max(TemporalReferencedSeriesObservation.PHENOMENON_TIME_END));
        c.setProjection(projectionList);
    }

    /**
     * Get default {@link Criteria} for {@link Class}
     *
     * @param session
     *            Hibernate Session
     * @return Default {@link Criteria}
     */
    protected Criteria getDefaultObservationCriteria(Session session) {
        return getDefaultCriteria(getSeriesValueTimeClass(), session);
//        return session.createCriteria().add(Restrictions.eq(TemporalReferencedSeriesObservation.DELETED, false))
//                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    private void addMinMaxTimeProjection(Criteria c) {
        ProjectionList projectionList = Projections.projectionList();
        projectionList.add(Projections.min(TemporalReferencedSeriesObservation.PHENOMENON_TIME_START));
        projectionList.add(Projections.max(TemporalReferencedSeriesObservation.PHENOMENON_TIME_END));
        projectionList.add(Projections.max(TemporalReferencedSeriesObservation.RESULT_TIME));
        if (HibernateHelper.isColumnSupported(getSeriesValueTimeClass(), TemporalReferencedSeriesObservation.VALID_TIME_START)
                && HibernateHelper
                        .isColumnSupported(getSeriesValueTimeClass(), TemporalReferencedSeriesObservation.VALID_TIME_END)) {
            projectionList.add(Projections.min(TemporalReferencedSeriesObservation.VALID_TIME_START));
            projectionList.add(Projections.max(TemporalReferencedSeriesObservation.VALID_TIME_END));
        }
        c.setProjection(projectionList);
    }

    private ObservationTimeExtrema parseMinMaxTime(Object[] result) {
        ObservationTimeExtrema ote = new ObservationTimeExtrema();
        if (result != null) {
            ote.setMinPhenomenonTime(DateTimeHelper.makeDateTime(result[0]));
            ote.setMaxPhenomenonTime(DateTimeHelper.makeDateTime(result[1]));
            ote.setMaxResultTime(DateTimeHelper.makeDateTime(result[2]));
            if (result.length == 5) {
                ote.setMinValidTime(DateTimeHelper.makeDateTime(result[3]));
                ote.setMaxValidTime(DateTimeHelper.makeDateTime(result[4]));
            }
        }
        return ote;
    }

    /**
     * Create {@link Criteria} for parameter
     *
     * @param request
     *            {@link AbstractObservationRequest}
     * @param series
     *            Datasource series id
     * @param temporalFilterCriterion
     *            Temporal filter {@link Criterion}
     * @param sosIndeterminateTime
     *            first/latest indicator
     * @param session
     *            Hibernate Session
     * @return Resulting {@link Criteria}
     * @throws OwsExceptionReport
     *             If an error occurs when adding Spatial Filtering Profile
     *             restrictions
     */
    private Criteria getSeriesValueCriteriaFor(AbstractObservationRequest request, long series,
            Criterion temporalFilterCriterion, IndeterminateValue sosIndeterminateTime, Session session)
            throws OwsExceptionReport {
        final Criteria c = getDefaultObservationCriteria(session).createAlias(TemporalReferencedSeriesObservation.SERIES, "s");
        c.add(Restrictions.eq("s." + Series.ID, series));
        String logArgs = "request, series";
        if (request instanceof GetObservationRequest) {
            GetObservationRequest getObsReq = (GetObservationRequest)request;
            checkAndAddSpatialFilteringProfileCriterion(c, getObsReq, session);
            if (CollectionHelper.isNotEmpty(getObsReq.getOfferings())) {
                c.createCriteria(TemporalReferencedSeriesObservation.OFFERINGS).add(
                        Restrictions.in(Offering.IDENTIFIER, getObsReq.getOfferings()));
            }

            logArgs += ", offerings";
            if (temporalFilterCriterion != null) {
                logArgs += ", filterCriterion";
                c.add(temporalFilterCriterion);
            }
            if (sosIndeterminateTime != null) {
                logArgs += ", sosIndeterminateTime";
                addIndeterminateTimeRestriction(c, sosIndeterminateTime, logArgs);
            }
            addSpecificRestrictions(c, getObsReq);
        }
        LOGGER.debug("QUERY getSeriesObservationFor({}): {}", logArgs, HibernateHelper.getSqlString(c));
        return c;
    }

    private Criteria getSeriesValueCriteriaFor(AbstractObservationRequest request, Set<Long> series,
            Criterion temporalFilterCriterion, IndeterminateValue sosIndeterminateTime, Session session) throws OwsExceptionReport {
        final Criteria c = getDefaultObservationCriteria(session).createAlias(TemporalReferencedSeriesObservation.SERIES, "s");
        c.add(Restrictions.in("s." + Series.ID, series));
        String logArgs = "request, series";
        if (request instanceof GetObservationRequest) {
            GetObservationRequest getObsReq = (GetObservationRequest)request;
            checkAndAddSpatialFilteringProfileCriterion(c, getObsReq, session);
            if (CollectionHelper.isNotEmpty(getObsReq.getOfferings())) {
                c.createCriteria(TemporalReferencedSeriesObservation.OFFERINGS).add(
                        Restrictions.in(Offering.IDENTIFIER, getObsReq.getOfferings()));
            }

            logArgs += ", offerings";
            if (temporalFilterCriterion != null) {
                logArgs += ", filterCriterion";
                c.add(temporalFilterCriterion);
            }
            if (sosIndeterminateTime != null) {
                logArgs += ", sosIndeterminateTime";
                addIndeterminateTimeRestriction(c, sosIndeterminateTime, logArgs);
            }
            addSpecificRestrictions(c, getObsReq);
        }
        LOGGER.debug("QUERY getSeriesValueCriteriaFor({}): {}", logArgs, HibernateHelper.getSqlString(c));
        return c;
    }

    protected Criteria getSeriesValueCriteriaFor(Collection<Series> series,
            Criterion temporalFilterCriterion, IndeterminateValue sosIndeterminateTime, Session session)
            throws OwsExceptionReport {
        final Criteria c = getDefaultObservationCriteria(session);

        c.add(QueryHelper.getCriterionForObjects(SeriesObservation.SERIES, series));

        String logArgs = "request, series";
        addTemporalFilterCriterion(c, temporalFilterCriterion, logArgs);
        addIndeterminateTimeRestriction(c, sosIndeterminateTime, logArgs);
        LOGGER.debug("QUERY getSeriesObservationFor({}): {}", logArgs, HibernateHelper.getSqlString(c));
        return c;
    }

    protected Criteria getSeriesValueCriteriaForSeriesIds(Collection<Long> series,
            Criterion temporalFilterCriterion, IndeterminateValue sosIndeterminateTime, Session session)
            throws OwsExceptionReport {
        final Criteria c = getDefaultObservationCriteria(session).createAlias(SeriesObservation.SERIES, "s");

        c.add(QueryHelper.getCriterionForObjects("s." + Series.ID, series));

        String logArgs = "request, series";
        addTemporalFilterCriterion(c, temporalFilterCriterion, logArgs);
        addIndeterminateTimeRestriction(c, sosIndeterminateTime, logArgs);
        LOGGER.debug("QUERY getSeriesObservationFor({}): {}", logArgs, HibernateHelper.getSqlString(c));
        return c;
    }
}
