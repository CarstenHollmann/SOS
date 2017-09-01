package org.n52.sw.db.dao;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import org.hibernate.Criteria;
import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projection;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.Subqueries;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.spatial.criterion.SpatialProjections;
import org.hibernate.transform.ResultTransformer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.n52.series.db.beans.AbstractFeatureEntity;
import org.n52.series.db.beans.BlobDatasetEntity;
import org.n52.series.db.beans.BooleanDatasetEntity;
import org.n52.series.db.beans.CategoryDatasetEntity;
import org.n52.series.db.beans.CodespaceEntity;
import org.n52.series.db.beans.ComplexDataEntity;
import org.n52.series.db.beans.ComplexDatasetEntity;
import org.n52.series.db.beans.CountDatasetEntity;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.GeometryDatasetEntity;
import org.n52.series.db.beans.GeometryEntity;
import org.n52.series.db.beans.HibernateRelations;
import org.n52.series.db.beans.ObservationConstellationEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProfileDataEntity;
import org.n52.series.db.beans.ProfileDatasetEntity;
import org.n52.series.db.beans.QuantityDatasetEntity;
import org.n52.series.db.beans.SweDataArrayDatasetEntity;
import org.n52.series.db.beans.TextDatasetEntity;
import org.n52.series.db.beans.UnitEntity;
import org.n52.series.db.dao.DataDao;
import org.n52.shetland.ogc.UoM;
import org.n52.shetland.ogc.filter.TemporalFilter;
import org.n52.shetland.ogc.filter.FilterConstants.TimeOperator;
import org.n52.shetland.ogc.gml.time.IndeterminateValue;
import org.n52.shetland.ogc.gml.time.Time;
import org.n52.shetland.ogc.gml.time.TimeInstant;
import org.n52.shetland.ogc.gml.time.TimePeriod;
import org.n52.shetland.ogc.gwml.GWMLConstants;
import org.n52.shetland.ogc.om.NamedValue;
import org.n52.shetland.ogc.om.OmConstants;
import org.n52.shetland.ogc.om.OmObservation;
import org.n52.shetland.ogc.om.SingleObservationValue;
import org.n52.shetland.ogc.om.values.BooleanValue;
import org.n52.shetland.ogc.om.values.CategoryValue;
import org.n52.shetland.ogc.om.values.ComplexValue;
import org.n52.shetland.ogc.om.values.CountValue;
import org.n52.shetland.ogc.om.values.CvDiscretePointCoverage;
import org.n52.shetland.ogc.om.values.GeometryValue;
import org.n52.shetland.ogc.om.values.HrefAttributeValue;
import org.n52.shetland.ogc.om.values.MultiPointCoverage;
import org.n52.shetland.ogc.om.values.NilTemplateValue;
import org.n52.shetland.ogc.om.values.ProfileLevel;
import org.n52.shetland.ogc.om.values.ProfileValue;
import org.n52.shetland.ogc.om.values.QuantityRangeValue;
import org.n52.shetland.ogc.om.values.QuantityValue;
import org.n52.shetland.ogc.om.values.RectifiedGridCoverage;
import org.n52.shetland.ogc.om.values.ReferenceValue;
import org.n52.shetland.ogc.om.values.SweDataArrayValue;
import org.n52.shetland.ogc.om.values.TLVTValue;
import org.n52.shetland.ogc.om.values.TVPValue;
import org.n52.shetland.ogc.om.values.TextValue;
import org.n52.shetland.ogc.om.values.TimeRangeValue;
import org.n52.shetland.ogc.om.values.UnknownValue;
import org.n52.shetland.ogc.om.values.Value;
import org.n52.shetland.ogc.om.values.XmlValue;
import org.n52.shetland.ogc.om.values.visitor.ProfileLevelVisitor;
import org.n52.shetland.ogc.om.values.visitor.ValueVisitor;
import org.n52.shetland.ogc.ows.exception.CodedException;
import org.n52.shetland.ogc.ows.exception.InvalidParameterValueException;
import org.n52.shetland.ogc.ows.exception.MissingParameterValueException;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OptionNotSupportedException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.ExtendedIndeterminateTime;
import org.n52.shetland.ogc.sos.Sos2Constants;
import org.n52.shetland.ogc.sos.request.GetObservationRequest;
import org.n52.shetland.ogc.swe.SweAbstractDataComponent;
import org.n52.shetland.ogc.swe.SweAbstractDataRecord;
import org.n52.shetland.ogc.swe.SweField;
import org.n52.shetland.util.CollectionHelper;
import org.n52.shetland.util.DateTimeHelper;
import org.n52.shetland.util.JavaHelper;
import org.n52.shetland.util.ReferencedEnvelope;
import org.n52.sos.ds.hibernate.entities.observation.series.Series;
import org.n52.sos.ds.hibernate.util.SpatialRestrictions;
import org.n52.sos.util.GeometryHandler;
import org.n52.sw.db.beans.ObservationVisitor;
import org.n52.sw.db.util.HibernateHelper;
import org.n52.sw.db.util.ObservationTimeExtrema;
import org.n52.sw.db.util.ScrollableIterable;
import org.n52.sw.db.util.TimeExtrema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

public abstract class AbstractObservationDao extends DataDao<DataEntity> implements Dao, TimeCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractObservationDao.class);
    private static final String SQL_QUERY_CHECK_SAMPLING_GEOMETRIES = "checkSamplingGeometries";
    private static final String SQL_QUERY_OBSERVATION_TIME_EXTREMA = "getObservationTimeExtrema";
    private org.n52.sw.db.dao.DaoFactory daoFactory;

    public AbstractObservationDao(DaoFactory daoFactory, Session session) {
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
     * Add observation identifier (procedure, observableProperty,
     * featureOfInterest) to observation
     *
     * @param observationIdentifiers
     *            Observation identifiers
     * @param observation
     *            Observation to add identifiers
     * @param session
     *
     * @throws OwsExceptionReport
     */
    protected abstract void addObservationContextToObservation(ObservationContext observationIdentifiers,
            DataEntity<?> observation) throws OwsExceptionReport;

    /**
     * Get Hibernate Criteria for querying observations with parameters
     * featureOfInterst and procedure
     *
     * @param feature
     *            FeatureOfInterest to query for
     * @param procedure
     *            Procedure to query for
     * @param session
     *            Hiberante Session
     *
     * @return Criteria to query observations
     */
    public abstract Criteria getObservationInfoCriteriaForFeatureOfInterestAndProcedure(String feature,
            String procedure);

    /**
     * Get Hibernate Criteria for querying observations with parameters
     * featureOfInterst and offering
     *
     * @param feature
     *            FeatureOfInterest to query for
     * @param offering
     *            Offering to query for
     * @param session
     *            Hiberante Session
     *
     * @return Criteria to query observations
     */
    public abstract Criteria getObservationInfoCriteriaForFeatureOfInterestAndOffering(String feature, String offering,
            Session session);

    /**
     * Get Hibernate Criteria for observation with restriction procedure
     *
     * @param procedure
     *            Procedure parameter
     * @param session
     *            Hibernate session
     *
     * @return Hibernate Criteria to query observations
     *
     * @throws OwsExceptionReport
     */
    public abstract Criteria getObservationCriteriaForProcedure(String procedure)
            throws OwsExceptionReport;

    /**
     * Get Hibernate Criteria for observation with restriction
     * observableProperty
     *
     * @param observableProperty
     * @param session
     *            Hibernate session
     *
     * @return Hibernate Criteria to query observations
     *
     * @throws OwsExceptionReport
     */
    public abstract Criteria getObservationCriteriaForObservableProperty(String observableProperty)
            throws OwsExceptionReport;

    /**
     * Get Hibernate Criteria for observation with restriction featureOfInterest
     *
     * @param featureOfInterest
     * @param session
     *            Hibernate session
     *
     * @return Hibernate Criteria to query observations
     *
     * @throws OwsExceptionReport
     */
    public abstract Criteria getObservationCriteriaForFeatureOfInterest(String featureOfInterest)
            throws OwsExceptionReport;

    /**
     * Get Hibernate Criteria for observation with restrictions procedure and
     * observableProperty
     *
     * @param procedure
     * @param observableProperty
     * @param session
     *            Hibernate session
     *
     * @return Hibernate Criteria to query observations
     *
     * @throws OwsExceptionReport
     */
    public abstract Criteria getObservationCriteriaFor(String procedure, String observableProperty)
            throws OwsExceptionReport;

    /**
     * Get Hibernate Criteria for observation with restrictions procedure,
     * observableProperty and featureOfInterest
     *
     * @param procedure
     * @param observableProperty
     * @param featureOfInterest
     * @param session
     *            Hibernate session
     *
     * @return Hibernate Criteria to query observations
     *
     * @throws OwsExceptionReport
     */
    public abstract Criteria getObservationCriteriaFor(String procedure, String observableProperty,
            String featureOfInterest) throws OwsExceptionReport;

    /**
     * Get all observation identifiers for a procedure.
     *
     * @param procedureIdentifier
     * @param session
     *
     * @return Collection of observation identifiers
     */
    public abstract Collection<String> getObservationIdentifiers(String procedureIdentifier);

    /**
     * Get Hibernate Criteria for {@link TemporalReferencedObservation} with
     * restrictions observation identifiers
     *
     * @param bservation
     *
     * @param observationConstellation
     *            The observation with restriction values
     * @param session
     *            Hibernate session
     *
     * @return Hibernate Criteria to query observations
     *
     * @throws OwsExceptionReport
     */
    public abstract Criteria getTemoralReferencedObservationCriteriaFor(OmObservation observation,
            ObservationConstellationEntity observationConstellation) throws OwsExceptionReport;

    /**
     * Query observation by identifier
     *
     * @param identifier
     *            Observation identifier (gml:identifier)
     * @param session
     *            Hiberante session
     *
     * @return Observation
     */
    public DataEntity<?> getObservationByIdentifier(String identifier) {
        Criteria criteria = getDefaultDataEntityCriteria();
        addObservationIdentifierToCriteria(criteria, identifier);
        return (DataEntity<?>) criteria.uniqueResult();
    }

    /**
     * Query observation by identifiers
     *
     * @param identifiers
     *            Observation identifiers (gml:identifier)
     * @param session
     *            Hiberante session
     * @return Observation
     */
    @SuppressWarnings("unchecked")
    public List<DataEntity<?>> getObservationByIdentifiers(Set<String> identifiers) {
        Criteria criteria = getDefaultDataEntityCriteria();
        addObservationIdentifierToCriteria(criteria, identifiers);
        return criteria.list();
    }

    /**
     * Check if there are numeric observations for the offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     *
     * @return If there are observations or not
     */
    public boolean checkNumericObservationsFor(String offeringIdentifier) {
        return check(getObservationFactory().numericClass(), offeringIdentifier);
    }

    /**
     * Check if there are boolean observations for the offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     *
     * @return If there are observations or not
     */
    public boolean checkBooleanObservationsFor(String offeringIdentifier) {
        return check(getObservationFactory().truthClass(), offeringIdentifier);
    }

    /**
     * Check if there are count observations for the offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     *
     * @return If there are observations or not
     */
    public boolean checkCountObservationsFor(String offeringIdentifier) {
        return check(getObservationFactory().countClass(), offeringIdentifier);
    }

    /**
     * Check if there are category observations for the offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     * @return If there are observations or not
     */
    public boolean checkCategoryObservationsFor(String offeringIdentifier) {
        return check(getObservationFactory().categoryClass(), offeringIdentifier);
    }

    /**
     * Check if there are text observations for the offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     *
     * @return If there are observations or not
     */
    public boolean checkTextObservationsFor(String offeringIdentifier) {
        return check(getObservationFactory().textClass(), offeringIdentifier);
    }

    /**
     * Check if there are complex observations for the offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     * @return If there are observations or not
     */
    public boolean checkComplexObservationsFor(String offeringIdentifier) {
        return check(getObservationFactory().complexClass(), offeringIdentifier);
    }

    /**
     * Check if there are profile observations for the offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     * @return If there are observations or not
     */
    public boolean checkProfileObservationsFor(String offeringIdentifier) {
        return check(getObservationFactory().profileClass(), offeringIdentifier);
    }

    /**
     * Check if there are blob observations for the offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     *
     * @return If there are observations or not
     */
    public boolean checkBlobObservationsFor(String offeringIdentifier) {
        return check(getObservationFactory().blobClass(), offeringIdentifier);
    }

    /**
     * Check if there are geometry observations for the offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     *
     * @return If there are observations or not
     */
    public boolean checkGeometryObservationsFor(String offeringIdentifier) {
        return check(getObservationFactory().geometryClass(), offeringIdentifier);
    }

    /**
     * Check if there are SweDataArray observations for the offering
     *
     * @param offeringIdentifier
     *            Offering identifier
     *
     * @return If there are observations or not
     */
    public boolean checkSweDataArrayObservationsFor(String offeringIdentifier) {
        return check(getObservationFactory().sweDataArrayClass(), offeringIdentifier);
    }

    /**
     * Get Hibernate Criteria for result model
     *
     * @param resultModel
     *            Result model
     *
     * @return Hibernate Criteria
     */
    public Criteria getObservationClassCriteriaForResultModel(String resultModel) {
        return createCriteriaForObservationClass(getObservationFactory().classForDataEntityType(resultModel));
    }

    /**
     * Get default Hibernate Criteria to query observations, default flag ==
     * <code>false</code>
     *
     * @return Default Criteria
     */
    public Criteria getDefaultDataEntityCriteria() {
        Criteria criteria = getDefaultCriteria(getObservationFactory().observationClass())
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
        Criteria seriesCriteria = criteria.createCriteria(DataEntity.PROPERTY_DATASETS);
        seriesCriteria.add(Restrictions.eq(DatasetEntity.PROPERTY_DELETED, false));
        seriesCriteria.add(Restrictions.eq(DatasetEntity.PROPERTY_PUBLISHED, true));
        return criteria;
    }

    /**
     * Get default Hibernate Criteria to query observation info, default flag ==
     * <code>false</code>
     *
     * @return Default Criteria
     */
    public Criteria getDefaultObservationInfoCriteria() {
        return getDefaultCriteria(getObservationFactory().observationClass());
    }

    /**
     * Get default Hibernate Criteria to query observation time, default flag ==
     * <code>false</code>
     *
     * @return Default Criteria
     */
    public Criteria getDefaultObservationTimeCriteria() {
        return getDefaultCriteria(getObservationFactory().observationClass());
    }

    @SuppressWarnings("rawtypes")
    private Criteria getDefaultCriteria(Class clazz) {
        Criteria criteria = session.createCriteria(clazz).add(Restrictions.eq(DataEntity.PROPERTY_DELETED, false));

        if (!isIncludeChildObservableProperties()) {
            criteria.add(Restrictions.eq(DataEntity.PROPERTY_CHILD, false));
        } else {
            criteria.add(Restrictions.eq(DataEntity.PROPERTY_PARENT, false));
        }

        return criteria.setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    /**
     * Get Hibernate Criteria for observation with restriction procedure Insert
     * a multi value observation for observation constellations and
     * featureOfInterest
     *
     * @param observationConstellations
     *                                  Observation constellation objects
     * @param feature
     *                                  FeatureOfInterest object
     * @param containerObservation
     *                                  SOS observation
     * @param codespaceCache
     *                                  Map based codespace object cache to prevent redundant queries
     * @param unitCache
     *                                  Map based unit object cache to prevent redundant queries
     * @param session
     *                                  Hibernate session
     *
     * @throws OwsExceptionReport
     *                            If an error occurs
     */
    public void insertObservationMultiValue(Set<ObservationConstellationEntity> observationConstellations,
            AbstractFeatureEntity feature, OmObservation containerObservation,
                                            Map<String, CodespaceEntity> codespaceCache,
                                            Map<UoM, UnitEntity> unitCache) throws OwsExceptionReport {
        List<OmObservation> unfoldObservations = HibernateObservationUtilities.unfoldObservation(containerObservation);
        for (OmObservation sosObservation : unfoldObservations) {
            insertObservationSingleValue(observationConstellations, feature, sosObservation, codespaceCache, unitCache);
        }
    }

    /**
     * Insert a single observation for observation constellations and
     * featureOfInterest without local caching for codespaces and units
     *
     * @param hObservationConstellations
     *                                   Observation constellation objects
     * @param hFeature
     *                                   FeatureOfInterest object
     * @param sosObservation
     *                                   SOS observation to insert
     * @param session
     *                                   Hibernate session
     *
     * @throws OwsExceptionReport
     */
    public void insertObservationSingleValue(Set<ObservationConstellationEntity> hObservationConstellations,
            AbstractFeatureEntity hFeature, OmObservation sosObservation)
            throws OwsExceptionReport {
        insertObservationSingleValue(hObservationConstellations, hFeature, sosObservation, null, null);
    }

    /**
     * Insert a single observation for observation constellations and
     * featureOfInterest with local caching for codespaces and units
     *
     * @param hObservationConstellations
     *                                   Observation constellation objects
     * @param hFeature
     *                                   FeatureOfInterest object
     * @param sosObservation
     *                                   SOS observation to insert
     * @param codespaceCache
     *                                   Map cache for codespace objects (to prevent redundant
     *                                   querying)
     * @param unitCache
     *                                   Map cache for unit objects (to prevent redundant querying)
     * @param session
     *                                   Hibernate session
     *
     * @throws OwsExceptionReport
     */
    @SuppressWarnings("rawtypes")
    public void insertObservationSingleValue(Set<ObservationConstellationEntity> hObservationConstellations,
                                             AbstractFeatureEntity hFeature, OmObservation sosObservation,
                                             Map<String, CodespaceEntity> codespaceCache,
                                             Map<UoM, UnitEntity> unitCache)
            throws OwsExceptionReport {
        SingleObservationValue<?> value
                = (SingleObservationValue) sosObservation.getValue();
        ObservationPersister persister = new ObservationPersister(
                getGeometryHandler(),
                this,
                this.daoFactory,
                sosObservation,
                hObservationConstellations,
                hFeature,
                codespaceCache,
                unitCache,
                getOfferings(hObservationConstellations),
                session
        );
        value.getValue().accept(persister);
    }

    private Set<OfferingEntity> getOfferings(Set<ObservationConstellationEntity> hObservationConstellations) {
        Set<OfferingEntity> offerings = Sets.newHashSet();
        for (ObservationConstellationEntity observationConstellation : hObservationConstellations) {
            offerings.add(observationConstellation.getOffering());
        }
        return offerings;
    }

    protected ObservationContext createObservationContext() {
        return new ObservationContext();
    }

    protected ObservationContext fillObservationContext(ObservationContext ctx, OmObservation sosObservation,
            Session session) {
        return ctx;
    }

    /**
     * If the local codespace cache isn't null, use it when retrieving
     * codespaces.
     *
     * @param codespace
     *            CodespaceEntity
     * @param localCache
     *            Cache (possibly null)
     * @param session
     *
     * @return CodespaceEntity
     */
    protected CodespaceEntity getCodespaceEntity(String codespace, Map<String, CodespaceEntity> localCache) {
        if (localCache != null && localCache.containsKey(codespace)) {
            return localCache.get(codespace);
        } else {
            // query codespace and set cache
            CodespaceEntity hCodespaceEntity = daoFactory.getCodespaceDao(getSession()).getOrInsert(codespace);
            if (localCache != null) {
                localCache.put(codespace, hCodespaceEntity);
            }
            return hCodespaceEntity;
        }
    }

    /**
     * If the local unit cache isn't null, use it when retrieving unit.
     *
     * @param unit
     *            UnitEntity
     * @param localCache
     *            Cache (possibly null)
     * @param session
     *
     * @return UnitEntity
     */
    protected UnitEntity getUnitEntity(String unit, Map<UoM, UnitEntity> localCache) {
        return getUnit(new UoM(unit), localCache);
    }

    /**
     * If the local unit cache isn't null, use it when retrieving unit.
     *
     * @param unit
     *            UnitEntity
     * @param localCache
     *            Cache (possibly null)
     * @return UnitEntity
     */
    protected UnitEntity getUnit(UoM unit, Map<UoM, UnitEntity> localCache) {
        if (localCache != null && localCache.containsKey(unit)) {
            return localCache.get(unit);
        } else {
            // query unit and set cache
            UnitEntity hUnitEntity = daoFactory.getUnitDao(getSession()).getOrInsert(unit);
            if (localCache != null) {
                localCache.put(unit, hUnitEntity);
            }
            return hUnitEntity;
        }
    }

    /**
     * Add observation identifier (gml:identifier) to Hibernate Criteria
     *
     * @param criteria
     *            Hibernate Criteria
     * @param identifier
     *            Observation identifier (gml:identifier)
     * @param session
     *            Hibernate session
     */
    protected void addObservationIdentifierToCriteria(Criteria criteria, String identifier) {
        criteria.add(Restrictions.eq(DataEntity.PROPERTY_DOMAIN_ID, identifier));
    }

    /**
     * Add observation identifiers (gml:identifier) to Hibernate Criteria
     *
     * @param criteria
     *            Hibernate Criteria
     * @param identifiers
     *            Observation identifiers (gml:identifier)
     * @param session
     *            Hibernate session
     */
    protected void addObservationIdentifierToCriteria(Criteria criteria, Set<String> identifiers) {
        criteria.add(Restrictions.in(DataEntity.PROPERTY_DOMAIN_ID, identifiers));
    }

    // /**
    // * Add offerings to observation and return the observation identifiers
    // * procedure and observableProperty
    // *
    // * @param hObservation
    // * Observation to add offerings
    // * @param hObservationConstellations
    // * Observation constellation with offerings, procedure and
    // * observableProperty
    // * @return ObservaitonIdentifiers object with procedure and
    // * observableProperty
    // */
    // protected ObservationIdentifiers
    // addOfferingsToObaservationAndGetProcedureObservableProperty(
    // AbstractObservation hObservation, Set<ObservationConstellation>
    // hObservationConstellations) {
    // Iterator<ObservationConstellation> iterator =
    // hObservationConstellations.iterator();
    // boolean firstObsConst = true;
    // ObservationIdentifiers observationIdentifiers = new
    // ObservationIdentifiers();
    // while (iterator.hasNext()) {
    // ObservationConstellation observationConstellation = iterator.next();
    // if (firstObsConst) {
    // observationIdentifiers.setObservableProperty(observationConstellation.getObservableProperty());
    // observationIdentifiers.setProcedure(observationConstellation.getProcedure());
    // firstObsConst = false;
    // }
    // hObservation.getOfferings().add(observationConstellation.getOffering());
    // }
    // return observationIdentifiers;
    // }
    protected void finalizeObservationInsertion(OmObservation sosObservation, DataEntity<?> hObservation,
            Session session) throws OwsExceptionReport {
        // TODO if this observation is a deleted=true, how to set deleted=false
        // instead of insert

    }

    /**
     * Insert om:parameter into database. Differs between Spatial Filtering
     * Profile parameter and others.
     *
     * @param parameter
     *            om:Parameter to insert
     * @param observation
     *            related observation
     * @param session
     *            Hibernate session
     *
     * @throws OwsExceptionReport
     */
    @Deprecated
    protected void insertParameter(Collection<NamedValue<?>> parameter, DataEntity<?> observation)
            throws OwsExceptionReport {
        for (NamedValue<?> namedValue : parameter) {
            if (!Sos2Constants.HREF_PARAMETER_SPATIAL_FILTERING_PROFILE.equals(namedValue.getName().getHref())) {
                throw new OptionNotSupportedException().at("om:parameter")
                        .withMessage("The om:parameter support is not yet implemented!");
            }
        }
    }

    /**
     * Check if there are observations for the offering
     *
     * @param clazz
     *            Observation sub class
     * @param offeringIdentifier
     *            Offering identifier
     * @param session
     *            Hibernate session
     *
     * @return If there are observations or not
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected boolean check(Class clazz, String offeringIdentifier) {
        Criteria c = session.createCriteria(clazz).add(Restrictions.eq(DataEntity.PROPERTY_DELETED, false));
        c.createCriteria(DataEntity.PROPERTY_DATASETS).createCriteria(DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION)
                .createCriteria(ObservationConstellationEntity.OFFERING)
                .add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offeringIdentifier));
        c.setMaxResults(1);
        LOGGER.debug("QUERY checkObservationFor(clazz, offeringIdentifier): {}", HibernateHelper.getSqlString(c));
        return CollectionHelper.isNotEmpty(c.list());
    }

    /**
     * Get min phenomenon time from observations
     *
     * @param session
     *            Hibernate session Hibernate session
     *
     * @return min time
     */
    public DateTime getMinPhenomenonTime(Session session) {
        Criteria criteria = session.createCriteria(getObservationFactory().observationClass())
                .setProjection(Projections.min(DataEntity.PROPERTY_PHENOMENON_TIME_START))
                .add(Restrictions.eq(DataEntity.PROPERTY_DELETED, false));
        LOGGER.debug("QUERY getMinPhenomenonTime(): {}", HibernateHelper.getSqlString(criteria));
        Object min = criteria.uniqueResult();
        if (min != null) {
            return new DateTime(min, DateTimeZone.UTC);
        }
        return null;
    }

    /**
     * Get max phenomenon time from observations
     *
     * @param session
     *            Hibernate session Hibernate session
     *
     * @return max time
     */
    public DateTime getMaxPhenomenonTime(Session session) {

        Criteria criteriaStart = session.createCriteria(getObservationFactory().observationClass())
                .setProjection(Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_START))
                .add(Restrictions.eq(DataEntity.PROPERTY_DELETED, false));
        LOGGER.debug("QUERY getMaxPhenomenonTime() start: {}", HibernateHelper.getSqlString(criteriaStart));
        Object maxStart = criteriaStart.uniqueResult();

        Criteria criteriaEnd = session.createCriteria(getObservationFactory().observationClass())
                .setProjection(Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_END))
                .add(Restrictions.eq(DataEntity.PROPERTY_DELETED, false));
        LOGGER.debug("QUERY getMaxPhenomenonTime() end: {}", HibernateHelper.getSqlString(criteriaEnd));
        Object maxEnd = criteriaEnd.uniqueResult();
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
     * Get min result time from observations
     *
     * @param session
     *            Hibernate session Hibernate session
     *
     * @return min time
     */
    public DateTime getMinResultTime(Session session) {

        Criteria criteria = session.createCriteria(getObservationFactory().observationClass())
                .setProjection(Projections.min(DataEntity.PROPERTY_RESULT_TIME))
                .add(Restrictions.eq(DataEntity.PROPERTY_DELETED, false));
        LOGGER.debug("QUERY getMinResultTime(): {}", HibernateHelper.getSqlString(criteria));
        Object min = criteria.uniqueResult();
        return (min == null) ? null : new DateTime(min, DateTimeZone.UTC);
    }

    /**
     * Get max phenomenon time from observations
     *
     * @param session
     *            Hibernate session Hibernate session
     *
     * @return max time
     */
    public DateTime getMaxResultTime(Session session) {

        Criteria criteria = session.createCriteria(getObservationFactory().observationClass())
                .setProjection(Projections.max(DataEntity.PROPERTY_RESULT_TIME))
                .add(Restrictions.eq(DataEntity.PROPERTY_DELETED, false));
        LOGGER.debug("QUERY getMaxResultTime(): {}", HibernateHelper.getSqlString(criteria));
        Object max = criteria.uniqueResult();
        return (max == null) ? null : new DateTime(max, DateTimeZone.UTC);
    }

    /**
     * Get global temporal bounding box
     *
     * @param session
     *            Hibernate session the session
     *
     * @return the global getEqualRestiction bounding box over all observations,
     *         or <tt>null</tt>
     */
    public TimePeriod getGlobalTemporalBoundingBox(Session session) {
        if (session != null) {
            Criteria criteria = session.createCriteria(getObservationFactory().observationClass());
            criteria.add(Restrictions.eq(DataEntity.PROPERTY_DELETED, false));
            criteria.setProjection(Projections.projectionList().add(Projections.min(DataEntity.PROPERTY_PHENOMENON_TIME_START))
                    .add(Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_START))
                    .add(Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_END)));
            LOGGER.debug("QUERY getGlobalTemporalBoundingBox(): {}", HibernateHelper.getSqlString(criteria));
            Object temporalBoundingBox = criteria.uniqueResult();
            if (temporalBoundingBox instanceof Object[]) {
                Object[] record = (Object[]) temporalBoundingBox;
                TimePeriod bBox =
                        createTimePeriod((Timestamp) record[0], (Timestamp) record[1], (Timestamp) record[2]);
                return bBox;
            }
        }
        return null;
    }

    /**
     * Get order for {@link ExtendedIndeterminateTime} value
     *
     * @param indetTime
     *            Value to get order for
     *
     * @return Order
     */
    protected Order getOrder(IndeterminateValue indetTime) {
        if (indetTime.equals(ExtendedIndeterminateTime.FIRST)) {
            return Order.asc(DataEntity.PROPERTY_PHENOMENON_TIME_START);
        } else if (indetTime.equals(ExtendedIndeterminateTime.LATEST)) {
            return Order.desc(DataEntity.PROPERTY_PHENOMENON_TIME_END);
        }
        return null;
    }

    /**
     * Get projection for {@link ExtendedIndeterminateTime} value
     *
     * @param indetTime
     *            Value to get projection for
     *
     * @return Projection to use to determine indeterminate time extrema
     */
    protected Projection getIndeterminateTimeExtremaProjection(IndeterminateValue indetTime) {
        if (indetTime.equals(ExtendedIndeterminateTime.FIRST)) {
            return Projections.min(DataEntity.PROPERTY_PHENOMENON_TIME_START);
        } else if (indetTime.equals(ExtendedIndeterminateTime.LATEST)) {
            return Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_END);
        }
        return null;
    }

    /**
     * Get the Observation property to filter on for an
     * {@link ExtendedIndeterminateTime}
     *
     * @param indetTime
     *            Value to get property for
     *
     * @return String property to filter on
     */
    protected String getIndeterminateTimeFilterProperty(IndeterminateValue indetTime) {
        if (indetTime.equals(ExtendedIndeterminateTime.FIRST)) {
            return DataEntity.PROPERTY_PHENOMENON_TIME_START;
        } else if (indetTime.equals(ExtendedIndeterminateTime.LATEST)) {
            return DataEntity.PROPERTY_PHENOMENON_TIME_END;
        }
        return null;
    }

    /**
     * Add an indeterminate time restriction to a criteria. This allows for
     * multiple results if more than one observation has the extrema time (max
     * for latest, min for first). Note: use this method *after* adding all
     * other applicable restrictions so that they will apply to the min/max
     * observation time determination.
     *
     * @param c
     *            Criteria to add the restriction to
     * @param sosIndeterminateTime
     *            Indeterminate time restriction to add
     *
     * @return Modified criteria
     */
    protected Criteria addIndeterminateTimeRestriction(Criteria c, IndeterminateValue sosIndeterminateTime) {
        // get extrema indeterminate time
        c.setProjection(getIndeterminateTimeExtremaProjection(sosIndeterminateTime));
        Timestamp indeterminateExtremaTime = (Timestamp) c.uniqueResult();
        return addIndeterminateTimeRestriction(c, sosIndeterminateTime, indeterminateExtremaTime);
    }

    /**
     * Add an indeterminate time restriction to a criteria. This allows for
     * multiple results if more than one observation has the extrema time (max
     * for latest, min for first). Note: use this method *after* adding all
     * other applicable restrictions so that they will apply to the min/max
     * observation time determination.
     *
     * @param c
     *            Criteria to add the restriction to
     * @param sosIndeterminateTime
     *            Indeterminate time restriction to add
     * @param indeterminateExtremaTime
     *            Indeterminate time extrema
     *
     * @return Modified criteria
     */
    protected Criteria addIndeterminateTimeRestriction(Criteria c, IndeterminateValue sosIndeterminateTime,
            Date indeterminateExtremaTime) {
        // reset criteria
        // see http://stackoverflow.com/a/1472958/193435
        c.setProjection(null);
        c.setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);

        // get observations with exactly the extrema time
        c.add(Restrictions.eq(getIndeterminateTimeFilterProperty(sosIndeterminateTime), indeterminateExtremaTime));

        // not really necessary to return the Criteria object, but useful if we
        // want to chain
        return c;
    }

    /**
     * Create Hibernate Criteria for Class
     *
     * @param clazz
     *            Class
     * @param session
     *            Hibernate session
     *
     * @return Hibernate Criteria for Class
     */
    @SuppressWarnings("rawtypes")
    protected Criteria createCriteriaForObservationClass(Class clazz) {
        return session.createCriteria(clazz).add(Restrictions.eq(DataEntity.PROPERTY_DELETED, false))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    /**
     * Add phenomenon and result time to observation object
     *
     * @param observation
     *            Observation object
     * @param phenomenonTime
     *            SOS phenomenon time
     * @param resultTime
     *            SOS result Time
     *
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    protected void addPhenomeonTimeAndResultTimeToObservation(DataEntity<?> observation, Time phenomenonTime,
            TimeInstant resultTime) throws OwsExceptionReport {
        addPhenomenonTimeToObservation(observation, phenomenonTime);
        addResultTimeToObservation(observation, resultTime, phenomenonTime);
    }

    /**
     * Add phenomenon and result time to observation object
     *
     * @param sosObservation
     *            the SOS observation
     * @param observation
     *            Observation object
     *
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    protected void addTime(OmObservation sosObservation, DataEntity<?> observation) throws OwsExceptionReport {
        addPhenomeonTimeAndResultTimeToObservation(observation, sosObservation.getPhenomenonTime(),
                sosObservation.getResultTime());
        addValidTimeToObservation(observation, sosObservation.getValidTime());
    }

    /**
     * Add phenomenon time to observation object
     *
     * @param observation
     *            Observation object
     * @param phenomenonTime
     *            SOS phenomenon time
     * @throws OwsExceptionReport
     */
    public void addPhenomenonTimeToObservation(DataEntity<?> observation, Time phenomenonTime)
            throws OwsExceptionReport {
        if (phenomenonTime instanceof TimeInstant) {
            TimeInstant time = (TimeInstant) phenomenonTime;
            if (time.isSetValue()) {
                observation.setPhenomenonTimeStart(time.getValue().toDate());
                observation.setPhenomenonTimeEnd(time.getValue().toDate());
            } else if (time.isSetIndeterminateValue()) {
                Date now = getDateForTimeIndeterminateValue(time.getIndeterminateValue(),
                        "gml:TimeInstant/gml:timePosition[@indeterminatePosition]");
                observation.setPhenomenonTimeStart(now);
                observation.setPhenomenonTimeEnd(now);
            } else {
                throw new MissingParameterValueException("gml:TimeInstant/gml:timePosition");
            }
        } else if (phenomenonTime instanceof TimePeriod) {
            TimePeriod time = (TimePeriod) phenomenonTime;
            if (time.isSetStart()) {
                observation.setPhenomenonTimeStart(time.getStart().toDate());
            } else if (time.isSetStartIndeterminateValue()) {
                observation.setPhenomenonTimeStart(getDateForTimeIndeterminateValue(time.getStartIndet(),
                        "gml:TimePeriod/gml:beginPosition[@indeterminatePosition]"));
            } else {
                throw new MissingParameterValueException("gml:TimePeriod/gml:beginPosition");
            }
            if (time.isSetEnd()) {
                observation.setPhenomenonTimeEnd(time.getEnd().toDate());
            } else if (time.isSetEndIndeterminateValue()) {
                observation.setPhenomenonTimeEnd(getDateForTimeIndeterminateValue(time.getEndIndet(),
                        "gml:TimePeriod/gml:endPosition[@indeterminatePosition]"));
            } else {
                throw new MissingParameterValueException("gml:TimePeriod/gml:endPosition");
            }

            observation.setPhenomenonTimeEnd(time.getEnd().toDate());
        }
    }

    /**
     * Add result time to observation object
     *
     * @param observation
     *            Observation object
     * @param resultTime
     *            SOS result time
     * @param phenomenonTime
     *            SOS phenomenon time
     *
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    public void addResultTimeToObservation(DataEntity<?> observation, TimeInstant resultTime, Time phenomenonTime)
            throws CodedException {
        if (resultTime != null) {
            if (resultTime.isSetValue()) {
                observation.setResultTime(resultTime.getValue().toDate());
            } else if (resultTime.isSetGmlId() && resultTime.getGmlId().contains(Sos2Constants.EN_PHENOMENON_TIME)
                    && phenomenonTime instanceof TimeInstant) {
                if (((TimeInstant) phenomenonTime).isSetValue()) {
                    observation.setResultTime(((TimeInstant) phenomenonTime).getValue().toDate());
                } else if (((TimeInstant) phenomenonTime).isSetIndeterminateValue()) {
                    observation.setResultTime(
                            getDateForTimeIndeterminateValue(((TimeInstant) phenomenonTime).getIndeterminateValue(),
                                    "gml:TimeInstant/gml:timePosition[@indeterminatePosition]"));
                } else {
                    throw new NoApplicableCodeException()
                            .withMessage("Error while adding result time to Hibernate Observation entitiy!");
                }
            } else if (resultTime.isSetIndeterminateValue()) {
                observation.setResultTime(getDateForTimeIndeterminateValue(resultTime.getIndeterminateValue(),
                        "gml:TimeInstant/gml:timePosition[@indeterminatePosition]"));
            } else {
                throw new NoApplicableCodeException()
                        .withMessage("Error while adding result time to Hibernate Observation entitiy!");
            }
        } else if (phenomenonTime instanceof TimeInstant) {
            observation.setResultTime(((TimeInstant) phenomenonTime).getValue().toDate());
        } else {
            throw new NoApplicableCodeException()
                    .withMessage("Error while adding result time to Hibernate Observation entitiy!");
        }
    }

    protected Date getDateForTimeIndeterminateValue(IndeterminateValue indeterminateValue, String parameter)
            throws InvalidParameterValueException {
        if (indeterminateValue.isNow()) {
            return new DateTime().toDate();
        }
        throw new InvalidParameterValueException(parameter, indeterminateValue.getValue());
    }

    /**
     * Add valid time to observation object
     *
     * @param observation
     *            Observation object
     * @param validTime
     *            SOS valid time
     */
    protected void addValidTimeToObservation(DataEntity<?> observation, TimePeriod validTime) {
        if (validTime != null) {
            observation.setValidTimeStart(validTime.getStart().toDate());
            observation.setValidTimeEnd(validTime.getEnd().toDate());
        }
    }

    /**
     * Update observations, set deleted flag
     *
     * @param scroll
     *            Observations to update
     * @param deleteFlag
     *            New deleted flag value
     * @param session
     *            Hibernate session
     */
    protected void updateObservation(ScrollableIterable<? extends DataEntity<?>> scroll, boolean deleteFlag,
            Session session) {
        if (scroll != null) {
            try {
                for (DataEntity<?> o : scroll) {
                    o.setDeleted(deleteFlag);
                    session.update(o);
                    session.flush();
                }
            } finally {
                scroll.close();
            }
        }
    }

    /**
     * Check if a Spatial Filtering Profile filter is requested and add to
     * criteria
     *
     * @param c
     *            Criteria to add crtierion
     * @param request
     *            GetObservation request
     *
     * @throws OwsExceptionReport
     *             If Spatial Filteirng Profile is not supported or an error
     *             occurs.
     */
    protected void checkAndAddSpatialFilteringProfileCriterion(Criteria c, GetObservationRequest request) throws OwsExceptionReport {
        if (request.hasSpatialFilteringProfileSpatialFilter()) {
            c.add(SpatialRestrictions.filter(DataEntity.PROPERTY_GEOMETRY_ENTITY, request.getSpatialFilter().getOperator(),
                    getGeometryHandler()
                            .switchCoordinateAxisFromToDatasourceIfNeeded(request.getSpatialFilter().getGeometry())));
        }
    }

    /**
     * Get all observation identifiers
     *
     * @param session
     *            Hibernate session
     *
     * @return Observation identifiers
     */
    @SuppressWarnings("unchecked")
    public List<String> getObservationIdentifier(Session session) {
        Criteria criteria = session.createCriteria(getObservationFactory().observationClass())
                .add(Restrictions.eq(DataEntity.PROPERTY_DELETED, false))
                .add(Restrictions.isNotNull(DataEntity.PROPERTY_DOMAIN_ID))
                .setProjection(Projections.distinct(Projections.property(DataEntity.PROPERTY_DOMAIN_ID)));
        LOGGER.debug("QUERY getObservationIdentifiers(): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    public ReferencedEnvelope getSpatialFilteringProfileEnvelopeForOfferingId(String offeringID)
            throws OwsExceptionReport {
        try {
            // XXX workaround for Hibernate Spatial's lack of support for
            // GeoDB's extent aggregate see
            // http://www.hibernatespatial.org/pipermail/hibernatespatial-users/2013-August/000876.html
            Dialect dialect = ((SessionFactoryImplementor) session.getSessionFactory()).getDialect();
            if (getGeometryHandler().isSpatialDatasource()
                    && HibernateHelper.supportsFunction(dialect, HibernateConstants.FUNC_EXTENT)) {
                Criteria criteria = getDefaultObservationInfoCriteria();
                criteria.setProjection(SpatialProjections.extent(DataEntity.PROPERTY_GEOMETRY_ENTITY));
                criteria.createCriteria(Observation.OFFERINGS).add(Restrictions.eq(Offering.IDENTIFIER, offeringID));
                LOGGER.debug("QUERY getSpatialFilteringProfileEnvelopeForOfferingId(offeringID): {}",
                        HibernateHelper.getSqlString(criteria));
                Geometry geom = (Geometry) criteria.uniqueResult();
                geom = getGeometryHandler().switchCoordinateAxisFromToDatasourceIfNeeded(geom);
                if (geom != null) {
                    return new ReferencedEnvelope(geom.getEnvelopeInternal(), getGeometryHandler().getStorageEPSG());
                }
            } else {
                Envelope envelope = null;
                Criteria criteria = getDefaultObservationInfoCriteria();
                criteria.createCriteria(AbstractObservation.OFFERINGS)
                        .add(Restrictions.eq(Offering.IDENTIFIER, offeringID));
                LOGGER.debug("QUERY getSpatialFilteringProfileEnvelopeForOfferingId(offeringID): {}",
                        HibernateHelper.getSqlString(criteria));
                @SuppressWarnings("unchecked")
                final List<DataEntity> observationTimes = criteria.list();
                if (CollectionHelper.isNotEmpty(observationTimes)) {
                    observationTimes.stream().filter(HasSamplingGeometry::hasSamplingGeometry)
                            .map(HasSamplingGeometry::getSamplingGeometry).filter(Objects::nonNull)
                            .filter(geom -> (geom != null && geom.getEnvelopeInternal() != null))
                            .forEachOrdered((geom) -> {
                                envelope.expandToInclude(geom.getEnvelopeInternal());
                            });
                    if (!envelope.isNull()) {
                        return new ReferencedEnvelope(envelope, getGeometryHandler().getStorageEPSG());
                    }
                }
                if (!envelope.isNull()) {
                    return new ReferencedEnvelope(envelope, GeometryHandler.getInstance().getStorageEPSG());
                }

            }
        } catch (final HibernateException he) {
            throw new NoApplicableCodeException().causedBy(he)
                    .withMessage("Exception thrown while requesting feature envelope for observation ids");
        }
        return null;
    }

    public abstract String addProcedureAlias(Criteria criteria);

    public abstract List<Geometry> getSamplingGeometries(String feature) throws OwsExceptionReport;

    public abstract Long getSamplingGeometriesCount(String feature) throws OwsExceptionReport;

    public abstract Envelope getBboxFromSamplingGeometries(String feature) throws OwsExceptionReport;

    public abstract ObservationFactory getObservationFactory();

    protected abstract Criteria addAdditionalObservationIdentification(Criteria c, OmObservation sosObservation);
    
    public abstract ObservationTimeExtrema getTimeExtremaForSeries(Collection<Series> series, Criterion temporalFilter, Session session) throws OwsExceptionReport;

    public abstract ObservationTimeExtrema getTimeExtremaForSeriesIds(Collection<Long> series, Criterion temporalFilter, Session session) throws OwsExceptionReport;

    /**
     * @param sosObservation
     *            {@link OmObservation} to check
     * @param session
     *            Hibernate {@link Session}
     *
     * @throws OwsExceptionReport
     */
    public void checkForDuplicatedObservations(OmObservation sosObservation,
            ObservationConstellationEntity observationConstellation) throws OwsExceptionReport {
        Criteria c = getTemoralReferencedObservationCriteriaFor(sosObservation, observationConstellation);
        addAdditionalObservationIdentification(c, sosObservation);
        // add times check (start/end phen, result)
        List<TemporalFilter> filters = Lists.newArrayListWithCapacity(2);
        filters.add(getPhenomeonTimeFilter(c, sosObservation.getPhenomenonTime()));
        filters.add(getResultTimeFilter(c, sosObservation.getResultTime(), sosObservation.getPhenomenonTime()));
        c.add(SosTemporalRestrictions.filter(filters));
        if (sosObservation.isSetHeightDepthParameter()) {
            NamedValue<Double> hdp = sosObservation.getHeightDepthParameter();
            addParameterRestriction(c, hdp);
        }
        c.setMaxResults(1);
        LOGGER.debug("QUERY checkForDuplicatedObservations(): {}", HibernateHelper.getSqlString(c));
        if (!c.list().isEmpty()) {
            StringBuilder builder = new StringBuilder();
            builder.append("procedure=").append(sosObservation.getObservationConstellation().getProcedureIdentifier());
            builder.append("observedProperty=")
                    .append(sosObservation.getObservationConstellation().getObservablePropertyIdentifier());
            builder.append("featureOfInter=")
                    .append(sosObservation.getObservationConstellation().getFeatureOfInterestIdentifier());
            builder.append("phenomenonTime=").append(sosObservation.getPhenomenonTime().toString());
            builder.append("resultTime=").append(sosObservation.getResultTime().toString());
            // TODO for e-Reporting SampligPoint should be added.
            if (sosObservation.isSetHeightDepthParameter()) {
                NamedValue<Double> hdp = sosObservation.getHeightDepthParameter();
                builder.append("height/depth=").append(hdp.getName().getHref()).append("/")
                        .append(hdp.getValue().getValue());
            }
            throw new NoApplicableCodeException().withMessage("The observation for %s already exists in the database!",
                    builder.toString());
        }
    }

    private void addParameterRestriction(Criteria c, NamedValue<?> hdp) throws OwsExceptionReport {
        c.add(Subqueries.propertyIn(HibernateRelations.HasParamerterId.ID,
                getParameterRestriction(c, hdp.getName().getHref(), hdp.getValue().getValue(),
                        hdp.getValue().accept(getParameterFactory()).getClass())));
    }

    protected DetachedCriteria getParameterRestriction(Criteria c, String name, Object value, Class<?> clazz) {
        DetachedCriteria detachedCriteria = DetachedCriteria.forClass(clazz);
        addParameterNameRestriction(detachedCriteria, name);
        addParameterValueRestriction(detachedCriteria, value);
        detachedCriteria.setProjection(Projections.distinct(Projections.property(Parameter.OBS_ID)));
        return detachedCriteria;
    }

    protected DetachedCriteria addParameterNameRestriction(DetachedCriteria detachedCriteria, String name) {
        detachedCriteria.add(Restrictions.eq(Parameter.NAME, name));
        return detachedCriteria;
    }

    protected DetachedCriteria addParameterValueRestriction(DetachedCriteria detachedCriteria, Object value) {
        detachedCriteria.add(Restrictions.eq(Parameter.VALUE, value));
        return detachedCriteria;
    }

    private TemporalFilter getPhenomeonTimeFilter(Criteria c, Time phenomenonTime) {
        return new TemporalFilter(TimeOperator.TM_Equals, phenomenonTime, Sos2Constants.EN_PHENOMENON_TIME);
    }

    private TemporalFilter getResultTimeFilter(Criteria c, TimeInstant resultTime, Time phenomenonTime)
            throws OwsExceptionReport {
        String valueReferencep = Sos2Constants.EN_RESULT_TIME;
        if (resultTime != null) {
            if (resultTime.getValue() != null) {
                return new TemporalFilter(TimeOperator.TM_Equals, resultTime, valueReferencep);
            } else if (phenomenonTime instanceof TimeInstant) {
                return new TemporalFilter(TimeOperator.TM_Equals, phenomenonTime, valueReferencep);
            } else {
                throw new NoApplicableCodeException()
                        .withMessage("Error while creating result time filter for querying observations!");
            }
        } else {
            if (phenomenonTime instanceof TimeInstant) {
                return new TemporalFilter(TimeOperator.TM_Equals, phenomenonTime, valueReferencep);
            } else {
                throw new NoApplicableCodeException()
                        .withMessage("Error while creating result time filter for querying observations!");
            }
        }
    }

    public boolean isIdentifierContained(String identifier) {
        Criteria c = getDefaultDataEntityCriteria().add(Restrictions.eq(DataEntity.PROPERTY_DOMAIN_ID, identifier));
        LOGGER.debug("QUERY isIdentifierContained(identifier): {}", HibernateHelper.getSqlString(c));
        return c.list().size() > 0;
    }

    public ParameterFactory getParameterFactory() {
        return ParameterFactory.getInstance();
    }

    /**
     * Check if the observation table contains samplingGeometries with values
     *
     * @param session
     *            Hibernate session
     * @return <code>true</code>, if the observation table contains
     *         samplingGeometries with values
     */
    public boolean containsSamplingGeometries(Session session) {
        Criteria criteria = getDefaultObservationInfoCriteria(session);
        criteria.setProjection(Projections.rowCount());
        if (HibernateHelper.isNamedQuerySupported(SQL_QUERY_CHECK_SAMPLING_GEOMETRIES, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_CHECK_SAMPLING_GEOMETRIES);
            LOGGER.debug("QUERY containsSamplingGeometries() with NamedQuery: {}",
                    SQL_QUERY_CHECK_SAMPLING_GEOMETRIES);
            return (boolean) namedQuery.uniqueResult();
        } else if (HibernateHelper.isColumnSupported(getObservationFactory().contextualReferencedClass(),
                DataEntity.PROPERTY_GEOMETRY_ENTITY)) {
            criteria.add(Restrictions.isNotNull(DataEntity.PROPERTY_GEOMETRY_ENTITY));
            LOGGER.debug("QUERY containsSamplingGeometries(): {}", HibernateHelper.getSqlString(criteria));
            return (Long) criteria.uniqueResult() > 0;
        } else if (HibernateHelper.isColumnSupported(getObservationFactory().contextualReferencedClass(),
                AbstractObservation.LONGITUDE)
                && HibernateHelper.isColumnSupported(getObservationFactory().contextualReferencedClass(),
                        AbstractObservation.LATITUDE)) {
            criteria.add(Restrictions.and(Restrictions.isNotNull(AbstractObservation.LONGITUDE),
                    Restrictions.isNotNull(AbstractObservation.LATITUDE)));
            LOGGER.debug("QUERY containsSamplingGeometries(): {}", HibernateHelper.getSqlString(criteria));
            return (Long) criteria.uniqueResult() > 0;
        }
        return false;
    }

    public TimeExtrema getObservationTimeExtrema(Session session) throws OwsExceptionReport {
        if (HibernateHelper.isNamedQuerySupported(SQL_QUERY_OBSERVATION_TIME_EXTREMA, session)) {
            Query namedQuery = session.getNamedQuery(SQL_QUERY_OBSERVATION_TIME_EXTREMA);
            LOGGER.debug("QUERY getObservationTimeExtrema() with NamedQuery: {}", SQL_QUERY_OBSERVATION_TIME_EXTREMA);
            namedQuery.setResultTransformer(new ObservationTimeTransformer());
            return (TimeExtrema) namedQuery.uniqueResult();
        } else {
            Criteria c = getDefaultObservationTimeCriteria().setProjection(
                    Projections.projectionList().add(Projections.min(DataEntity.PROPERTY_PHENOMENON_TIME_START))
                            .add(Projections.max(DataEntity.PROPERTY_PHENOMENON_TIME_END))
                            .add(Projections.min(DataEntity.PROPERTY_RESULT_TIME))
                            .add(Projections.max(DataEntity.PROPERTY_RESULT_TIME)));
            c.setResultTransformer(new ObservationTimeTransformer());
            return (TimeExtrema) c.uniqueResult();
        }
    }

    protected boolean isIncludeChildObservableProperties() {
        return ObservationSettingProvider.getInstance().isIncludeChildObservableProperties();
    }

    private GeometryHandler getGeometryHandler() {
        return GeometryHandler.getInstance();
    }

    private static class ObservationPersister
            implements ValueVisitor<DataEntity<?>, OwsExceptionReport>, ProfileLevelVisitor<DataEntity<?>> {
        private static final ObservationVisitor<String> SERIES_TYPE_VISITOR = new SeriesTypeVisitor();

        private final Set<ObservationConstellationEntity> observationConstellations;

        private final AbstractFeatureEntity featureOfInterest;

        private final Caches caches;

        private final Geometry samplingGeometry;

        private final DAOs daos;

        private final ObservationFactory observationFactory;

        private final OmObservation sosObservation;

        private final boolean childObservation;

        private final Set<OfferingEntity> offerings;

        private GeometryHandler geometryHandler;

        private Session session;

        ObservationPersister(
                GeometryHandler geometryHandler, AbstractObservationDao observationDao, DaoFactory daoFactory,
                OmObservation sosObservation, Set<ObservationConstellationEntity> hObservationConstellations,
                AbstractFeatureEntity hFeature, Map<String, CodespaceEntity> codespaceCache, Map<UoM, UnitEntity> unitCache,
                Set<OfferingEntity> hOfferings, Session session) throws OwsExceptionReport {
            this(geometryHandler, new DAOs(observationDao, daoFactory, session), new Caches(codespaceCache, unitCache),
                    sosObservation, hObservationConstellations, hFeature, null, hOfferings, false, session);
        }

        private ObservationPersister(
                GeometryHandler geometryHandler, DAOs daos, Caches caches, OmObservation observation,
                Set<ObservationConstellationEntity> hObservationConstellations, AbstractFeatureEntity hFeature,
                Geometry samplingGeometry, Set<OfferingEntity> hOfferings, boolean childObservation, Session session)
                throws OwsExceptionReport {
            this.observationConstellations = hObservationConstellations;
            this.featureOfInterest = hFeature;
            this.caches = caches;
            this.sosObservation = observation;
            this.samplingGeometry = samplingGeometry != null ? samplingGeometry : getSamplingGeometry(sosObservation);
            this.session = session;
            this.daos = daos;
            this.observationFactory = daos.observation().getObservationFactory();
            this.childObservation = childObservation;
            this.offerings = hOfferings;
            this.geometryHandler = geometryHandler;
            checkForDuplicity();
        }

        private void checkForDuplicity() throws OwsExceptionReport {
            /*
             * TODO check if observation exists in database for - series,
             * phenTimeStart, phenTimeEnd, resultTime - series, phenTimeStart,
             * phenTimeEnd, resultTime, depth/height parameter (same observation
             * different depth/height)
             */
            daos.observation.checkForDuplicatedObservations(sosObservation, observationConstellations.iterator().next(), session);

        }

        @Override
        public DataEntity<?> visit(BooleanValue value) throws OwsExceptionReport {
            return setUnitEntityAndPersist(observationFactory.truth(), value);
        }

        @Override
        public DataEntity<?> visit(CategoryValue value) throws OwsExceptionReport {
            return setUnitEntityAndPersist(observationFactory.category(), value);
        }

        @Override
        public DataEntity<?> visit(CountValue value) throws OwsExceptionReport {
            return setUnitEntityAndPersist(observationFactory.count(), value);
        }

        @Override
        public DataEntity<?> visit(GeometryValue value) throws OwsExceptionReport {
            return setUnitEntityAndPersist(observationFactory.geometry(), value);
        }

        @Override
        public DataEntity<?> visit(QuantityValue value) throws OwsExceptionReport {
            return setUnitEntityAndPersist(observationFactory.numeric(), value);
        }

        @Override
        public DataEntity<?> visit(QuantityRangeValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public DataEntity<?> visit(TextValue value)
                throws OwsExceptionReport {
            return setUnitEntityAndPersist(observationFactory.text(), value);
        }

        @Override
        public DataEntity<?> visit(UnknownValue value) throws OwsExceptionReport {
            return setUnitEntityAndPersist(observationFactory.blob(), value);
        }

        @Override
        public DataEntity<?> visit(SweDataArrayValue value) throws OwsExceptionReport {
            return persist(observationFactory.sweDataArray(), value.getValue().getXml());
        }

        @Override
        public DataEntity<?> visit(ComplexValue value) throws OwsExceptionReport {
            ComplexDataEntity complex = observationFactory.complex();
            complex.setParent(true);
            return persist(complex, persistChildren(value.getValue()));
        }

        @Override
        public DataEntity<?> visit(HrefAttributeValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public DataEntity<?> visit(NilTemplateValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public DataEntity<?> visit(ReferenceValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public DataEntity<?> visit(TVPValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public DataEntity<?> visit(TLVTValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public DataEntity<?> visit(CvDiscretePointCoverage value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public DataEntity<?> visit(MultiPointCoverage value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public DataEntity<?> visit(RectifiedGridCoverage value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public DataEntity<?> visit(ProfileValue value) throws OwsExceptionReport {
            ProfileDataEntity profile = observationFactory.profile();
            profile.setParent(true);
            sosObservation.getValue().setPhenomenonTime(value.getPhenomenonTime());
            return persist(profile, persistChildren(value.getValue()));
        }

        @Override
        public Collection<DataEntity<?>> visit(ProfileLevel value) throws OwsExceptionReport {
            List<DataEntity<?>> childObservations = new ArrayList<>();
            if (value.isSetValue()) {
                for (Value<?> v : value.getValue()) {
                    childObservations.add(v.accept(this));
                }
            }
            return childObservations;
        }

        @Override
        public DataEntity<?> visit(XmlValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        @Override
        public DataEntity<?> visit(TimeRangeValue value) throws OwsExceptionReport {
            throw notSupported(value);
        }

        private Set<DataEntity<?>> persistChildren(SweAbstractDataRecord dataRecord)
                throws HibernateException, OwsExceptionReport {
            Set<DataEntity<?>> children = new TreeSet<>();
            for (SweField field : dataRecord.getFields()) {
                PhenomenonEntity observableProperty = getObservablePropertyForField(field);
                ObservationPersister childPersister = createChildPersister(observableProperty);
                children.add(field.accept(ValueCreatingSweDataComponentVisitor.getInstance()).accept(childPersister));
            }
            session.flush();
            return children;
        }

        private Set<DataEntity<?>> persistChildren(List<ProfileLevel> values) throws OwsExceptionReport {
            Set<DataEntity<?>> children = new TreeSet<>();
            for (ProfileLevel level : values) {
                if (level.isSetValue()) {
                    for (Value<?> v : level.getValue()) {
                        if (v instanceof SweAbstractDataComponent
                                && ((SweAbstractDataComponent) v).isSetDefinition()) {
                            children.add(v.accept(
                                    createChildPersister(level, ((SweAbstractDataComponent) v).getDefinition())));
                        } else {
                            children.add(v.accept(createChildPersister(level)));
                        }
                    }
                }
            }
            session.flush();
            return children;
        }

        private OmObservation getObservationWithLevelParameter(ProfileLevel level) {
            OmObservation o = new OmObservation();
            sosObservation.copyTo(o);
            o.setParameter(level.getLevelStartEndAsParameter());
            if (level.isSetPhenomenonTime()) {
                o.setValue(new SingleObservationValue<>());
                o.getValue().setPhenomenonTime(level.getPhenomenonTime());
            }
            return o;
        }

        private ObservationPersister createChildPersister(ProfileLevel level, String observableProperty)
                throws OwsExceptionReport {
            return new ObservationPersister(geometryHandler, daos, caches, getObservationWithLevelParameter(level),
                    getObservationConstellation(getObservableProperty(observableProperty)), featureOfInterest,
                    getSamplingGeometryFromLevel(level), offerings, true, session);
        }

        private ObservationPersister createChildPersister(ProfileLevel level) throws OwsExceptionReport {
            return new ObservationPersister(geometryHandler, daos, caches, getObservationWithLevelParameter(level),
                    observationConstellations, featureOfInterest, getSamplingGeometryFromLevel(level), offerings,
                    true, session);

        }

        private ObservationPersister createChildPersister(PhenomenonEntity observableProperty)
                throws OwsExceptionReport {
            return new ObservationPersister(geometryHandler, daos, caches, sosObservation,
                    getObservationConstellation(observableProperty), featureOfInterest, samplingGeometry, offerings,
                    true, session);
        }

        private Set<ObservationConstellationEntity> getObservationConstellation(PhenomenonEntity observableProperty) {
            Set<ObservationConstellationEntity> newObservationConstellations = new HashSet<>(observationConstellations.size());
            for (ObservationConstellationEntity constellation : observationConstellations) {
                newObservationConstellations.add(daos.observationConstellation().checkOrInsert(
                        constellation.getProcedure(), observableProperty, constellation.getOffering(), true));
            }
            return newObservationConstellations;

        }

        private OwsExceptionReport notSupported(Value<?> value) throws OwsExceptionReport {
            throw new NoApplicableCodeException().withMessage("Unsupported observation value %s",
                    value.getClass().getCanonicalName());
        }

        private PhenomenonEntity getObservablePropertyForField(SweField field) {
            String definition = field.getElement().getDefinition();
            return getObservableProperty(definition);
        }

        private PhenomenonEntity getObservableProperty(String observableProperty) {
            return daos.observableProperty().get(observableProperty);
        }

        private <V, T extends DataEntity<V>> T setUnitEntityAndPersist(T observation, Value<V> value)
                throws OwsExceptionReport {
            return persist(observation, value.getValue());
        }

        private UnitEntity getUnit(Value<?> value) {
            return value.isSetUnit() ? daos.observation.getUnit(value.getUnitObject(), caches.units())
                    : null;
        }

        private <V, T extends DataEntity<V>> T persist(T observation, V value) throws OwsExceptionReport {
            observation.setDeleted(false);

            if (!childObservation) {
                daos.observation().addIdentifier(sosObservation, observation);
            } else {
                observation.setChild(true);
            }

            daos.observation().addName(sosObservation, observation, session);
            daos.observation().addDescription(sosObservation, observation);
            daos.observation().addTime(sosObservation, observation);
            observation.setValue(value);
            observation.setGeometryEntity(new GeometryEntity().setGeometry(samplingGeometry).setSrid(samplingGeometry.getSRID()));
            checkUpdateFeatureOfInterestGeometry();

            ObservationContext observationContext = daos.observation().createObservationContext();

            String observationType = observation.accept(ObservationTypeObservationVisitor.getInstance());

                for (ObservationConstellationEntity oc : observationConstellations) {
                    if (!isProfileObservation(oc) || (isProfileObservation(oc) && !childObservation)) {
                    offerings.add(oc.getOffering());
                    if (!daos.observationConstellation().checkObservationType(oc, observationType)) {
                        throw new InvalidParameterValueException().withMessage(
                                "The requested observationType (%s) is invalid for procedure = %s, observedProperty = %s and offering = %s! The valid observationType is '%s'!",
                                observationType, oc.getProcedure().getIdentifier(),
                                oc.getObservableProperty().getIdentifier(), oc.getOffering().getIdentifier(),
                                oc.getObservationType().getType());
                    }
                    }
                }

            if (sosObservation.isSetSeriesType()) {
                observationContext.setDatasetType(sosObservation.getSeriesType());
            } else {
                observationContext.setDatasetType(observation.accept(SERIES_TYPE_VISITOR));
            }

            ObservationConstellationEntity first = Iterables.getFirst(observationConstellations, null);
            if (first != null) {
                observationContext.setPhenomenon(first.getObservableProperty());
                observationContext.setProcedure(first.getProcedure());
                observationContext.setOffering(first.getOffering());
            }

            if (childObservation) {
                observationContext.setHiddenChild(true);
            }
            observationContext.setFeatureOfInterest(featureOfInterest);
            daos.observation().fillObservationContext(observationContext, sosObservation, session);
            daos.observation().addObservationContextToObservation(observationContext, observation, session);

            session.saveOrUpdate(observation);

            if (sosObservation.isSetParameter()) {
                daos.parameter().insertParameter(sosObservation.getParameter(), observation.getObservationId(),
                        caches.units, session);
            }
            return observation;
        }

        private boolean isProfileObservation(ObservationConstellationEntity observationConstellation) {
            return observationConstellation.isSetObservationType() && (OmConstants.OBS_TYPE_PROFILE_OBSERVATION
                    .equals(observationConstellation.getObservationType().getType())
                    || GWMLConstants.OBS_TYPE_GEOLOGY_LOG
                            .equals(observationConstellation.getObservationType().getType())
                    || GWMLConstants.OBS_TYPE_GEOLOGY_LOG_COVERAGE
                            .equals(observationConstellation.getObservationType().getType()));
        }

        private Geometry getSamplingGeometryFromLevel(ProfileLevel level) throws OwsExceptionReport {
            if (level.isSetLocation()) {
                return geometryHandler.switchCoordinateAxisFromToDatasourceIfNeeded(level.getLocation());
            }
            return null;
        }

        private Geometry getSamplingGeometry(OmObservation sosObservation) throws OwsExceptionReport {
            if (!sosObservation.isSetSpatialFilteringProfileParameter()) {
                return null;
            }
            if (sosObservation.isSetValue() && sosObservation.getValue().isSetValue()
                    && sosObservation.getValue().getValue() instanceof ProfileValue
                    && ((ProfileValue) sosObservation.getValue().getValue()).isSetGeometry()) {
                return geometryHandler.switchCoordinateAxisFromToDatasourceIfNeeded(
                        ((ProfileValue) sosObservation.getValue().getValue()).getGeometry());
            }
            NamedValue<Geometry> spatialFilteringProfileParameter =
                    sosObservation.getSpatialFilteringProfileParameter();
            Geometry geometry = spatialFilteringProfileParameter.getValue().getValue();
            return geometryHandler.switchCoordinateAxisFromToDatasourceIfNeeded(geometry);
        }

        private void checkUpdateFeatureOfInterestGeometry() {
            // check if flag is set and if this observation is not a child
            // observation
            if (samplingGeometry != null && isUpdateFeatureGeometry() && !childObservation) {
                daos.feature.updateFeatureOfInterestGeometry(featureOfInterest, samplingGeometry, session);
            }
        }

        private boolean isUpdateFeatureGeometry() {
            // TODO
            return true;
        }

        private static class Caches {
            private final Map<String, CodespaceEntity> codespaces;

            private final Map<UoM, UnitEntity> units;

            Caches(Map<String, CodespaceEntity> codespaces, Map<UoM, UnitEntity> units) {
                this.codespaces = codespaces;
                this.units = units;
            }

            public Map<String, CodespaceEntity> codespaces() {
                return codespaces;
            }

            public Map<UoM, UnitEntity> units() {
                return units;
            }
        }

        private static class DAOs {
            private final PhenomenonDao observableProperty;

            private final ObservationConstellationDao observationConstellation;

            private final AbstractObservationDao observation;

            private final ObservationTypeDao observationType;

            private final ParameterDAO parameter;

            private final FeatureDao feature;
            
            DAOs(AbstractObservationDao observationDao, DaoFactory daoFactory, Session session) {
                this.observation = observationDao;
                this.observableProperty = daoFactory.getObservablePropertyDao(session);
                this.observationConstellation = daoFactory.getObservationConstellationDao(session);
                this.observationType = daoFactory.getObservationTypeDao(session);
                this.parameter = daoFactory.getParameterDao(session);
                this.feature = daoFactory.getFeatureOfInterestDao(session);
            }

            public PhenomenonDao observableProperty() {
                return this.observableProperty;
            }

            public ObservationConstellationDao observationConstellation() {
                return this.observationConstellation;
            }

            public AbstractObservationDao observation() {
                return this.observation;
            }

            public ObservationTypeDao observationType() {
                return this.observationType;
            }

            public ParameterDAO parameter() {
                return this.parameter;
            }

            public FeatureDao feature() {
                return this.feature;
            }
        }

        private static class SeriesTypeVisitor
                implements ObservationVisitor<String> {

            @Override
            public String visit(QuantityDatasetEntity o) throws OwsExceptionReport {
                return "quantity";
            }

            @Override
            public String visit(BlobDatasetEntity o) throws OwsExceptionReport {
                return "blob";
            }

            @Override
            public String visit(BooleanDatasetEntity o) throws OwsExceptionReport {
                return "boolean";
            }

            @Override
            public String visit(CategoryDatasetEntity o) throws OwsExceptionReport {
                return "category";
            }

            @Override
            public String visit(ComplexDatasetEntity o) throws OwsExceptionReport {
                return "complex";
            }

            @Override
            public String visit(CountDatasetEntity o) throws OwsExceptionReport {
                return "count";
            }

            @Override
            public String visit(GeometryDatasetEntity o) throws OwsExceptionReport {
                return "geometry";
            }

            @Override
            public String visit(TextDatasetEntity o) throws OwsExceptionReport {
                return "text";
            }

            @Override
            public String visit(SweDataArrayDatasetEntity o) throws OwsExceptionReport {
                return "swedataarray";
            }

            @Override
            public String visit(ProfileDatasetEntity o) throws OwsExceptionReport {
                if (o.isSetValue()) {
                    for (DataEntity<?> value : o.getValue()) {
                        return value.accept(this) + "-profile";
                    }
                }
                return "profile";
            }
        }
    }

    /**
     * Observation time extrema {@link ResultTransformer}
     *
     * @author <a href="mailto:c.hollmann@52north.org">Carsten Hollmann</a>
     * @since 4.4.0
     *
     */
    protected static class ObservationTimeTransformer
            implements ResultTransformer {

        private static final long serialVersionUID = -3401483077212678275L;

        @Override
        public TimeExtrema transformTuple(Object[] tuple, String[] aliases) {
            TimeExtrema timeExtrema = new TimeExtrema();
            if (tuple != null) {
                timeExtrema.setMinPhenomenonTime(DateTimeHelper.makeDateTime(tuple[0]));
                timeExtrema.setMaxPhenomenonTime(DateTimeHelper.makeDateTime(tuple[1]));
                timeExtrema.setMinResultTime(DateTimeHelper.makeDateTime(tuple[2]));
                timeExtrema.setMaxResultTime(DateTimeHelper.makeDateTime(tuple[3]));
            }
            return timeExtrema;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public List transformList(List collection) {
            return collection;
        }
    }

    public class MinMaxLatLon {
        private Double minLat;

        private Double maxLat;

        private Double minLon;

        private Double maxLon;

        public MinMaxLatLon(Object[] result) {
            setMinLat(JavaHelper.asDouble(result[0]));
            setMinLon(JavaHelper.asDouble(result[1]));
            setMaxLat(JavaHelper.asDouble(result[2]));
            setMaxLon(JavaHelper.asDouble(result[3]));
        }

        /**
         * @return the minLat
         */
        public Double getMinLat() {
            return minLat;
        }

        /**
         * @param minLat
         *            the minLat to set
         */
        public void setMinLat(Double minLat) {
            this.minLat = minLat;
        }

        /**
         * @return the maxLat
         */
        public Double getMaxLat() {
            return maxLat;
        }

        /**
         * @param maxLat
         *            the maxLat to set
         */
        public void setMaxLat(Double maxLat) {
            this.maxLat = maxLat;
        }

        /**
         * @return the minLon
         */
        public Double getMinLon() {
            return minLon;
        }

        /**
         * @param minLon
         *            the minLon to set
         */
        public void setMinLon(Double minLon) {
            this.minLon = minLon;
        }

        /**
         * @return the maxLon
         */
        public Double getMaxLon() {
            return maxLon;
        }

        /**
         * @param maxLon
         *            the maxLon to set
         */
        public void setMaxLon(Double maxLon) {
            this.maxLon = maxLon;
        }
    }

}
