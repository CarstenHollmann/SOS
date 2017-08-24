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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.hibernate.criterion.Subqueries;
import org.hibernate.sql.JoinType;
import org.n52.series.db.DataAccessException;
import org.n52.series.db.DataModelUtil;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.DescribableEntity;
import org.n52.series.db.beans.ObservationConstellationEntity;
import org.n52.series.db.beans.ObservationTypeEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProcedureEntity;
import org.n52.series.db.beans.dataset.Dataset;
import org.n52.series.db.dao.AbstractDao;
import org.n52.series.db.dao.DbQuery;
import org.n52.series.db.dao.OfferingDao;
import org.n52.series.db.dao.PhenomenonDao;
import org.n52.series.db.dao.ProcedureDao;
import org.n52.shetland.ogc.om.AbstractPhenomenon;
import org.n52.shetland.ogc.om.OmCompositePhenomenon;
import org.n52.shetland.ogc.om.OmObservableProperty;
import org.n52.shetland.ogc.om.OmObservationConstellation;
import org.n52.shetland.ogc.ows.exception.InvalidParameterValueException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.Sos2Constants;
import org.n52.shetland.util.CollectionHelper;
import org.n52.sw.suite.db.util.ObservationConstellationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ObservationConstellationDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObservationConstellationDao.class);

    private final Session session;

    public ObservationConstellationDao(Session session) {
        this.session = session;
    }

    protected Session getSession() {
        return session;
    }

    /**
     * Get all observation constellation objects
     *
     * @param session
     *            Hibernate session
     * @return Observation constellation objects
     */
    @SuppressWarnings("unchecked")
    public List<ObservationConstellationEntity> get(Session session) {
        Criteria criteria = getNotDeletedCriteria();
        LOGGER.debug("QUERY getObservationConstellations(): {}", DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get observation constellation objects for procedure and observable
     * property object and offering identifiers
     *
     * @param procedure
     *            Procedure object
     * @param observableProperty
     *            Observable property object
     * @param offerings
     *            Offering identifiers
     * @param session
     *            Hibernate session
     * @return Observation constellation objects
     */
    @SuppressWarnings("unchecked")
    public List<ObservationConstellationEntity> get(ProcedureEntity procedure, PhenomenonEntity observableProperty,
            Collection<String> offerings) {
        Criteria criteria = getNotDeletedCriteria();
        add(criteria, procedure);
        add(criteria, observableProperty);
        addOnIdentifier(criteria, "o", ObservationConstellationEntity.OFFERING, offerings);
        LOGGER.debug("QUERY get(procedure, observableProperty, offerings(String)): {}",
                DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get ObservationConstellations for procedure, observableProperty and
     * offerings
     *
     * @param procedure
     *            Procedure to get ObservaitonConstellation for
     * @param observableProperty
     *            observableProperty to get ObservaitonConstellation for
     * @param offerings
     *            Offerings to get ObservaitonConstellation for
     * @param session
     *            Hibernate session
     * @return ObservationConstellations
     */
    @SuppressWarnings("unchecked")
    public List<ObservationConstellationEntity> getForOfferings(ProcedureEntity procedure,
            PhenomenonEntity observableProperty, Collection<OfferingEntity> offerings) {
        Criteria criteria = getNotDeletedCriteria();
        add(criteria, procedure);
        add(criteria, observableProperty);
        addOfferings(criteria, offerings);
        LOGGER.debug("QUERY getForOfferings(procedure, observableProperty, offerings): {}",
                DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get observation constellation objects for procedure and observable
     * property object and offering identifier
     *
     * @param procedure
     *            Procedure object
     * @param observableProperty
     *            Observable property object
     * @param offering
     *            Offering identifier
     * @param session
     *            Hibernate session
     * @return Observation constellation objects
     */
    public ObservationConstellationEntity get(ProcedureEntity procedure, PhenomenonEntity observableProperty,
            OfferingEntity offering) {
        Criteria criteria = getNotDeletedCriteria();
        add(criteria, procedure);
        add(criteria, observableProperty);
        add(criteria, offering);
        LOGGER.debug("QUERY get(procedure, observableProperty, offering): {}",
                DataModelUtil.getSqlString(criteria));
        return (ObservationConstellationEntity) criteria.uniqueResult();
    }

    /**
     * Get ObservationConstellations for observableProperty and offerings
     *
     * @param observableProperty
     *            observableProperty to get ObservaitonConstellation for
     * @param offerings
     *            Offerings to get ObservaitonConstellation for
     * @param session
     *            Hibernate session
     * @return ObservationConstellations
     */
    @SuppressWarnings("unchecked")
    public List<ObservationConstellationEntity> get(PhenomenonEntity observableProperty,
            Collection<OfferingEntity> offerings) {
        Criteria criteria = getNotDeletedCriteria();
        addOfferings(criteria, offerings);
        add(criteria, observableProperty);
        LOGGER.debug("QUERY get(observableProperty, offerings): {}", DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get ObservationConstellations for procedure and observableProperty
     *
     * @param procedure
     *            Procedure to get ObservaitonConstellation for
     * @param observableProperty
     *            observableProperty to get ObservaitonConstellation for
     * @param session
     *            Hibernate session
     * @return ObservationConstellations
     */
    @SuppressWarnings("unchecked")
    public List<ObservationConstellationEntity> get(String procedure, String observableProperty, Session session) {
        Criteria criteria = getNotDeletedCriteria();
        addOnIdentifier(criteria, ObservationConstellationEntity.PROCEDURE, procedure);
        addOnIdentifier(criteria, ObservationConstellationEntity.OBSERVABLE_PROPERTY, observableProperty);
        LOGGER.debug("QUERY get(procedure, observableProperty): {}",
                DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Return the non-deleted observation constellations for an offering
     *
     * @param offering
     *            Offering to fetch observation constellations for
     * @param session
     *            Session to use
     * @return Offering's observation constellations
     */
    @SuppressWarnings("unchecked")
    public List<ObservationConstellationEntity> get(OfferingEntity offering) {
        Criteria criteria = getNotDeletedCriteria();
        add(criteria, offering);
        LOGGER.debug("QUERY get(offering): {}",
                DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get ObservationCollection entities for procedures, observableProperties
     * and offerings where observationType is not null;
     *
     * @param procedures
     *            Procedures to get ObservationCollection entities for
     * @param observedProperties
     *            ObservableProperties to get ObservationCollection entities for
     * @param offerings
     *            Offerings to get ObservationCollection entities for
     * @param session
     *            Hibernate session
     * @return Resulting ObservationCollection entities
     */
    @SuppressWarnings("unchecked")
    public List<ObservationConstellationEntity> getNotNullTypes(Collection<String> procedures,
            Collection<String> observedProperties, Collection<String> offerings) {
        final Criteria criteria = getNotDeletedCriteria();
        addOnIdentifier(criteria, ObservationConstellationEntity.OFFERING, offerings);
        addOnIdentifier(criteria, ObservationConstellationEntity.OBSERVABLE_PROPERTY, observedProperties);
        addOnIdentifier(criteria, ObservationConstellationEntity.PROCEDURE, procedures);
        criteria.add(Restrictions.isNotNull(ObservationConstellationEntity.OBSERVATION_TYPE));
        LOGGER.debug("QUERY getNotNullTypes(procedure): {}", DataModelUtil.getSqlString(criteria));
        return criteria.list();

    }

    @SuppressWarnings("unchecked")
    protected Set<ObservationConstellationEntity> get(ProcedureEntity procedure) {
        Criteria criteria = getNotDeletedCriteria();
        add(criteria, procedure);
        LOGGER.debug("QUERY get(): {}", DataModelUtil.getSqlString(criteria));
        return Sets.newHashSet(criteria.list());
    }

    public ObservationConstellationEntity get(OmObservationConstellation omObsConst) {
        Criteria criteria = getNotDeletedCriteria();
        addOnIdentifier(criteria, ObservationConstellationEntity.PROCEDURE, omObsConst.getProcedureIdentifier());
        addOnIdentifier(criteria, ObservationConstellationEntity.OBSERVABLE_PROPERTY, omObsConst.getObservablePropertyIdentifier());
        addOnIdentifier(criteria, "o", ObservationConstellationEntity.OFFERING, omObsConst.getOfferings());
        LOGGER.debug("QUERY get(omObservationConstellation): {}",
                DataModelUtil.getSqlString(criteria));
        return (ObservationConstellationEntity) criteria.uniqueResult();
    }

    /**
     * Get info for all observation constellation objects
     *
     * @param session
     *            Hibernate session
     * @return Observation constellation info objects
     */
    public List<ObservationConstellationInfo> getObservationConstellationInfo(Session session) {
        List<ObservationConstellationInfo> ocis = Lists.newArrayList();
        Criteria criteria = getNotDeletedAndPublishedCriteria("oc")
                .createAlias(ObservationConstellationEntity.OFFERING, "o")
                .createAlias(ObservationConstellationEntity.PROCEDURE, "p")
                .createAlias(ObservationConstellationEntity.OBSERVABLE_PROPERTY, "op")
                .add(Restrictions.eq("op." + PhenomenonEntity.PROPERTY_HIDDEN_CHILD, false))
                .createAlias(ObservationConstellationEntity.OBSERVATION_TYPE, "ot", JoinType.LEFT_OUTER_JOIN)
                .setProjection(Projections.projectionList()
                        .add(Projections.property("o." + OfferingEntity.PROPERTY_DOMAIN_ID))
                        .add(Projections.property("p." + ProcedureEntity.PROPERTY_DOMAIN_ID))
                        .add(Projections.property("op." + PhenomenonEntity.PROPERTY_DOMAIN_ID))
                        .add(Projections.property("ot." + ObservationTypeEntity.TYPE))
                        .add(Projections.property("oc." + ObservationConstellationEntity.HIDDEN_CHILD)));
        LOGGER.debug("QUERY getObservationConstellationInfo(): {}", DataModelUtil.getSqlString(criteria));

        @SuppressWarnings("unchecked")
        List<Object[]> results = criteria.list();
        for (Object[] result : results) {
            ObservationConstellationInfo oci = new ObservationConstellationInfo();
            oci.setOffering((String) result[0]);
            oci.setProcedure((String) result[1]);
            oci.setObservableProperty((String) result[2]);
            oci.setObservationType((String) result[3]);
            oci.setHiddenChild((Boolean) result[4]);
            ocis.add(oci);
        }
        return ocis;
    }

    /**
     * Get first ObservationConstellation for procedure, observableProperty and
     * offerings
     *
     * @param p
     *            Procedure to get ObservaitonConstellation for
     * @param op
     *            ObservedProperty to get ObservaitonConstellation for
     * @param o
     *            Offerings to get ObservaitonConstellation for
     * @param session
     *            Hibernate session
     * @return First ObservationConstellation
     */
    public ObservationConstellationEntity getFirst(ProcedureEntity p, PhenomenonEntity op,
            Collection<OfferingEntity> o) {
        final List<ObservationConstellationEntity> oc = getForOfferings(p, op, o);
        return oc.isEmpty() ? null : oc.get(0);
    }

    /**
     * Insert or update and get observation constellation for procedure,
     * observable property and offering
     *
     * @param procedure
     *            Procedure object
     * @param observableProperty
     *            Observable property object
     * @param offering
     *            Offering object
     * @param hiddenChild
     *            Is observation constellation hidden child
     * @param session
     *            Hibernate session
     * @return Observation constellation object
     */
    public ObservationConstellationEntity checkOrInsert(ProcedureEntity procedure, PhenomenonEntity observableProperty,
            OfferingEntity offering, boolean hiddenChild) {
        Criteria criteria = getDefaultCriteria();
                add(criteria, offering);
                add(criteria, observableProperty);
                add(criteria, procedure);
        LOGGER.debug(
                "QUERY checkOrInsert(procedure, observableProperty, offering, hiddenChild): {}",
                DataModelUtil.getSqlString(criteria));
        ObservationConstellationEntity obsConst = (ObservationConstellationEntity) criteria.uniqueResult();
        if (obsConst == null) {
            obsConst = new ObservationConstellationEntity();
            obsConst.setObservableProperty(observableProperty);
            obsConst.setProcedure(procedure);
            obsConst.setOffering(offering);
            obsConst.setDisabled(false);
            obsConst.setHiddenChild(hiddenChild);
            getSession().save(obsConst);
            getSession().flush();
            getSession().refresh(obsConst);
        } else if (obsConst.getDisabled()) {
            obsConst.setDisabled(false);
            getSession().update(obsConst);
            getSession().flush();
            getSession().refresh(obsConst);
        }
        return obsConst;
    }

    /**
     * Check and Update and/or get observation constellation objects
     *
     * @param sosOC
     *            SOS observation constellation
     * @param offering
     *            Offering identifier
     * @param session
     *            Hibernate session
     * @param parameterName
     *            Parameter name for exception
     * @return Observation constellation object
     * @throws OwsExceptionReport
     *             If the requested observation type is invalid
     */
    public ObservationConstellationEntity check(OmObservationConstellation sosOC, String offering, Session session,
            String parameterName)
            throws OwsExceptionReport {
        AbstractPhenomenon observableProperty = sosOC.getObservableProperty();
        String observablePropertyIdentifier = observableProperty.getIdentifier();

        Criteria criteria = getDefaultCriteria();
        addOnIdentifier(criteria, ObservationConstellationEntity.OFFERING, offering);
        addOnIdentifier(criteria, ObservationConstellationEntity.OBSERVABLE_PROPERTY, observablePropertyIdentifier);
        addOnIdentifier(criteria, ObservationConstellationEntity.PROCEDURE, sosOC.getProcedureIdentifier());

        LOGGER.debug("QUERY check(sosObservationConstellation, offering): {}",
                DataModelUtil.getSqlString(criteria));
        List<ObservationConstellationEntity> hocs = criteria.list();

        if (hocs == null || hocs.isEmpty()) {
            throw new InvalidParameterValueException().at(Sos2Constants.InsertObservationParams.observation)
                    .withMessage(
                            "The requested observation constellation (procedure=%s, observedProperty=%s and offering=%s) is invalid!",
                            sosOC.getProcedureIdentifier(), observablePropertyIdentifier, sosOC.getOfferings());
        }
        String observationType = sosOC.getObservationType();

        ObservationConstellationEntity hObsConst = null;
        for (ObservationConstellationEntity hoc : hocs) {
            if (!checkObservationType(hoc, observationType)) {
                throw new InvalidParameterValueException().at(parameterName).withMessage(
                        "The requested observationType (%s) is invalid for procedure = %s, observedProperty = %s and offering = %s! The valid observationType is '%s'!",
                        observationType, sosOC.getProcedureIdentifier(), observablePropertyIdentifier,
                        sosOC.getOfferings(), hoc.getObservationType().getType());
            }
            if (hObsConst == null) {
                if (sosOC.isSetProcedure()) {
                    if (hoc.getProcedure().getIdentifier().equals(sosOC.getProcedureIdentifier())) {
                        hObsConst = hoc;
                    }
                } else {
                    hObsConst = hoc;
                }
            }

            // add parent/childs
            if (observableProperty instanceof OmCompositePhenomenon) {
                OmCompositePhenomenon omCompositePhenomenon = (OmCompositePhenomenon) observableProperty;
                PhenomenonDao dao = new PhenomenonDao(getSession());
                Map<String, PhenomenonEntity> obsprop =
                        dao.getOrInsertAsMap(Arrays.asList(observableProperty), false, session);
                for (OmObservableProperty child : omCompositePhenomenon) {
                    checkOrInsert(hoc.getProcedure(), obsprop.get(child.getIdentifier()),
                            hoc.getOffering(), true);
                }
            }
        }
        return hObsConst;
    }

    public boolean checkObservationType(ObservationConstellationEntity hoc, String observationType) {
        String hObservationType = hoc.getObservationType() == null ? null : hoc.getObservationType().getType();
        if (hObservationType == null || hObservationType.isEmpty() || hObservationType.equals("NOT_DEFINED")) {
            update(hoc, observationType);
        } else if (!hObservationType.equals(observationType)) {
            return false;
        }
        return true;
    }

    /**
     * Update observation constellation with observation type
     *
     * @param observationConstellation
     *            Observation constellation object
     * @param observationType
     *            Observation type
     * @param session
     *            Hibernate session
     */
    @SuppressWarnings("unchecked")
    public void update(ObservationConstellationEntity observationConstellation, String observationType) {
        ObservationTypeEntity obsType = new ObservationTypeDao(getSession()).get(observationType);
        observationConstellation.setObservationType(obsType);
        getSession().saveOrUpdate(observationConstellation);

        // update hidden child observation constellations
        // TODO should hidden child observation constellations be restricted to
        // the parent observation type?
        Set<String> offerings = getOfferings(get(observationConstellation.getProcedure()));
        offerings.remove(observationConstellation.getOffering().getIdentifier());

        if (CollectionHelper.isNotEmpty(offerings)) {
            Criteria c = getDefaultCriteria().setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY)
                    .add(Restrictions.eq(ObservationConstellationEntity.OBSERVABLE_PROPERTY,
                            observationConstellation.getObservableProperty()))
                    .add(Restrictions.eq(ObservationConstellationEntity.PROCEDURE,
                            observationConstellation.getProcedure()))
                    .add(Restrictions.eq(ObservationConstellationEntity.HIDDEN_CHILD, true));
            c.createCriteria(ObservationConstellationEntity.OFFERING)
                    .add(Restrictions.in(OfferingEntity.PROPERTY_DOMAIN_ID, offerings));
            LOGGER.debug("QUERY updateObservationConstellation(observationConstellation, observationType): {}",
                    DataModelUtil.getSqlString(c));
            List<ObservationConstellationEntity> hiddenChildObsConsts = c.list();
            for (ObservationConstellationEntity hiddenChildObsConst : hiddenChildObsConsts) {
                hiddenChildObsConst.setObservationType(obsType);
                getSession().saveOrUpdate(hiddenChildObsConst);
            }
        }
    }

    private Set<String> getOfferings(Set<ObservationConstellationEntity> oce) {
        return oce.stream().map(o -> o.getOffering().getDomainId()).collect(Collectors.toSet());
    }

    protected Criteria add(Criteria criteria, ProcedureEntity procedure) {
        if (procedure != null) {
            return criteria.add(Restrictions.eq(ObservationConstellationEntity.PROCEDURE, procedure));
        }
        return criteria;
    }

    protected Criteria addProcedures(Criteria criteria, Collection<ProcedureEntity> procedures) {
        return add(criteria, ObservationConstellationEntity.PROCEDURE, procedures);
    }

    protected Criteria add(Criteria criteria, OfferingEntity offering) {
        if (offering != null) {
            return criteria.add(Restrictions.eq(ObservationConstellationEntity.OFFERING, offering));
        }
        return criteria;
    }

    protected Criteria addOfferings(Criteria criteria, Collection<OfferingEntity> offerings) {
        return add(criteria, ObservationConstellationEntity.OFFERING, offerings);
    }

    protected Criteria add(Criteria criteria, PhenomenonEntity observableProperty) {
        if (observableProperty != null) {
            return criteria.add(Restrictions.eq(ObservationConstellationEntity.OBSERVABLE_PROPERTY, observableProperty));
        }
        return criteria;
    }

    protected Criteria addObservablePropertyies(Criteria criteria, Collection<PhenomenonEntity> observableProperties) {
        return add(criteria, ObservationConstellationEntity.OBSERVABLE_PROPERTY, observableProperties);
    }

    protected Criteria add(Criteria criteria, String propteryName, Collection<?> values) {
        if (CollectionHelper.isNotEmpty(values)) {
            return criteria.add(Restrictions.in(propteryName, values));
        }
        return criteria;
    }

    protected Criteria addOnIdentifier(Criteria criteria, String alias, String propertyName,
            Collection<String> values) {
        if (CollectionHelper.isNotEmpty(values)) {
            return criteria.createAlias(propertyName, alias)
                    .add(Restrictions.in(alias + "." + DescribableEntity.PROPERTY_DOMAIN_ID, values));
        }
        return criteria;
    }

    protected Criteria addOnIdentifier(Criteria criteria, String propertyName, String value) {
        if (!Strings.isNullOrEmpty(value)) {
            criteria.createCriteria(propertyName).add(Restrictions.eq(DescribableEntity.PROPERTY_DOMAIN_ID, value));
        }
        return criteria;
    }

    protected Criteria addOnIdentifier(Criteria criteria, String propertyName, Collection<String> values) {
        if (CollectionHelper.isNotEmpty(values)) {
            return criteria.createCriteria(propertyName).add(Restrictions.in(DescribableEntity.PROPERTY_DOMAIN_ID, values));
        }
        return criteria;
    }

    protected Criteria getDefaultCriteria() {
        return getDefaultCriteria(null);
    }

    protected Criteria getDefaultCriteria(String alias) {
        return Strings.isNullOrEmpty(alias)
                ? getSession().createCriteria(ObservationConstellationEntity.class)
                        .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY)
                : getSession().createCriteria(ObservationConstellationEntity.class, alias)
                        .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    protected Criteria getNotDeletedCriteria() {
        return getNotDeletedAndPublishedCriteria(null);
    }

    protected Criteria getNotDeletedAndPublishedCriteria(String alias) {
        return getSession().createCriteria(ObservationConstellationEntity.class, alias)
                .add(Subqueries.propertyIn(ObservationConstellationEntity.PROPERTY_ID, getNotDeletedAndPublished()));
    }

    private DetachedCriteria getNotDeletedAndPublished() {
        final DetachedCriteria detachedCriteria = DetachedCriteria.forClass(DatasetEntity.class);
        detachedCriteria.add(Restrictions.eq(DatasetEntity.PROPERTY_DELETED, false))
                .add(Restrictions.eq(DatasetEntity.PROPERTY_PUBLISHED, true));
        detachedCriteria.setProjection(
                Projections.distinct(Projections.property(DatasetEntity.PROPERTY_OBSERVATION_CONSTELLATION)));
        return detachedCriteria;
    }
}
