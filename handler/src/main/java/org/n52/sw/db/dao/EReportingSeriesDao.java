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
import org.hibernate.criterion.Restrictions;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.ereporting.EReportingAssessmentTypeEntity;
import org.n52.series.db.beans.ereporting.EReportingDatasetEntity;
import org.n52.series.db.beans.ereporting.EReportingSamplingPointEntity;
import org.n52.shetland.aqd.AqdConstants;
import org.n52.shetland.aqd.ReportObligationType;
import org.n52.shetland.aqd.ReportObligations;
import org.n52.shetland.ogc.ows.exception.OptionNotSupportedException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.request.GetObservationByIdRequest;
import org.n52.shetland.ogc.sos.request.GetObservationRequest;
import org.n52.shetland.util.CollectionHelper;
import org.n52.sw.db.util.QueryHelper;

public class EReportingSeriesDao extends AbstractSeriesDao {

    public EReportingSeriesDao(DaoFactory daoFactory, Session session, boolean includeChildObservableProperties) {
        super(daoFactory, session, includeChildObservableProperties);
    }

    @Override
    public Class<?> getSeriesClass() {
        return EReportingDatasetEntity.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<DatasetEntity> get(GetObservationRequest request, Collection<String> features) throws OwsExceptionReport {
        return getCriteria(request, features).list();
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<DatasetEntity> get(GetObservationByIdRequest request) throws OwsExceptionReport {
        return getCriteria(request).list();
    }

    @Override
    public DatasetEntity get(String procedure, String observableProperty, String featureOfInterest) {
        return (DatasetEntity) getCriteriaFor(procedure, observableProperty, featureOfInterest).uniqueResult();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<DatasetEntity> get(String observedProperty, Collection<String> features) {
        if (CollectionHelper.isNotEmpty(features)) {
            List<DatasetEntity> series = new ArrayList<>();
            for (List<String> ids : QueryHelper.getListsForIdentifiers(features)) {
                series.addAll(getCriteria(observedProperty, ids).list());
            }
            return series;
        } else {
            return getCriteria(observedProperty, features).list();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<DatasetEntity> get(String procedure, String observedProperty, String offering, Collection<String> features) {
        if (CollectionHelper.isNotEmpty(features)) {
            List<DatasetEntity> series = new ArrayList<>();
            for (List<String> ids : QueryHelper.getListsForIdentifiers(features)) {
                series.addAll(getCriteria(procedure, observedProperty, offering, ids).list());
            }
            return series;
        } else {
            return getCriteria(procedure, observedProperty, offering, features).list();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<DatasetEntity> get(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features) {
        if (CollectionHelper.isNotEmpty(features)) {
            List<DatasetEntity> series = new ArrayList<>();
            for (List<String> ids : QueryHelper.getListsForIdentifiers(features)) {
                series.addAll(getCriteria(procedures, observedProperties, ids).list());
            }
            return series;
        } else {
            return getCriteria(procedures, observedProperties, features).list();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<DatasetEntity> get(Collection<String> procedures, Collection<String> observedProperties,
            Collection<String> features, Collection<String> offerings) {
        if (CollectionHelper.isNotEmpty(features)) {
            List<DatasetEntity> series = new ArrayList<>();
            for (List<String> ids : QueryHelper.getListsForIdentifiers(features)) {
                series.addAll(getCriteria(procedures, observedProperties, ids, offerings).list());
            }
            return series;
        } else {
            return getCriteria(procedures, observedProperties, features, offerings).list();
        }
    }

    @Override
    public EReportingDatasetEntity getOrInsert(ObservationContext identifiers) throws OwsExceptionReport {
        return (EReportingDatasetEntity) super.getOrInsert(identifiers);
    }

    /**
     * Add EReportingSamplingPoint restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param samplingPoint
     *            EReportingSamplingPoint identifier to add
     */
    public void addEReportingSamplingPointToCriteria(Criteria c, String samplingPoint) {
        c.createCriteria(EReportingDatasetEntity.SAMPLING_POINT).add(Restrictions.eq(EReportingDatasetEntity.PROPERTY_DOMAIN_ID, samplingPoint));

    }

    /**
     * Add EReportingSamplingPoint restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param samplingPoint
     *            EReportingSamplingPoint to add
     */
    public void addEReportingSamplingPointToCriteria(Criteria c, EReportingSamplingPointEntity samplingPoint) {
        c.add(Restrictions.eq(EReportingDatasetEntity.SAMPLING_POINT, samplingPoint));
    }

    /**
     * Add EReportingSamplingPoint restriction to Hibernate Criteria
     *
     * @param c
     *            Hibernate Criteria to add restriction
     * @param samplingPoints
     *            EReportingSamplingPoint identifiers to add
     */
    public void addEReportingSamplingPointToCriteria(Criteria c, Collection<String> samplingPoints) {
        c.createCriteria(EReportingDatasetEntity.SAMPLING_POINT).add(Restrictions.in(EReportingSamplingPointEntity.PROPERTY_DOMAIN_ID, samplingPoints));
    }

    @Override
    protected void addSpecificRestrictions(Criteria c, GetObservationRequest request) throws OwsExceptionReport {
        if (request.isSetResponseFormat() && AqdConstants.NS_AQD.equals(request.getResponseFormat())) {
            ReportObligationType flow = ReportObligations.getFlow(request.getExtensions());
            if (null == flow) {
                throw new OptionNotSupportedException().withMessage("The requested e-Reporting flow %s is not supported!", flow.name());
            } else {
                switch (flow) {
                    case E1A:
                    case E2A:
                        addAssessmentType(c, AqdConstants.AssessmentType.Fixed.name());
                        break;
                    case E1B:
                        addAssessmentType(c, AqdConstants.AssessmentType.Model.name());
                        break;
                    default:
                        throw new OptionNotSupportedException().withMessage("The requested e-Reporting flow %s is not supported!", flow.name());
                }
            }
        }
    }

    private void addAssessmentType(Criteria c, String assessmentType) {
        c.createCriteria(EReportingDatasetEntity.SAMPLING_POINT).createCriteria(EReportingSamplingPointEntity.ASSESSMENTTYPE).
        add(Restrictions.ilike(EReportingAssessmentTypeEntity.ASSESSMENT_TYPE, assessmentType));
    }

    @Override
    protected DatasetEntity getImpl(ObservationContext ctx)
            throws OwsExceptionReport {
        return EReportingDatasetFactoy.getInstance().forDatasetType(ctx.getDatasetType());
    }

}
