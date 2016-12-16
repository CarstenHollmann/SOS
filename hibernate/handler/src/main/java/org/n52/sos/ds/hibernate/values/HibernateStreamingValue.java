/*
 * Copyright (C) 2012-2016 52°North Initiative for Geospatial Open Source
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
package org.n52.sos.ds.hibernate.values;

import org.n52.iceland.ds.ConnectionProvider;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.request.GetObservationRequest;
import org.n52.sos.ds.hibernate.dao.ValueDAO;
import org.n52.sos.ds.hibernate.dao.ValueTimeDAO;
import org.n52.sos.ds.hibernate.entities.observation.legacy.TemporalReferencedLegacyObservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract Hibernate streaming value class for old observation concept
 *
 * @author <a href="mailto:c.hollmann@52north.org">Carsten Hollmann</a>
 * @since 4.1.0
 *
 */
public abstract class HibernateStreamingValue extends AbstractHibernateStreamingValue {

    private static final Logger LOGGER = LoggerFactory.getLogger(HibernateStreamingValue.class);
    protected final ValueDAO valueDAO = new ValueDAO();
    protected final ValueTimeDAO valueTimeDAO = new ValueTimeDAO();
    protected final long procedure;
    protected final long featureOfInterest;
    protected final long observableProperty;

    /**
     * constructor
     *
     * @param request
     *            {@link GetObservationRequest}
     * @param procedure
     *            Datasource procedure id
     * @param observableProperty
     *            observableProperty procedure id
     * @param featureOfInterest
     *            featureOfInterest procedure id
     */
    public HibernateStreamingValue(ConnectionProvider connectionProvider, GetObservationRequest request, long procedure, long observableProperty,
            long featureOfInterest) {
        super(connectionProvider, request);
        this.procedure = procedure;
        this.observableProperty = observableProperty;
        this.featureOfInterest = featureOfInterest;
    }

    @Override
    protected void queryTimes() {
        try {
            if (session == null) {
                session = sessionHolder.getSession();
            }
            TemporalReferencedLegacyObservation minTime;
            TemporalReferencedLegacyObservation maxTime;
            // query with temporal filter
            if (temporalFilterCriterion != null) {
                minTime = valueTimeDAO.getMinValueFor((GetObservationRequest)request, procedure, observableProperty, featureOfInterest, temporalFilterCriterion, session);
                maxTime = valueTimeDAO.getMaxValueFor((GetObservationRequest)request, procedure, observableProperty, featureOfInterest, temporalFilterCriterion, session);
            }
            // query without temporal or indeterminate filters
            else {
                minTime = valueTimeDAO.getMinValueFor((GetObservationRequest)request, procedure, observableProperty, featureOfInterest, session);
                maxTime = valueTimeDAO.getMaxValueFor((GetObservationRequest)request, procedure, observableProperty, featureOfInterest, session);
            }
            setPhenomenonTime(createPhenomenonTime(minTime, maxTime));
            setResultTime(createResutlTime(maxTime));
            setValidTime(createValidTime(minTime, maxTime));
        } catch (OwsExceptionReport owse) {
            LOGGER.error("Error while querying times", owse);
        }
    }

    @Override
    protected void queryUnit() {
        try {
            setUnit(valueDAO.getUnit((GetObservationRequest)request, procedure, observableProperty, featureOfInterest, session));
        } catch (OwsExceptionReport owse) {
            LOGGER.error("Error while querying unit", owse);
        }
    }

}