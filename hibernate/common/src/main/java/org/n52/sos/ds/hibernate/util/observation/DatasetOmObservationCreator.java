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
package org.n52.sos.ds.hibernate.util.observation;

import java.util.List;
import java.util.Locale;

import org.hibernate.Query;
import org.hibernate.Session;
import org.n52.iceland.convert.ConverterException;
import org.n52.janmayen.http.MediaType;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.shetland.ogc.UoM;
import org.n52.shetland.ogc.gml.AbstractFeature;
import org.n52.shetland.ogc.gml.time.TimeInstant;
import org.n52.shetland.ogc.om.ObservationStream;
import org.n52.shetland.ogc.om.OmObservableProperty;
import org.n52.shetland.ogc.om.OmObservation;
import org.n52.shetland.ogc.om.OmObservationConstellation;
import org.n52.shetland.ogc.om.SingleObservationValue;
import org.n52.shetland.ogc.om.values.NilTemplateValue;
import org.n52.shetland.ogc.ows.exception.CodedException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.SosProcedureDescription;
import org.n52.shetland.ogc.sos.request.AbstractObservationRequest;
import org.n52.sos.ds.hibernate.dao.observation.series.AbstractSeriesObservationDAO;
import org.n52.sos.ds.hibernate.dao.observation.series.parameter.SeriesParameterDAO;
import org.n52.sos.ds.hibernate.entities.observation.series.Series;
import org.n52.sos.ds.hibernate.entities.parameter.series.SeriesParameterAdder;
import org.n52.sw.db.util.HibernateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Create {@link OmObservation}s from series
 *
 * @since 4.0.0
 *
 */
public class DatasetOmObservationCreator extends AbstractOmObservationCreator {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatasetOmObservationCreator.class);
    private final DatasetEntity dataset;

    public DatasetOmObservationCreator(
            DatasetEntity dataset,
            AbstractObservationRequest request,
            Locale i18n,
            String pdf,
            OmObservationCreatorContext creatorContext,
            Session session) {
        super(request, i18n, pdf, creatorContext, session);
        this.dataset = dataset;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public ObservationStream create() throws OwsExceptionReport, ConverterException {
        if(dataset == null) {
            return ObservationStream.empty();
        }
        final OmObservationConstellation obsConst = createObservationConstellation(dataset);
        final OmObservation sosObservation = new OmObservation();
        sosObservation.setNoDataValue(getNoDataValue());
        sosObservation.setTokenSeparator(getTokenSeparator());
        sosObservation.setTupleSeparator(getTupleSeparator());
        sosObservation.setDecimalSeparator(getDecimalSeparator());
        sosObservation.setObservationConstellation(obsConst);
        checkForAdditionalObservationCreator(dataset, sosObservation);
        final NilTemplateValue value = new NilTemplateValue();
        value.setUnit(createUnit(dataset));
        sosObservation.setValue(new SingleObservationValue(new TimeInstant(), value));
        return ObservationStream.of(sosObservation);
    }

    @SuppressWarnings("unchecked")
    protected void checkForAdditionalObservationCreator(DatasetEntity dataset, OmObservation sosObservation) throws CodedException {
        AdditionalObservationCreatorKey key = new AdditionalObservationCreatorKey(getResponseFormat(), dataset.getClass());
        if (getCreatorContext().getAdditionalObservationCreatorRepository().hasAdditionalObservationCreatorFor(key)) {
            getCreatorContext().getAdditionalObservationCreatorRepository().get(key).create(sosObservation, dataset);
        } else if (checkAcceptType()) {
            for (MediaType acceptType : getAcceptType()) {
                AdditionalObservationCreatorKey acceptKey = new AdditionalObservationCreatorKey(acceptType.withoutParameters().toString(), dataset.getClass());
                if (getCreatorContext().getAdditionalObservationCreatorRepository().hasAdditionalObservationCreatorFor(acceptKey)) {
                    AdditionalObservationCreator creator = getCreatorContext().getAdditionalObservationCreatorRepository().get(acceptKey);
                    creator.create(sosObservation, dataset, getSession());
                }
            }
        }
    }

    private void addParameter(OmObservation observation, DatasetEntity dataset) throws OwsExceptionReport {
        new SeriesParameterAdder(observation, new SeriesParameterDAO().getSeriesParameter(dataset, getSession())).add();
    }

}
