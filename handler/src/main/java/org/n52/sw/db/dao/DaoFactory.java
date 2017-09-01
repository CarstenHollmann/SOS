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

import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import org.hibernate.Session;
import org.n52.faroe.annotation.Configurable;
import org.n52.faroe.annotation.Setting;
import org.n52.series.db.DataModelUtil;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.ereporting.EReportingDataEntity;
import org.n52.series.db.beans.ereporting.EReportingDatasetEntity;
import org.n52.series.db.dao.CodespaceDao;
import org.n52.series.db.dao.DatasetDao;
import org.n52.shetland.ogc.ows.exception.CodedException;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.util.EReportingSetting;
import org.n52.sos.ds.FeatureQueryHandler;
import org.n52.sos.util.GeometryHandler;
import org.n52.svalbard.decode.DecoderRepository;
import org.n52.svalbard.encode.EncoderRepository;
import org.n52.svalbard.util.XmlOptionsHelper;
import org.n52.sw.db.util.HibernateHelper;

@Configurable
public class DaoFactory {

    private Set<Integer> validityFlags;
    private Set<Integer> verificationFlags;
    private EncoderRepository encoderRepository;
    private DecoderRepository decoderRepository;
    private GeometryHandler geometryHandler;
    private FeatureQueryHandler featureQueryHandler;
    private XmlOptionsHelper xmlOptionsHelper;
    private Boolean includeChildObservableProperties;

    @Setting(value = EReportingSetting.EREPORTING_VALIDITY_FLAGS, required = false)
    public void setValidityFlags(String validityFlags) {
        this.validityFlags = Optional.ofNullable(validityFlags)
                .map(s -> Arrays.stream(s.split(",")).map(Integer::parseInt).collect(toSet()))
                .orElseGet(Collections::emptySet);
    }

    @Setting(value = EReportingSetting.EREPORTING_VERIFICATION_FLAGS, required = false)
    public void setVerificationFlags(String verificationFlags) {
        this.verificationFlags = Optional.ofNullable(verificationFlags)
                .map(s -> Arrays.stream(s.split(",")).map(Integer::parseInt).collect(toSet()))
                .orElseGet(Collections::emptySet);
    }

    @Setting(value = EReportingSetting.EREPORTING_VERIFICATION_FLAGS, required = false)
    public void setIncludeChildObservableProperties(String includeChildObservableProperties) {
        this.includeChildObservableProperties = Optional.ofNullable(includeChildObservableProperties).map(Boolean::parseBoolean).orElse(false);
    }

    @Inject
    public void setEncoderRepository(EncoderRepository encoderRepository) {
        this.encoderRepository = encoderRepository;
    }

    @Inject
    public void setDecoderRepository(DecoderRepository decoderRepository) {
        this.decoderRepository = decoderRepository;
    }

    @Inject
    public void setXmlOptionsHelper(XmlOptionsHelper xmlOptionsHelper) {
        this.xmlOptionsHelper = xmlOptionsHelper;
    }

    @Inject
    public void setGeometryHandler(GeometryHandler geometryHandler) {
        this.geometryHandler = geometryHandler;
    }

    @Inject
    public void setFeatureQueryHandler(FeatureQueryHandler featureQueryHandler) {
        this.featureQueryHandler = featureQueryHandler;
    }

    public boolean isSeriesDao() {
        if (HibernateHelper.isEntitySupported(EReportingDatasetEntity.class)) {
            return true;
        } else if (HibernateHelper.isEntitySupported(DatasetEntity.class)) {
            return true;
        } else {
            return false;
        }
    }

    public AbstractSeriesDao getSeriesDao(Session session)
            throws OwsExceptionReport {
        if (HibernateHelper.isEntitySupported(EReportingDatasetEntity.class)) {
            return new EReportingSeriesDao(this, session, includeChildObservableProperties);
        } else if (HibernateHelper.isEntitySupported(DatasetEntity.class)) {
            return new SeriesDao(this, session, includeChildObservableProperties);
        } else {
            throw new NoApplicableCodeException().withMessage("Implemented series Dao is missing!");
        }
    }

    /**
     * Get the currently supported Hibernate Observation data access
     * implementation
     *
     * @return Currently supported Hibernate Observation data access
     *         implementation
     *
     * @throws OwsExceptionReport
     *             If no Hibernate Observation data access is supported
     */
    public AbstractSeriesObservationDao getObservationDao(Session session)
            throws OwsExceptionReport {
        if (HibernateHelper.isEntitySupported(EReportingDataEntity.class)) {
            return new EReportingDataDao(this.verificationFlags, this.validityFlags, this);
        } else if (HibernateHelper.isEntitySupported(DataEntity.class)) {
            return new DataDao(session);
        } else {
            throw new NoApplicableCodeException().withMessage("Implemented observation Dao is missing!");
        }
    }

    public AbstractSeriesObservationDao getObservationTimeDao(Session session)
            throws OwsExceptionReport {
//        if (HibernateHelper.isEntitySupported(TemporalReferencedEReportingObservation.class)) {
//            return new EReportingObservationTimeDao();
//        } else if (HibernateHelper.isEntitySupported(TemporalReferencedSeriesObservation.class)) {
//            return new SeriesObservationTimeDao();
//        } else {
//            throw new NoApplicableCodeException().withMessage("Implemented observation time Dao is missing!");
//        }
        return getObservationDao(session);
    }

    public AbstractSeriesObservationDao getValueDao(Session session)
            throws OwsExceptionReport {
//        if (HibernateHelper.isEntitySupported(AbstractValuedEReportingObservation.class)) {
//            return new EReportingValueDao(this.verificationFlags, this.validityFlags);
//        } else if (HibernateHelper.isEntitySupported(AbstractValuedSeriesObservation.class)) {
//            return new SeriesValueDao();
//        } else {
//            throw new NoApplicableCodeException().withMessage("Implemented value Dao is missing!");
//        }
        return getObservationDao(session);
    }

    public AbstractSeriesObservationDao getValueTimeDao(Session session)
            throws OwsExceptionReport {
//        if (HibernateHelper.isEntitySupported(TemporalReferencedEReportingObservation.class)) {
//            return new EReportingValueTimeDao(this.verificationFlags, this.validityFlags);
//        } else if (HibernateHelper.isEntitySupported(TemporalReferencedSeriesObservation.class)) {
//            return new SeriesValueTimeDao();
//        } else {
//            throw new NoApplicableCodeException().withMessage("Implemented value time Dao is missing!");
//        }
        return getObservationDao(session);
    }

    public AbstractFeatureDao getFeatureDao(Session session)
            throws CodedException {
        return getFeatureOfInterestDao(session);
    }

    public ProcedureDao getProcedureDao(Session session) {
        return new ProcedureDao(this, session);
    }

    public PhenomenonDao getObservablePropertyDao(Session session) {
        return new PhenomenonDao(this, session);
    }

    public FeatureDao getFeatureOfInterestDao(Session session) {
        return new FeatureDao(this, session, geometryHandler, featureQueryHandler);
    }

    public ValidProcedureTimeDao getValidProcedureTimeDao(Session session) {
        return new ValidProcedureTimeDao(this, session);
    }

    public ObservationConstellationDao getObservationConstellationDao(Session session) {
        return new ObservationConstellationDao(this, session);
    }

    public RelatedFeatureDao getRelatedFeatureDao(Session session) {
        return new RelatedFeatureDao(this, session);
    }

    public UnitDao getUnitDao(Session session) {
        return new UnitDao(this, session);
    }

    public ResultTemplateDao getResultTemplateDao(Session session) {
        return new ResultTemplateDao(this, session, encoderRepository, xmlOptionsHelper, decoderRepository);
    }

    public RelatedFeatureRoleDao getRelatedFeatureRoleDao(Session session) {
        return new RelatedFeatureRoleDao(this, session);
    }

    public CodespaceDao getCodespaceDao(Session session) {
        return new CodespaceDao(session);
    }

    public ObservationTypeDao getObservationTypeDao(Session session) {
        return new ObservationTypeDao(this, session);
    }

    public OfferingDao getOfferingDao(Session session) {
        return new OfferingDao(this, session);
    }

    public ParameterDao getParameterDao(Session session) {
        return new ParameterDao(this, session);
    }

    public ProcedureDescriptionFormatDao getProcedureDescriptionFormatDao(Session session) {
        return new ProcedureDescriptionFormatDao(this, session);
    }

    public FeatureTypeDao getFeatureTypeDao(Session session) {
        return new FeatureTypeDao(this, session);
    }

}
