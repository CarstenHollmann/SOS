package org.n52.sw.suite.db.dao;

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
import org.n52.series.db.dao.DatasetDao;
import org.n52.shetland.ogc.ows.exception.CodedException;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.util.EReportingSetting;
import org.n52.svalbard.decode.DecoderRepository;
import org.n52.svalbard.encode.EncoderRepository;
import org.n52.svalbard.util.XmlOptionsHelper;
import org.n52.sw.suite.db.util.HibernateHelper;

@Configurable
public class DaoFactory {

    private Set<Integer> validityFlags;
    private Set<Integer> verificationFlags;
    private EncoderRepository encoderRepository;
    private DecoderRepository decoderRepository;
    private XmlOptionsHelper xmlOptionsHelper;

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

    public boolean isSeriesDao() {
        if (HibernateHelper.isEntitySupported(EReportingDatasetEntity.class)) {
            return true;
        } else if (HibernateHelper.isEntitySupported(DatasetEntity.class)) {
            return true;
        } else {
            return false;
        }
    }

    public DatasetDao getSeriesDao(Session session)
            throws OwsExceptionReport {
        if (HibernateHelper.isEntitySupported(EReportingDatasetEntity.class)) {
            return new EReportingDatasetDao(session);
        } else if (HibernateHelper.isEntitySupported(DatasetEntity.class)) {
            return new DatasetDao(session);
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
    public DataDao getObservationDao(Session session)
            throws OwsExceptionReport {
        if (HibernateHelper.isEntitySupported(EReportingDataEntity.class)) {
            return new EReportingDataDao(this.verificationFlags, this.validityFlags, this);
        } else if (HibernateHelper.isEntitySupported(DataEntity.class)) {
            return new DataDao(session);
        } else {
            throw new NoApplicableCodeException().withMessage("Implemented observation Dao is missing!");
        }
    }

    public AbstractObservationTimeDao getObservationTimeDao(Session session)
            throws OwsExceptionReport {
        if (HibernateHelper.isEntitySupported(TemporalReferencedEReportingObservation.class)) {
            return new EReportingObservationTimeDao();
        } else if (HibernateHelper.isEntitySupported(TemporalReferencedSeriesObservation.class)) {
            return new SeriesObservationTimeDao();
        } else {
            throw new NoApplicableCodeException().withMessage("Implemented observation time Dao is missing!");
        }
    }

    public AbstractSeriesValueDao getValueDao(Session session)
            throws OwsExceptionReport {
        if (HibernateHelper.isEntitySupported(AbstractValuedEReportingObservation.class)) {
            return new EReportingValueDao(this.verificationFlags, this.validityFlags);
        } else if (HibernateHelper.isEntitySupported(AbstractValuedSeriesObservation.class)) {
            return new SeriesValueDao();
        } else {
            throw new NoApplicableCodeException().withMessage("Implemented value Dao is missing!");
        }
    }

    public AbstractSeriesValueTimeDao getValueTimeDaoSession(Session session)
            throws OwsExceptionReport {
        if (HibernateHelper.isEntitySupported(TemporalReferencedEReportingObservation.class)) {
            return new EReportingValueTimeDao(this.verificationFlags, this.validityFlags);
        } else if (HibernateHelper.isEntitySupported(TemporalReferencedSeriesObservation.class)) {
            return new SeriesValueTimeDao();
        } else {
            throw new NoApplicableCodeException().withMessage("Implemented value time Dao is missing!");
        }
    }

    public AbstractFeatureOfInterestDao getFeatureDao(Session session)
            throws CodedException {
        return getFeatureOfInterestDao();
    }

    public ProcedureDao getProcedureDao(Session session) {
        return new ProcedureDao(this, session);
    }

    public PhenomenonDao getObservablePropertyDao(Session session) {
        return new PhenomenonDao(this, session);
    }

    public FeatureOfInterestDao getFeatureOfInterestDao(Session session) {
        return new FeatureOfInterestDao(this);
    }

    public ValidProcedureTimeDao getValidProcedureTimeDao(Session session) {
        return new ValidProcedureTimeDao(this);
    }

    public ObservationConstellationDao getObservationConstellationDao(Session session) {
        return new ObservationConstellationDao(this);
    }

    public RelatedFeatureDao getRelatedFeatureDao(Session session) {
        return new RelatedFeatureDao(this);
    }

    public UnitDao getUnitDao(Session session) {
        return new UnitDao();
    }

    public ResultTemplateDao getResultTemplateDao(Session session) {
        return new ResultTemplateDao(this, session, encoderRepository, xmlOptionsHelper, decoderRepository);
    }

    public RelatedFeatureRoleDao getRelatedFeatureRoleDao(Session session) {
        return new RelatedFeatureRoleDao();
    }

    public CodespaceDao getCodespaceDao(Session session) {
        return new CodespaceDao(this, session);
    }

    public ObservationTypeDao getObservationTypeDao(Session session) {
        return new ObservationTypeDao();
    }

    public OfferingDao getOfferingDao(Session session) {
        return new OfferingDao(session);
    }

    public ParameterDao getParameterDao(Session session) {
        return new ParameterDao();
    }

    public ProcedureDescriptionFormatDao getProcedureDescriptionFormatDao(Session session) {
        return new ProcedureDescriptionFormatDao();
    }

}
