package org.n52.sw.suite.db.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.xmlbeans.XmlException;
import org.apache.xmlbeans.XmlObject;
import org.hibernate.Criteria;
import org.hibernate.FetchMode;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.hibernate.sql.JoinType;
import org.n52.series.db.beans.AbstractFeatureEntity;
import org.n52.series.db.beans.FeatureEntity;
import org.n52.series.db.beans.ObservationConstellationEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProcedureEntity;
import org.n52.series.db.beans.ResultTemplateEntity;
import org.n52.shetland.ogc.gml.AbstractFeature;
import org.n52.shetland.ogc.ows.exception.InvalidParameterValueException;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.shetland.ogc.sos.Sos2Constants;
import org.n52.shetland.ogc.sos.SosResultEncoding;
import org.n52.shetland.ogc.sos.SosResultStructure;
import org.n52.shetland.ogc.sos.request.InsertResultTemplateRequest;
import org.n52.shetland.ogc.swe.SweAbstractDataComponent;
import org.n52.shetland.ogc.swe.SweConstants;
import org.n52.shetland.ogc.swe.encoding.SweAbstractEncoding;
import org.n52.shetland.util.CollectionHelper;
import org.n52.svalbard.decode.Decoder;
import org.n52.svalbard.decode.DecoderKey;
import org.n52.svalbard.decode.DecoderRepository;
import org.n52.svalbard.decode.XmlNamespaceDecoderKey;
import org.n52.svalbard.decode.exception.DecodingException;
import org.n52.svalbard.decode.exception.NoDecoderForKeyException;
import org.n52.svalbard.decode.exception.XmlDecodingException;
import org.n52.svalbard.encode.Encoder;
import org.n52.svalbard.encode.EncoderKey;
import org.n52.svalbard.encode.EncoderRepository;
import org.n52.svalbard.encode.EncodingContext;
import org.n52.svalbard.encode.XmlBeansEncodingFlags;
import org.n52.svalbard.encode.XmlEncoderKey;
import org.n52.svalbard.encode.exception.EncodingException;
import org.n52.svalbard.encode.exception.NoEncoderForKeyException;
import org.n52.svalbard.util.CodingHelper;
import org.n52.svalbard.util.XmlOptionsHelper;
import org.n52.sw.suite.db.util.HibernateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class ResultTemplateDao extends AbstractDao {
    private static Logger LOGGER = LoggerFactory.getLogger(ResultTemplateDao.class);
    
    private EncoderRepository encoderRepository;
    private DecoderRepository decoderRepository;
    private XmlOptionsHelper xmlOptionsHelper;

    public ResultTemplateDao(DaoFactory daoFactory, Session session, EncoderRepository encoderRepository, XmlOptionsHelper xmlOptionsHelper, DecoderRepository decoderRepository) {
        super(daoFactory, session);
        this.encoderRepository = encoderRepository;
        this.xmlOptionsHelper = xmlOptionsHelper;
        this.decoderRepository = decoderRepository;
    }

    /**
     * Get result template object for result template identifier
     *
     * @param identifier
     *            Result template identifier
     * @return Result template object
     */
    public ResultTemplateEntity get(String identifier) {
        Criteria criteria =
                getDefaultCriteria()
                        .add(Restrictions.eq(ResultTemplateEntity.PROPERTY_DOMAIN_ID, identifier))
                        .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
        LOGGER.debug("QUERY getResultTemplateObject(identifier): {}", HibernateHelper.getSqlString(criteria));
        return (ResultTemplateEntity) criteria.uniqueResult();
    }

    /**
     * Get all result template objects
     *
     * @return Result template objects
     */
    @SuppressWarnings("unchecked")
    public List<ResultTemplateEntity> get(Session session) {
        return getDefaultCriteria()
                .setFetchMode(ResultTemplateEntity.PROPERTY_OFFERING, FetchMode.JOIN)
                .setFetchMode(ResultTemplateEntity.PROPERTY_OBSERVABLE_PROPERTY, FetchMode.JOIN)
                .setFetchMode(ResultTemplateEntity.PROPERTY_FEATURE_OF_INTEREST, FetchMode.JOIN)
                .list();
    }

    /**
     * Get result template object for observation constellation
     *
     * @param observationConstellation
     *            Observation constellation object
     * @return Result template object
     */
    public ResultTemplateEntity get(
            ObservationConstellationEntity observationConstellation) {
        return get(observationConstellation.getOffering().getIdentifier(),
                observationConstellation.getObservableProperty().getIdentifier());
    }

    /**
     * Get result template objects for observation constellation and
     * featureOfInterest
     *
     * @param observationConstellation
     *            Observation constellation object
     * @param sosAbstractFeature
     *            FeatureOfInterest
     * @return Result template objects
     */
    public List<ResultTemplateEntity> get(
            ObservationConstellationEntity observationConstellation, AbstractFeature sosAbstractFeature) {
        return getResultTemplateObject(observationConstellation.getOffering().getIdentifier(),
                observationConstellation.getObservableProperty().getIdentifier(),
                Lists.newArrayList(sosAbstractFeature.getIdentifierCodeWithAuthority().getValue()));
    }

    /**
     * Get result template object for offering identifier and observable
     * property identifier
     *
     * @param offering
     *            Offering identifier
     * @param observedProperty
     *            Observable property identifier
     * @return Result template object
     */
    @SuppressWarnings("unchecked")
    public ResultTemplateEntity get(String offering, String observedProperty) {
        Criteria rtc = getDefaultCriteria().setMaxResults(1);
        rtc.createCriteria(ObservationConstellationEntity.OFFERING).add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering));
        rtc.createCriteria(ObservationConstellationEntity.OBSERVABLE_PROPERTY).add(
                Restrictions.eq(PhenomenonEntity.PROPERTY_DOMAIN_ID, observedProperty));
        /* there can be multiple but equal result templates... */
        LOGGER.debug("QUERY getResultTemplateObject(offering, observedProperty): {}",
                HibernateHelper.getSqlString(rtc));
        List<ResultTemplateEntity> templates = rtc.list();
        return templates.isEmpty() ? null : templates.iterator().next();
    }

    /**
     * Get result template objects for offering identifier, observable property
     * identifier and featureOfInterest identifier
     *
     * @param offering
     *            Offering identifier
     * @param observedProperty
     *            Observable property identifier
     * @param featureOfInterest
     *            FeatureOfInterest identifier
     * @return Result template objects
     */
    @SuppressWarnings("unchecked")
    public List<ResultTemplateEntity> getResultTemplateObject(String offering, String observedProperty,
            Collection<String> featureOfInterest) {
        Criteria rtc = getDefaultCriteria();
        rtc.createCriteria(ResultTemplateEntity.PROPERTY_OFFERING).add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering));
        rtc.createCriteria(ResultTemplateEntity.PROPERTY_OBSERVABLE_PROPERTY).add(
                Restrictions.eq(PhenomenonEntity.PROPERTY_DOMAIN_ID, observedProperty));
        if (featureOfInterest != null && !featureOfInterest.isEmpty()) {
            rtc.createAlias(ResultTemplateEntity.PROPERTY_FEATURE_OF_INTEREST, "foi", JoinType.LEFT_OUTER_JOIN);
            rtc.add(Restrictions.or(Restrictions.isNull(ResultTemplateEntity.PROPERTY_FEATURE_OF_INTEREST),
                    Restrictions.in("foi." + FeatureEntity.PROPERTY_DOMAIN_ID, featureOfInterest)));
            // rtc.createCriteria(ResultTemplateEntity.FEATURE_OF_INTEREST).add(
            // Restrictions.in(FeatureOfInterest.PROPERTY_DOMAIN_ID,
            // featureOfInterest));
        }
        LOGGER.debug("QUERY getResultTemplateObject(offering, observedProperty, featureOfInterest): {}",
                HibernateHelper.getSqlString(rtc));
        return rtc.list();
    }

    /**
     * Check or insert result template
     *
     * @param request
     *            Insert result template request
     * @param observationConstellation
     *            Observation constellation object
     * @param procedure
     * @param featureOfInterest
     *            FeatureOfInterest object
     * @param session
     *            Hibernate session
     * @throws OwsExceptionReport
     *             If the requested structure/encoding is invalid
     */
    public void checkOrInsertResultTemplateEntity(InsertResultTemplateRequest request,
            ObservationConstellationEntity observationConstellation,
            ProcedureEntity procedure,
            AbstractFeatureEntity featureOfInterest)
            throws OwsExceptionReport {
        try {
            String offering = observationConstellation.getOffering().getIdentifier();
            String observableProperty = observationConstellation.getObservableProperty().getIdentifier();

            List<ResultTemplateEntity> resultTemplates =
                    getResultTemplateObject(offering, observableProperty, null);
            if (CollectionHelper.isEmpty(resultTemplates)) {
                createAndSaveResultTemplateEntity(request, observationConstellation, procedure, featureOfInterest);
            } else {
                List<String> storedIdentifiers = new ArrayList<>(0);

                for (ResultTemplateEntity storedResultTemplateEntity : resultTemplates) {
                    storedIdentifiers.add(storedResultTemplateEntity.getIdentifier());
                    SosResultStructure storedStructure =
                            createSosResultStructure(storedResultTemplateEntity.getResultStructure());
                    SosResultEncoding storedEncoding =
                            createSosResultEncoding(storedResultTemplateEntity.getResultEncoding());

                    if (!storedStructure.equals(request.getResultStructure())) {
                        throw new InvalidParameterValueException()
                                .at(Sos2Constants.InsertResultTemplateParams.proposedTemplate)
                                .withMessage(
                                        "The requested resultStructure is different from already inserted result template for procedure (%s) observedProperty (%s) and offering (%s)!",
                                        procedure.getIdentifier(), observableProperty, offering);
                    }

                    if (!storedEncoding.equals(request.getResultEncoding())) {
                        throw new InvalidParameterValueException()
                                .at(Sos2Constants.InsertResultTemplateParams.proposedTemplate)
                                .withMessage(
                                        "The requested resultEncoding is different from already inserted result template for procedure (%s) observedProperty (%s) and offering (%s)!",
                                        procedure.getIdentifier(), observableProperty, offering);
                    }
                }
                if (request.getIdentifier() != null && !storedIdentifiers.contains(request.getIdentifier())) {
                    /* save it only if the identifier is different */
                    createAndSaveResultTemplateEntity(request, observationConstellation, procedure, featureOfInterest);
                }

            }
        } catch (EncodingException | DecodingException ex) {
            throw new NoApplicableCodeException().causedBy(ex);
        }
    }
    
    protected Criteria getDefaultCriteria() {
        return getSession().createCriteria(ResultTemplateEntity.class).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

    /**
     * Insert result template
     *
     * @param request
     *            Insert result template request
     * @param observationConstellation
     *            Observation constellation object
     * @param procedure
     * @param featureOfInterest
     *            FeatureOfInterest object
     * @throws OwsExceptionReport
     */
    private void createAndSaveResultTemplateEntity(InsertResultTemplateRequest request,
            ObservationConstellationEntity observationConstellation, ProcedureEntity procedure,
            AbstractFeatureEntity featureOfInterest)
            throws EncodingException {
        ResultTemplateEntity resultTemplate = new ResultTemplateEntity();
        resultTemplate.setIdentifier(request.getIdentifier().getValue());
        resultTemplate.setObservableProperty(observationConstellation.getObservableProperty());
        resultTemplate.setOffering(observationConstellation.getOffering());
        if (procedure != null) {
            resultTemplate.setProcedure(procedure);
        }
        if (featureOfInterest != null) {
            resultTemplate.setFeatureOfInterest(featureOfInterest);
        }

        if (request.getResultEncoding().getXml().isPresent()) {
            resultTemplate.setResultEncoding(request.getResultEncoding().getXml().get());
        } else {
            resultTemplate.setResultEncoding(
                    encodeObjectToXmlText(SweConstants.NS_SWE_20, request.getResultEncoding().get().get()));
        }
        if (request.getResultStructure().getXml().isPresent()) {
            resultTemplate.setResultStructure(request.getResultStructure().getXml().get());
        } else {
            resultTemplate.setResultStructure(
                    encodeObjectToXmlText(SweConstants.NS_SWE_20, request.getResultStructure().get().get()));
        }

        getSession().save(resultTemplate);
        getSession().flush();
    }

    private SosResultEncoding createSosResultEncoding(String resultEncoding)
            throws DecodingException {
        return new SosResultEncoding((SweAbstractEncoding) decodeXmlObject(resultEncoding), resultEncoding);
    }

    private SosResultStructure createSosResultStructure(String resultStructure)
            throws DecodingException {
        return new SosResultStructure((SweAbstractDataComponent) decodeXmlObject(resultStructure), resultStructure);
    }

    private String encodeObjectToXmlText(String namespace, Object object)
            throws EncodingException {
        return encodeObjectToXml(namespace, object).xmlText(this.xmlOptionsHelper.getXmlOptions());
    }

    private XmlObject encodeObjectToXml(String namespace, Object object)
            throws EncodingException {
        return getEncoder(namespace, object).encode(object, EncodingContext.of(XmlBeansEncodingFlags.DOCUMENT, true));
    }

    private <T> Encoder<XmlObject, T> getEncoder(String namespace, T o)
            throws EncodingException {
        EncoderKey key = new XmlEncoderKey(namespace, o.getClass());
        Encoder<XmlObject, T> encoder = encoderRepository.getEncoder(key);
        if (encoder == null) {
            throw new NoEncoderForKeyException(key);
        }
        return encoder;
    }

    private <T> T decodeXmlObject(XmlObject xbObject)
            throws DecodingException {
        DecoderKey key = CodingHelper.getDecoderKey(xbObject);
        Decoder<T, XmlObject> decoder = decoderRepository.getDecoder(key);
        if (decoder == null) {
            DecoderKey schemaTypeKey = new XmlNamespaceDecoderKey(xbObject.schemaType().getName().getNamespaceURI(),
                    xbObject.getClass());
            decoder = decoderRepository.getDecoder(schemaTypeKey);
        }
        if (decoder == null) {
            throw new NoDecoderForKeyException(key);
        }
        return decoder.decode(xbObject);
    }

    private Object decodeXmlObject(String xmlString)
            throws DecodingException {
        try {
            return decodeXmlObject(XmlObject.Factory.parse(xmlString));
        } catch (XmlException e) {
            throw new XmlDecodingException("XML string", xmlString, e);
        }
    }

}
