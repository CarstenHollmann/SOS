package org.n52.sw.suite.db.dao;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.n52.series.db.beans.FeatureEntity;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.RelatedFeatureEntity;
import org.n52.series.db.beans.RelatedFeatureRoleEntity;
import org.n52.shetland.ogc.gml.AbstractFeature;
import org.n52.shetland.ogc.om.features.samplingFeatures.AbstractSamplingFeature;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;
import org.n52.sw.suite.db.util.HibernateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelatedFeatureDao
        extends
        AbstractDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelatedFeatureDao.class);

    public RelatedFeatureDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }

    /**
     * Get all related feature objects
     *
     * @return Related feature objects
     */
    @SuppressWarnings("unchecked")
    public List<RelatedFeatureEntity> get(Session session) {
        Criteria criteria = getDefaultCriteria();
        LOGGER.debug("QUERY getRelatedFeatureObjects(): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get related feature objects for target identifier
     *
     * @param targetIdentifier
     *            Target identifier
     * @return Related feature objects
     */
    @SuppressWarnings("unchecked")
    public List<RelatedFeatureEntity> get(String targetIdentifier) {
        Criteria criteria = getDefaultCriteria();
        criteria.createCriteria(RelatedFeatureEntity.FEATURE_OF_INTEREST)
                .add(Restrictions.eq(FeatureEntity.PROPERTY_DOMAIN_ID, targetIdentifier));
        LOGGER.debug("QUERY getRelatedFeatures(targetIdentifier): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get related feature objects for offering identifier
     *
     * @param offering
     *            Offering identifier
     * @return Related feature objects
     */
    @SuppressWarnings("unchecked")
    public List<RelatedFeatureEntity> getForOffering(String offering) {
        Criteria criteria = getDefaultCriteria();
        criteria.createCriteria(RelatedFeatureEntity.OFFERINGS)
                .add(Restrictions.eq(OfferingEntity.PROPERTY_DOMAIN_ID, offering));
        LOGGER.debug("QUERY getRelatedFeatureForOffering(offering): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Insert and get related feature objects.
     *
     * @param feature
     *            Related feature
     * @param roles
     *            Related feature role objects
     * @return Related feature objects
     * @throws OwsExceptionReport
     *             If an error occurs
     */
    public List<RelatedFeatureEntity> getOrInsert(AbstractFeature feature, List<RelatedFeatureRoleEntity> roles)
            throws OwsExceptionReport {
        // TODO: create featureOfInterest and link to relatedFeature
        List<RelatedFeatureEntity> relFeats = get(feature.getIdentifierCodeWithAuthority().getValue());
        if (relFeats == null) {
            relFeats = new LinkedList<>();
        }
        if (relFeats.isEmpty()) {
            RelatedFeatureEntity relFeat = new RelatedFeatureEntity();
            String identifier = feature.getIdentifierCodeWithAuthority().getValue();
            String url = null;
            if (feature instanceof AbstractSamplingFeature) {
                identifier = Configurator.getInstance().getFeatureQueryHandler()
                        .insertFeature((AbstractSamplingFeature) feature, session);
                url = ((AbstractSamplingFeature) feature).getUrl();
            }
            relFeat.setFeatureOfInterest(getDaoFactory().getFeatureDao(getSession()).getOrInsert(identifier, url));
            relFeat.setRelatedFeatureRoles(new HashSet<>(roles));
            getSession().save(relFeat);
            getSession().flush();
            relFeats.add(relFeat);
        }
        return relFeats;
    }

    protected Criteria getDefaultCriteria() {
        return getSession().createCriteria(RelatedFeatureEntity.class).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }
}
