package org.n52.sw.suite.db.dao;

import java.util.LinkedList;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.n52.series.db.beans.RelatedFeatureRoleEntity;
import org.n52.sw.suite.db.util.HibernateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelatedFeatureRoleDao extends AbstractDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(RelatedFeatureRoleDao.class);

    public RelatedFeatureRoleDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }

    /**
     * Get related feature role objects for role
     *
     * @param role
     *            Related feature role
     * @return Related feature role objects
     */
    @SuppressWarnings("unchecked")
    public List<RelatedFeatureRoleEntity> get(String role) {
        Criteria criteria =
                getSession().createCriteria(RelatedFeatureRoleEntity.class).add(
                        Restrictions.eq(RelatedFeatureRoleEntity.RELATED_FEATURE_ROLE, role));
        LOGGER.debug("QUERY get(role): {}", HibernateHelper.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Insert and get related feature role objects
     *
     * @param role
     *            Related feature role
     * @return Related feature objects
     */
    public List<RelatedFeatureRoleEntity> getOrInsertRelatedFeatureRole(String role) {
        List<RelatedFeatureRoleEntity> relFeatRoles = get(role);
        if (relFeatRoles == null) {
            relFeatRoles = new LinkedList<RelatedFeatureRoleEntity>();
        }
        if (relFeatRoles.isEmpty()) {
            RelatedFeatureRoleEntity relFeatRole = new RelatedFeatureRoleEntity();
            relFeatRole.setRelatedFeatureRole(role);
            getSession().save(relFeatRole);
            getSession().flush();
            relFeatRoles.add(relFeatRole);
        }
        return relFeatRoles;
    }
}
