package org.n52.sw.suite.db.dao;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.n52.series.db.beans.CodespaceEntity;
import org.n52.sw.suite.db.util.HibernateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CodespaceDao
        extends
        AbstractDao {
    
    public CodespaceDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CodespaceDao.class);

    /**
     * Get codespace object for identifier
     *
     * @param codespace
     *            identifier
     * @return CodespaceEntity object
     */
    public CodespaceEntity get(String codespace) {
        Criteria criteria =
                getDefaultCriteria().add(Restrictions.eq(CodespaceEntity.PROPERTY_CODESPACE, codespace));
        LOGGER.debug("QUERY get(codespace): {}", HibernateHelper.getSqlString(criteria));
        return (CodespaceEntity) criteria.uniqueResult();
    }

    /**
     * Insert and/or get codespace object
     *
     * @param codespace
     *            Codespace identifier
     * @return CodespaceEntity object
     */
    public CodespaceEntity getOrInsert(String codespace) {
        CodespaceEntity result = get(codespace);
        if (result == null) {
            result = new CodespaceEntity();
            result.setCodespace(codespace);
            getSession().save(result);
            getSession().flush();
            getSession().refresh(result);
        }
        return result;
    }

   protected Criteria getDefaultCriteria() {
       return getSession().createCriteria(CodespaceEntity.class);
   }
}
