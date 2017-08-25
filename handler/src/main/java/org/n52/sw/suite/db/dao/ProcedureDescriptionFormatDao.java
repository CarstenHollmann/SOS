package org.n52.sw.suite.db.dao;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.n52.series.db.DataModelUtil;
import org.n52.series.db.beans.ProcedureDescriptionFormatEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcedureDescriptionFormatDao
        extends
        AbstractDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcedureDescriptionFormatDao.class);

    public ProcedureDescriptionFormatDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }

    /**
     * Get procedure description format object
     *
     * @param procedureDescriptionFormat
     *            Procedure description format
     * @return Procedure description format object
     */
    public ProcedureDescriptionFormatEntity get(String procedureDescriptionFormat) {
        Criteria c = getDefaultCriteria().add(Restrictions
                .eq(ProcedureDescriptionFormatEntity.PROCEDURE_DESCRIPTION_FORMAT, procedureDescriptionFormat));
        LOGGER.debug("QUERY get(procedureDescriptionFormat): {}", DataModelUtil.getSqlString(c));
        return (ProcedureDescriptionFormatEntity) c.uniqueResult();
    }
    
    @SuppressWarnings("unchecked")
    public List<ProcedureDescriptionFormatEntity> get() {
        Criteria c = getDefaultCriteria();
        LOGGER.debug("QUERY get(): {}", DataModelUtil.getSqlString(c));
        return c.list();
    }
    
    @SuppressWarnings("unchecked")
    public List<String> getProcedureDescriptionFormats() {
        Criteria c = getDefaultCriteria();
        c.setProjection(Projections
                .distinct(Projections.property(ProcedureDescriptionFormatEntity.PROCEDURE_DESCRIPTION_FORMAT)));
        LOGGER.debug("QUERY getProcedureDescriptionFormats(): {}", DataModelUtil.getSqlString(c));
        return c.list();
    }

    /**
     * Insert and get procedure description format
     *
     * @param procedureDescriptionFormat
     *            Procedure description format
     * @return Procedure description format object
     */
    public ProcedureDescriptionFormatEntity getOrInsert(String procedureDescriptionFormat) {
        ProcedureDescriptionFormatEntity hProcedureDescriptionFormat = get(procedureDescriptionFormat);
        if (hProcedureDescriptionFormat == null) {
            hProcedureDescriptionFormat = new ProcedureDescriptionFormatEntity();
            hProcedureDescriptionFormat.setProcedureDescriptionFormat(procedureDescriptionFormat);
            getSession().save(hProcedureDescriptionFormat);
            getSession().flush();
        }
        return hProcedureDescriptionFormat;
    }

    protected Criteria getDefaultCriteria() {
        return getSession().createCriteria(ProcedureDescriptionFormatEntity.class).setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY);
    }

}
