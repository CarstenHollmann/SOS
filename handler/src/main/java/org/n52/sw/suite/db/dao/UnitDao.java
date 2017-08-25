package org.n52.sw.suite.db.dao;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;
import org.n52.series.db.DataModelUtil;
import org.n52.series.db.beans.UnitEntity;
import org.n52.shetland.ogc.UoM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnitDao extends AbstractDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnitDao.class);
    
    public UnitDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }

    @SuppressWarnings("unchecked")
    public List<UnitEntity> getUnits() {
        Criteria criteria = getDeafaultCriteria();
        LOGGER.debug("QUERY get(): {}", DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    /**
     * Get unit object for unit
     *
     * @param unit
     *            UnitEntity
     * @param session
     *            Hibernate session
     * @return Unit object
     */
    public UnitEntity get(String unit) {
        Criteria criteria = getDeafaultCriteria().add(Restrictions.eq(UnitEntity.PROPERTY_UNIT, unit));
        LOGGER.debug("QUERY getUnit(unit): {}", DataModelUtil.getSqlString(criteria));
        return (UnitEntity) criteria.uniqueResult();
    }

    /**
     * Get unit object for unit
     *
     * @param unit
     *            UnitEntity
     * @return Unit object
     */
    public UnitEntity get(UoM unit) {
        Criteria criteria = getDeafaultCriteria().add(Restrictions.eq(UnitEntity.PROPERTY_UNIT, unit.getUom()));
        LOGGER.debug("QUERY getUnit(uom): {}", DataModelUtil.getSqlString(criteria));
        return (UnitEntity) criteria.uniqueResult();
    }

    /**
     * Insert and get unit object
     *
     * @param unit
     *            UnitEntity
     * @return Unit object
     */
    public UnitEntity getOrInsert(String unit) {
        return getOrInsert(new UoM(unit));
    }

    /**
     * Insert and get unit object
     *
     * @param unit
     *            UnitEntity
     * @return Unit object
     */
    public UnitEntity getOrInsert(UoM unit) {
        UnitEntity result = get(unit.getUom());
        if (result == null) {
            result = new UnitEntity();
            result.setUnit(unit.getUom());
            if (unit.isSetName()) {
                result.setName(unit.getName());
            }
            if (unit.isSetLink()) {
                result.setLink(unit.getLink());
            }
            getSession().save(result);
            getSession().flush();
            getSession().refresh(result);
        }
        return result;
    }
    
    protected Criteria getDeafaultCriteria() {
        return getSession().createCriteria(UnitEntity.class);
    }
}
