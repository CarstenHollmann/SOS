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
