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

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.n52.series.db.DataModelUtil;
import org.n52.series.db.beans.ProcedureDescriptionFormatEntity;
import org.n52.series.db.beans.ProcedureEntity;
import org.n52.series.db.beans.ValidProcedureTimeEntity;
import org.n52.shetland.ogc.gml.time.Time;
import org.n52.sos.exception.ows.concrete.UnsupportedOperatorException;
import org.n52.sos.exception.ows.concrete.UnsupportedTimeException;
import org.n52.sos.exception.ows.concrete.UnsupportedValueReferenceException;
import org.n52.sw.db.util.QueryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class ValidProcedureTimeDao extends AbstractDao {

    private static final Logger LOGGER = LoggerFactory.getLogger(ValidProcedureTimeDao.class);

    public ValidProcedureTimeDao(DaoFactory daoFactory, Session session) {
        super(daoFactory, session);
    }

    /**
     * Insert valid procedure time for procedrue
     *
     * @param procedure
     *            Procedure object
     * @param xmlDescription
     *            Procedure XML description
     * @param validStartTime
     *            Valid start time
     */
    public void insertValidProcedureTime(ProcedureEntity procedure, ProcedureDescriptionFormatEntity procedureDescriptionFormat,
            String xmlDescription, DateTime validStartTime) {
        ValidProcedureTimeEntity vpd = new ValidProcedureTimeEntity();
        vpd.setProcedure(procedure);
        vpd.setProcedureDescriptionFormat(procedureDescriptionFormat);
        vpd.setDescriptionXml(xmlDescription);
        vpd.setStartTime(validStartTime.toDate());
        getSession().save(vpd);
        getSession().flush();
    }

    /**
     * Update valid procedure time object
     *
     * @param validProcedureTime
     *            Valid procedure time object
     */
    public void updateValidProcedureTime(ValidProcedureTimeEntity validProcedureTime) {
        getSession().saveOrUpdate(validProcedureTime);
    }

    /**
     * Set valid end time to valid procedure time object for procedure
     * identifier
     *
     * @param procedureIdentifier
     *            Procedure identifier
     * @throws UnsupportedOperatorException
     * @throws UnsupportedValueReferenceException
     * @throws UnsupportedTimeException
     */
    public void setValidProcedureDescriptionEndTime(String procedureIdentifier, String procedureDescriptionFormat,
            Session session) throws UnsupportedTimeException, UnsupportedValueReferenceException,
            UnsupportedOperatorException {
        ProcedureEntity procedure = getDaoFactory().getProcedureDao(getSession()).get(procedureIdentifier, procedureDescriptionFormat, null);
        Set<ValidProcedureTimeEntity> validProcedureTimes = procedure.getValidProcedureTimes();
        for (ValidProcedureTimeEntity validProcedureTime : validProcedureTimes) {
            if (validProcedureTime.getEndTime() == null) {
                validProcedureTime.setEndTime(new DateTime(DateTimeZone.UTC).toDate());
            }
        }
    }

    /**
     * Set valid end time to valid procedure time object for procedure
     * identifier
     *
     * @param procedureIdentifier
     *            Procedure identifier
     * @param session
     *            Hibernate session
     */
    public void setValidProcedureDescriptionEndTime(String procedureIdentifier) {
        ProcedureEntity procedure = getDaoFactory().getProcedureDao(getSession()).getIncludeDeleted(procedureIdentifier);
        Set<ValidProcedureTimeEntity> validProcedureTimes = procedure.getValidProcedureTimes();
        Date endTime = new DateTime(DateTimeZone.UTC).toDate();
        validProcedureTimes.stream()
                .filter(validProcedureTime -> validProcedureTime.getEndTime() == null)
                .forEach(validProcedureTime -> validProcedureTime.setEndTime(endTime));
    }

    /**
     * Get ValidProcedureTimes for requested parameters
     *
     * @param procedure
     *            Requested Procedure
     * @param procedureDescriptionFormat
     *            Requested procedureDescriptionFormat
     * @param validTime
     *            Requested validTime (optional)
     * @param session
     *            Hibernate session
     * @return List with ValidProcedureTime objects
     * @throws UnsupportedTimeException
     *             If validTime time value is invalid
     * @throws UnsupportedValueReferenceException
     *             If valueReference is not supported
     * @throws UnsupportedOperatorException
     *             If temporal operator is not supported
     */
    @SuppressWarnings("unchecked")
    public List<ValidProcedureTimeEntity> getValidProcedureTimes(ProcedureEntity procedure, String procedureDescriptionFormat,
            Time validTime) throws UnsupportedTimeException, UnsupportedValueReferenceException,
            UnsupportedOperatorException {
        Criteria criteria = getSession().createCriteria(ValidProcedureTimeEntity.class);
        criteria.add(Restrictions.eq(ValidProcedureTimeEntity.PROCEDURE, procedure));
        criteria.createCriteria(ValidProcedureTimeEntity.PROCEDURE_DESCRIPTION_FORMAT).add(
                Restrictions.eq(ProcedureDescriptionFormatEntity.PROCEDURE_DESCRIPTION_FORMAT, procedureDescriptionFormat));

        Criterion validTimeCriterion = QueryHelper.getValidTimeCriterion(validTime);
        // if validTime == null or validTimeCriterion == null, query latest
        // valid procedure description
        if (validTime == null || validTimeCriterion == null) {
            criteria.add(Restrictions.isNull(ValidProcedureTimeEntity.END_TIME));
        } else {
            criteria.add(validTimeCriterion);
        }
        LOGGER.debug("QUERY getValidProcedureTimes(procedure,procedureDescriptionFormat, validTime): {}",
                DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    @SuppressWarnings("unchecked")
    public List<ValidProcedureTimeEntity> getValidProcedureTimes(ProcedureEntity procedure,
            Set<String> possibleProcedureDescriptionFormats, Time validTime)
            throws UnsupportedTimeException, UnsupportedValueReferenceException, UnsupportedOperatorException {
        Criteria criteria = getSession().createCriteria(ValidProcedureTimeEntity.class);
        criteria.add(Restrictions.eq(ValidProcedureTimeEntity.PROCEDURE, procedure));
        criteria.createCriteria(ValidProcedureTimeEntity.PROCEDURE_DESCRIPTION_FORMAT).add(
                Restrictions.in(ProcedureDescriptionFormatEntity.PROCEDURE_DESCRIPTION_FORMAT,
                        possibleProcedureDescriptionFormats));

        Criterion validTimeCriterion = QueryHelper.getValidTimeCriterion(validTime);
        // if validTime == null or validTimeCriterion == null, query latest
        // valid procedure description
        if (validTime == null || validTimeCriterion == null) {
            criteria.add(Restrictions.isNull(ValidProcedureTimeEntity.END_TIME));
        } else {
            criteria.add(validTimeCriterion);
        }
        LOGGER.debug("QUERY getValidProcedureTimes(procedure, possibleProcedureDescriptionFormats, validTime): {}",
                DataModelUtil.getSqlString(criteria));
        return criteria.list();
    }

    public Map<String,String> getTProcedureFormatMap(Session session) {
        Criteria criteria = getSession().createCriteria(ProcedureEntity.class);
        criteria.createAlias(ProcedureEntity.PROPERTY_VALID_PROCEDURE_TIME, "vpt");
        criteria.createAlias(ValidProcedureTimeEntity.PROCEDURE_DESCRIPTION_FORMAT, "pdf");
        criteria.add(Restrictions.isNull("vpt." + ValidProcedureTimeEntity.END_TIME));
        criteria.setProjection(Projections.projectionList()
                .add(Projections.property(ProcedureEntity.PROPERTY_DOMAIN_ID))
                .add(Projections.property("pdf." + ProcedureDescriptionFormatEntity.PROCEDURE_DESCRIPTION_FORMAT)));
        criteria.addOrder(Order.asc(ProcedureEntity.PROPERTY_DOMAIN_ID));
        LOGGER.debug("QUERY getTProcedureFormatMap(): {}", DataModelUtil.getSqlString(criteria));
        @SuppressWarnings("unchecked")
        List<Object[]> results = criteria.list();
        Map<String,String> tProcedureFormatMap = Maps.newTreeMap();
        for (Object[] result : results) {
            String procedureIdentifier = (String) result[0];
            String format = (String) result[1];
            tProcedureFormatMap.put(procedureIdentifier, format);
        }
        return tProcedureFormatMap;
    }
}
