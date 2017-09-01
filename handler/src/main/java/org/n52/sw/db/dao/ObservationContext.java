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

import org.hibernate.Criteria;
import org.hibernate.criterion.Restrictions;
import org.n52.series.db.beans.AbstractFeatureEntity;
import org.n52.series.db.beans.CategoryEntity;
import org.n52.series.db.beans.HibernateRelations;
import org.n52.series.db.beans.OfferingEntity;
import org.n52.series.db.beans.PhenomenonEntity;
import org.n52.series.db.beans.ProcedureEntity;

import com.google.common.base.Strings;

/**
 * Class to carry observation identifiers (featureOfInterest,
 * observableProperty, procedure).
 *
 * @author Carsten Hollmann <c.hollmann@52north.org>
 * @since 4.0.0
 *
 */
public class ObservationContext {
    private AbstractFeatureEntity featureOfInterest;
    private PhenomenonEntity observableProperty;
    private ProcedureEntity procedure;
    private OfferingEntity offering;
    private String datasetType;
    private boolean hiddenChild = false;
    private CategoryEntity category;

    /**
     * @return the featureOfInterest
     */
    public AbstractFeatureEntity getFeatureOfInterest() {
        return featureOfInterest;
    }

    /**
     * @param featureOfInterest
     *                          the featureOfInterest to set
     */
    public void setFeatureOfInterest(AbstractFeatureEntity featureOfInterest) {
        this.featureOfInterest = featureOfInterest;
    }

    /**
     * @return the observableProperty
     */
    public PhenomenonEntity getPhenomenon() {
        return observableProperty;
    }

    /**
     * @param observableProperty
     *                           the observableProperty to set
     */
    public void setPhenomenon(PhenomenonEntity observableProperty) {
        this.observableProperty = observableProperty;
    }

    /**
     * @return the procedure
     */
    public ProcedureEntity getProcedure() {
        return procedure;
    }

    /**
     * @param procedure
     *                  the procedure to set
     */
    public void setProcedure(ProcedureEntity procedure) {
        this.procedure = procedure;
    }

    public boolean isSetFeatureOfInterest() {
        return getFeatureOfInterest() != null;
    }

    public boolean isSetPhenomenon() {
        return getPhenomenon() != null;
    }

    public boolean isSetProcedure() {
        return getOffering() != null;
    }

    /**
     * @return the offering
     */
    public OfferingEntity getOffering() {
        return offering;
    }

    /**
     * @param offering the offering to set
     */
    public void setOffering(OfferingEntity offering) {
        this.offering = offering;
    }

    public boolean isSetOffering() {
        return getOffering() != null;
    }

    public CategoryEntity getCategory() {
        return category;
    }

    /**
     * @param category the category to set
     */
    public void setCategoryEntity(CategoryEntity category) {
        this.category = category;
    }

    public boolean isSetCategoryEntity() {
        return getCategory() != null;
    }

    public void addIdentifierRestrictionsToCritera(Criteria c) {
        if (isSetFeatureOfInterest()) {
            c.add(Restrictions
                    .eq(HibernateRelations.HasWriteableObservationContext.FEATURE_OF_INTEREST,
                        getFeatureOfInterest()));
        }
        if (isSetPhenomenon()) {
            c.add(Restrictions
                    .eq(HibernateRelations.HasWriteableObservationContext.OBSERVABLE_PROPERTY,
                        getPhenomenon()));
        }
        if (isSetOffering()) {
            c.add(Restrictions
                    .eq(HibernateRelations.HasWriteableObservationContext.PROCEDURE,
                        getProcedure()));
        }
        if (isSetOffering()) {
            c.add(Restrictions.eq(HibernateRelations.HasWriteableObservationContext.OFFERING, getOffering()));
        }
    }

    public void addValuesToSeries(HibernateRelations.HasWriteableObservationContext contextual) {
        if (isSetFeatureOfInterest()) {
            contextual.setFeature(getFeatureOfInterest());
        }
        if (isSetPhenomenon()) {
            contextual.setObservableProperty(getPhenomenon());
        }
        if (isSetOffering()) {
            contextual.setOffering(getOffering());
        }
        if (isSetOffering()) {
            contextual.setOffering(getOffering());
        }
        if (contextual instanceof HibernateRelations.HasDatasetType && isSetDatasetType()) {
            ((HibernateRelations.HasDatasetType)contextual).setDatasetType(getDatasetType());
        }
    }

    public void setDatasetType(String datasetType) {
        this.datasetType = datasetType;
    }

    public String getDatasetType() {
        return this.datasetType;
    }

    public boolean isSetDatasetType() {
        return !Strings.isNullOrEmpty(getDatasetType());
    }

    public void setHiddenChild(boolean hiddenChild) {
        this.hiddenChild = hiddenChild;
    }

    public boolean isHiddenChild() {
        return this.hiddenChild;
    }

}
