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

import org.n52.series.db.beans.BlobDataEntity;
import org.n52.series.db.beans.BooleanDataEntity;
import org.n52.series.db.beans.CategoryDataEntity;
import org.n52.series.db.beans.ComplexDataEntity;
import org.n52.series.db.beans.CountDataEntity;
import org.n52.series.db.beans.DataEntity;
import org.n52.series.db.beans.GeometryDataEntity;
import org.n52.series.db.beans.ProfileDataEntity;
import org.n52.series.db.beans.QuantityDataEntity;
import org.n52.series.db.beans.SweDataArrayDataEntity;
import org.n52.series.db.beans.TextDataEntity;
import org.n52.shetland.ogc.om.OmConstants;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;


public class ObservationFactory {

    @SuppressWarnings({"unchecked" })
    public Class<? extends DataEntity<?>> observationClass() {
        return (Class<? extends DataEntity<?>>) DataEntity.class;
    }

    public Class<? extends BlobDataEntity> blobClass() {
        return BlobDataEntity.class;
    }

    public BlobDataEntity blob()
            throws OwsExceptionReport {
        return instantiate(blobClass());
    }

    public Class<? extends BooleanDataEntity> truthClass() {
        return BooleanDataEntity.class;
    }

    public BooleanDataEntity truth()
            throws OwsExceptionReport {
        return instantiate(truthClass());
    }

    public Class<? extends CategoryDataEntity> categoryClass() {
        return CategoryDataEntity.class;
    }

    public CategoryDataEntity category()
            throws OwsExceptionReport {
        return instantiate(categoryClass());
    }

    public Class<? extends CountDataEntity> countClass() {
        return CountDataEntity.class;
    }

    public CountDataEntity count()
            throws OwsExceptionReport {
        return instantiate(countClass());
    }

    public Class<? extends GeometryDataEntity> geometryClass() {
        return GeometryDataEntity.class;
    }

    public GeometryDataEntity geometry()
            throws OwsExceptionReport {
        return instantiate(geometryClass());
    }

    public Class<? extends QuantityDataEntity> numericClass() {
        return QuantityDataEntity.class;
    }

    public QuantityDataEntity numeric()
            throws OwsExceptionReport {
        return instantiate(numericClass());
    }

    public Class<? extends SweDataArrayDataEntity> sweDataArrayClass() {
        return SweDataArrayDataEntity.class;
    }

    public SweDataArrayDataEntity sweDataArray()
            throws OwsExceptionReport {
        return instantiate(sweDataArrayClass());
    }

    public Class<? extends TextDataEntity> textClass() {
        return TextDataEntity.class;
    }

    public TextDataEntity text()
            throws OwsExceptionReport {
        return instantiate(textClass());
    }

    public Class<? extends ComplexDataEntity> complexClass() {
        return ComplexDataEntity.class;
    }

    public ComplexDataEntity complex()
            throws OwsExceptionReport {
        return instantiate(complexClass());
    }

    public Class<? extends ProfileDataEntity> profileClass() {
        return ProfileDataEntity.class;
    }

    public ProfileDataEntity profile()
            throws OwsExceptionReport {
        return instantiate(profileClass());
    }

    private <T extends DataEntity<?>> T instantiate(Class<T> c)
            throws OwsExceptionReport {
        try {
            return c.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new NoApplicableCodeException().causedBy(ex)
                    .withMessage("Error while creating observation instance for %s", c);
        }
    }

    @SuppressWarnings("rawtypes")
    public Class<? extends DataEntity> classForDataEntityType(
            String observationType) {
        if (observationType != null) {
            switch (observationType) {
                case OmConstants.OBS_TYPE_MEASUREMENT:
                    return numericClass();
                case OmConstants.OBS_TYPE_COUNT_OBSERVATION:
                    return countClass();
                case OmConstants.OBS_TYPE_CATEGORY_OBSERVATION:
                    return categoryClass();
                case OmConstants.OBS_TYPE_TRUTH_OBSERVATION:
                    return truthClass();
                case OmConstants.OBS_TYPE_TEXT_OBSERVATION:
                    return textClass();
                case OmConstants.OBS_TYPE_GEOMETRY_OBSERVATION:
                    return geometryClass();
                case OmConstants.OBS_TYPE_COMPLEX_OBSERVATION:
                    return complexClass();
                case OmConstants.OBS_TYPE_SWE_ARRAY_OBSERVATION:
                    return sweDataArrayClass();
                case OmConstants.OBS_TYPE_UNKNOWN:
                    return blobClass();
                default:
                    return observationClass();
            }
        }
        return observationClass();
    }

    public DataEntity<?> forDataEntityType(String observationType)
            throws OwsExceptionReport {
        return instantiate(classForDataEntityType(observationType));
    }
}
