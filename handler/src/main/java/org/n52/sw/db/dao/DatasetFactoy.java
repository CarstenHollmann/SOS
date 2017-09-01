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

import org.n52.series.db.beans.BlobDatasetEntity;
import org.n52.series.db.beans.BooleanDatasetEntity;
import org.n52.series.db.beans.CategoryDatasetEntity;
import org.n52.series.db.beans.ComplexDatasetEntity;
import org.n52.series.db.beans.CountDatasetEntity;
import org.n52.series.db.beans.DatasetEntity;
import org.n52.series.db.beans.GeometryDatasetEntity;
import org.n52.series.db.beans.ProfileDatasetEntity;
import org.n52.series.db.beans.QuantityDatasetEntity;
import org.n52.series.db.beans.SweDataArrayDatasetEntity;
import org.n52.series.db.beans.TextDatasetEntity;
import org.n52.series.db.beans.dataset.BooleanDataset;
import org.n52.series.db.beans.dataset.CategoryDataset;
import org.n52.series.db.beans.dataset.ComplexDataset;
import org.n52.series.db.beans.dataset.CountDataset;
import org.n52.series.db.beans.dataset.GeometryDataset;
import org.n52.series.db.beans.dataset.ProfileDataset;
import org.n52.series.db.beans.dataset.QuantityDataset;
import org.n52.series.db.beans.dataset.SweDataArrayDataset;
import org.n52.series.db.beans.dataset.TextDataset;
import org.n52.shetland.ogc.om.OmConstants;
import org.n52.shetland.ogc.ows.exception.NoApplicableCodeException;
import org.n52.shetland.ogc.ows.exception.OwsExceptionReport;

public class DatasetFactoy {

    protected DatasetFactoy() {
    }

    public BlobDatasetEntity blob()
            throws OwsExceptionReport {
        return instantiate(blobClass());
    }

    public BooleanDatasetEntity truth()
            throws OwsExceptionReport {
        return instantiate(truthClass());
    }

    public CategoryDatasetEntity category()
            throws OwsExceptionReport {
        return instantiate(categoryClass());
    }

    public CountDatasetEntity count()
            throws OwsExceptionReport {
        return instantiate(countClass());
    }

    public GeometryDatasetEntity geometry()
            throws OwsExceptionReport {
        return instantiate(geometryClass());
    }

    public QuantityDatasetEntity numeric()
            throws OwsExceptionReport {
        return instantiate(numericClass());
    }

    public SweDataArrayDatasetEntity sweDataArray()
            throws OwsExceptionReport {
        return instantiate(sweDataArrayClass());
    }

    public TextDatasetEntity text()
            throws OwsExceptionReport {
        return instantiate(textClass());
    }

    public ComplexDatasetEntity complex()
            throws OwsExceptionReport {
        return instantiate(complexClass());
    }

    public ProfileDatasetEntity profile()
            throws OwsExceptionReport {
        return instantiate(profileClass());
    }

    public Class<? extends DatasetEntity> classForDatasetType(
            String observationType) {
        if (observationType != null) {
            switch (observationType) {
                case QuantityDataset.DATASET_TYPE:
                    return numericClass();
                case CountDataset.DATASET_TYPE:
                    return countClass();
                case CategoryDataset.DATASET_TYPE:
                    return categoryClass();
                case BooleanDataset.DATASET_TYPE:
                    return truthClass();
                case TextDataset.DATASET_TYPE:
                    return textClass();
                case GeometryDataset.DATASET_TYPE:
                    return geometryClass();
                case ComplexDataset.DATASET_TYPE:
                    return complexClass();
                case SweDataArrayDataset.DATASET_TYPE:
                    return sweDataArrayClass();
                case ProfileDataset.DATASET_TYPE:
                    return sweDataArrayClass();
                case OmConstants.OBS_TYPE_UNKNOWN:
                    return blobClass();
                default:
                    return datasetClass();
            }
        }
        return datasetClass();
    }

    public DatasetEntity forDatasetType(String observationType)
            throws OwsExceptionReport {
        return instantiate(classForDatasetType(observationType));
    }

    public Class<? extends BlobDatasetEntity> blobClass() {
        return BlobDatasetEntity.class;
    }

    public Class<? extends BooleanDatasetEntity> truthClass() {
        return BooleanDatasetEntity.class;
    }

    public Class<? extends CategoryDatasetEntity> categoryClass() {
        return CategoryDatasetEntity.class;
    }

    public Class<? extends CountDatasetEntity> countClass() {
        return CountDatasetEntity.class;
    }

    public Class<? extends GeometryDatasetEntity> geometryClass() {
        return GeometryDatasetEntity.class;
    }

    public Class<? extends QuantityDatasetEntity> numericClass() {
        return QuantityDatasetEntity.class;
    }

    public Class<? extends SweDataArrayDatasetEntity> sweDataArrayClass() {
        return SweDataArrayDatasetEntity.class;
    }

    public Class<? extends TextDatasetEntity> textClass() {
        return TextDatasetEntity.class;
    }

    public Class<? extends ComplexDatasetEntity> complexClass() {
        return ComplexDatasetEntity.class;
    }

    public Class<? extends ProfileDatasetEntity> profileClass() {
        return ProfileDatasetEntity.class;
    }

    public DatasetEntity dataset() {
        return new DatasetEntity();
    }

    public Class<? extends DatasetEntity> datasetClass() {
        return DatasetEntity.class;
    }

    private <T extends DatasetEntity> T instantiate(Class<T> c)
            throws OwsExceptionReport {
        try {
            return c.newInstance();
        } catch (InstantiationException | IllegalAccessException ex) {
            throw new NoApplicableCodeException().causedBy(ex)
                    .withMessage("Error while creating observation instance for %s", c);
        }
    }

    public static DatasetFactoy getInstance() {
        return Holder.INSTANCE;
    }

    private static class Holder {
        private static final DatasetFactoy INSTANCE
                = new DatasetFactoy();

        private Holder() {
        }
    }
}
