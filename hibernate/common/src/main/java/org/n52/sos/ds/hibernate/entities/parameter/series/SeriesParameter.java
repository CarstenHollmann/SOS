/**
 * Copyright (C) 2012-2016 52°North Initiative for Geospatial Open Source
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
package org.n52.sos.ds.hibernate.entities.parameter.series;

import com.google.common.base.Strings;

public abstract class SeriesParameter<T> implements org.n52.sos.ds.hibernate.entities.parameter.Parameter<T> {

    private static final long serialVersionUID = -4205643420255372085L;
    public static String SERIES_ID = "seriesId";
    private long parameterId;
    private long seriesId;
    private String name;
    
    public SeriesParameter() {
        super();
    }
    
    /**
     * @return the parameterId
     */
    public long getParameterId() {
        return parameterId;
    }

    /**
     * @param parameterId the parameterId to set
     */
    public void setParameterId(long parameterId) {
        this.parameterId = parameterId;
    }

    /**
     * @return the seriesId
     */
    public long getSeriesId() {
        return seriesId;
    }

    /**
     * @param seriesId the seriesId to set
     */
    public void setSeriesId(long seriesId) {
        this.seriesId = seriesId;
    }

    /**
     * @return the name
     */
    @Override
    public String getName() {
        return name;
    }
    /**
     * @param name the name to set
     */
    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean isSetName() {
        return !Strings.isNullOrEmpty(getName());
    }

}
