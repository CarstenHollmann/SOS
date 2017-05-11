/**
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
package org.n52.sos.response;

import java.util.Collections;
import java.util.List;

import org.n52.sos.ogc.om.AbstractStreaming;
import org.n52.sos.ogc.om.ObservationMergeIndicator;
import org.n52.sos.ogc.om.ObservationMerger;
import org.n52.sos.ogc.gml.time.Time;
import org.n52.sos.ogc.gml.time.TimeInstant;
import org.n52.sos.ogc.gml.time.TimePeriod;
import org.n52.sos.ogc.om.OmObservation;
import org.n52.sos.ogc.ows.OwsExceptionReport;
import org.n52.sos.request.ResponseFormat;
import org.n52.sos.util.StringHelper;

import com.google.common.collect.Lists;

/**
 * TODO JavaDoc
 * 
 * @author Christian Autermann <c.autermann@52north.org>
 * 
 * @since 4.0.0
 */
public abstract class AbstractObservationResponse extends AbstractServiceResponse implements ResponseFormat, StreamingDataResponse {
    private List<OmObservation> observationCollection;

    private String responseFormat;

    private String resultModel;

    private boolean mergeObservation = false;
    
    private ObservationMerger observationMerger;
    
    private ObservationMergeIndicator observationMergeIndicator;

    private GlobalGetObservationValues globalValues;

    public List<OmObservation> getObservationCollection() {
        return Collections.unmodifiableList(observationCollection);
    }
    
    protected OmObservation getFirstObservation() {
        if (observationCollection != null && observationCollection.iterator().hasNext()) {
            return observationCollection.iterator().next();
        }
        return null;
    }

    public void setObservationCollection(final List<OmObservation> observationCollection) {
        this.observationCollection = observationCollection;
    }

    @Override
    public String getResponseFormat() {
        return responseFormat;
    }

    @Override
    public void setResponseFormat(final String responseFormat) {
        this.responseFormat = responseFormat;
    }

    @Override
    public boolean isSetResponseFormat() {
        return StringHelper.isNotEmpty(getResponseFormat());
    }

    public void setResultModel(final String resultModel) {
        this.resultModel = resultModel;
    }

    public String getResultModel() {
        return resultModel;
    }

    public boolean isSetResultModel() {
        return resultModel != null;
    }

    @Deprecated
    public void setMergeObservationValues(boolean mergeObservationValues) {
        setMergeObservations(mergeObservationValues);
    }

    @Deprecated
    public boolean isMergeObservationValues() {
        return isSetMergeObservation();
    }

    public void setMergeObservations(boolean mergeObservation) {
        this.mergeObservation = mergeObservation;
    }

    public boolean isSetMergeObservation() {
        return mergeObservation;
    }
    
    /**
     * @return the observationMerger
     */
    public ObservationMerger getObservationMerger() {
        if (observationMerger == null) {
            observationMerger = new ObservationMerger();
        }
        return observationMerger;
    }

    /**
     * @param observationMerger the observationMerger to set
     */
    public void setObservationMerger(ObservationMerger observationMerger) {
        this.observationMerger = observationMerger;
        setMergeObservations(true);
    }
    
    public void setObservationMergeIndicator(ObservationMergeIndicator indicator) {
        this.observationMergeIndicator = indicator;
    }

    public ObservationMergeIndicator getObservationMergeIndicator() {
        if (this.observationMergeIndicator == null) {
            setObservationMergeIndicator(new ObservationMergeIndicator());
        }
        return this.observationMergeIndicator;
    }

    @Override
    public boolean hasStreamingData() {
        OmObservation observation = getFirstObservation();
        if (observation != null) {
            return observation.getValue() instanceof AbstractStreaming;
        }
        return false;
    }

    @Override
    public void mergeStreamingData() throws OwsExceptionReport {
        List<OmObservation> observations = Lists.newArrayList();
        if (hasStreamingData()) {
            for (OmObservation observation : getObservationCollection()) {
                AbstractStreaming values = (AbstractStreaming) observation.getValue();
                if (values.hasNextValue()) {
                    if (isSetMergeObservation()) { 
                        observations.addAll(values.mergeObservation());
                    } else {
                        observations.addAll(values.getObservation());
                    }
                }
            }
        }
        setObservationCollection(observations);
    }

    public AbstractObservationResponse setGlobalValues(GlobalGetObservationValues globalValues) {
        this.globalValues = globalValues;
        return this;
    }
    
    public GlobalGetObservationValues getGlobalValues() {
        return globalValues;
    }
    
    public boolean hasGlobalValues() {
        return getGlobalValues() != null && !getGlobalValues().isEmpty();
    }

    public class GlobalGetObservationValues {
        private Time phenomenonTime;
        
        public GlobalGetObservationValues addPhenomenonTime(Time phenomenonTime) {
            if (isSetPhenomenonTime()) {
                if (phenomenonTime instanceof TimeInstant) {
                    this.phenomenonTime = new TimePeriod(this.phenomenonTime, this.phenomenonTime);
                }
                ((TimePeriod)this.phenomenonTime).extendToContain(phenomenonTime);
            } else {
                this.phenomenonTime = phenomenonTime;
            }
            return this;
        }
        
        public GlobalGetObservationValues setPhenomenonTime(Time phenomenonTime) {
            this.phenomenonTime = phenomenonTime;
            return this;
        }
        
        public Time getPhenomenonTime() {
            return phenomenonTime;
        }
        
        public boolean isSetPhenomenonTime() {
            return getPhenomenonTime() != null && !getPhenomenonTime().isEmpty();
        }
        
        public boolean isEmpty() {
            return !isSetPhenomenonTime();
        }
    
    }
    
}
