/*
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
package org.n52.sos.decode.sos.v2;

import java.util.Collections;
import java.util.Set;

import net.opengis.sos.x20.CapabilitiesDocument;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.n52.shetland.ogc.sos.Sos2Constants;
import org.n52.shetland.ogc.ows.service.GetCapabilitiesResponse;
import org.n52.shetland.ogc.ows.OwsCapabilities;
import org.n52.sos.util.CodingHelper;
import org.n52.svalbard.decode.DecoderKey;
import org.n52.svalbard.decode.exception.DecodingException;
import org.n52.svalbard.decode.exception.UnsupportedDecoderInputException;
import org.n52.svalbard.xml.AbstractXmlDecoder;

import com.google.common.base.Joiner;

public class CapabilitiesDocumentDecoder extends AbstractXmlDecoder<CapabilitiesDocument, GetCapabilitiesResponse> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetObservationResponseDocumentDecoder.class);

    private static final Set<DecoderKey> DECODER_KEYS = CodingHelper.decoderKeysForElements(
            Sos2Constants.NS_SOS_20,
            CapabilitiesDocument.class);


    public CapabilitiesDocumentDecoder() {
        LOGGER.debug("Decoder for the following keys initialized successfully: {}!", Joiner.on(", ")
                .join(DECODER_KEYS));
    }

    @Override
    public Set<DecoderKey> getKeys() {
        return Collections.unmodifiableSet(DECODER_KEYS);
    }

    @Override
    public GetCapabilitiesResponse decode(CapabilitiesDocument cd)
            throws DecodingException {
        if (cd != null)  {
            GetCapabilitiesResponse response = new GetCapabilitiesResponse();
            OwsCapabilities capabilities = (OwsCapabilities)decodeXmlObject(cd.getCapabilities());
            response.setCapabilities(capabilities);
            return response;
        }
        throw new UnsupportedDecoderInputException(this, cd);
    }

}