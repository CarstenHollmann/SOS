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
package org.n52.sos.decode.json.impl;

import org.n52.shetland.inspire.GeographicalName;
import org.n52.shetland.inspire.ad.AddressRepresentation;
import org.n52.sos.coding.json.AQDJSONConstants;
import org.n52.svalbard.decode.exception.DecodingException;

import com.fasterxml.jackson.databind.JsonNode;

public class AddressJSONDecoder extends AbstractJSONDecoder<AddressRepresentation> {

    public AddressJSONDecoder() {
        super(AddressRepresentation.class);
    }

    @Override
    public AddressRepresentation decodeJSON(JsonNode node, boolean validate)
            throws DecodingException {
        AddressRepresentation address = new AddressRepresentation();
        address.setAddressFeature(parseNillableReference(node
                .path(AQDJSONConstants.ADDRESS_FEATURE)));
        address.setPostCode(parseNillableString(node
                .path(AQDJSONConstants.POST_CODE)));
        for (JsonNode n : node.path(AQDJSONConstants.ADDRESS_AREAS)) {
            address.addAddressArea(decodeJsonToNillable(n, GeographicalName.class));
        }
        for (JsonNode n : node.path(AQDJSONConstants.ADMIN_UNITS)) {
            address.addAdminUnit(decodeJsonToObject(n, GeographicalName.class));
        }
        for (JsonNode n : node.path(AQDJSONConstants.LOCATOR_DESIGNATORS)) {
            address.addLocatorDesignator(n.textValue());
        }
        for (JsonNode n : node.path(AQDJSONConstants.LOCATOR_NAMES)) {
            address.addLocatorName(decodeJsonToObject(n, GeographicalName.class));
        }
        for (JsonNode n : node.path(AQDJSONConstants.POST_NAMES)) {
            address.addPostName(decodeJsonToNillable(n, GeographicalName.class));
        }
        for (JsonNode n : node.path(AQDJSONConstants.THOROUGHFARES)) {
            address.addThoroughfare(decodeJsonToNillable(n, GeographicalName.class));
        }
        return address;
    }
}