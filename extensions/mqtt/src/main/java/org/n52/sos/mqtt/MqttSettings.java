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
package org.n52.sos.mqtt;

import java.util.Collections;
import java.util.Set;

import org.n52.sos.config.SettingDefinition;
import org.n52.sos.config.SettingDefinitionGroup;
import org.n52.sos.config.SettingDefinitionProvider;
import org.n52.sos.config.settings.ChoiceSettingDefinition;
import org.n52.sos.config.settings.StringSettingDefinition;
import org.n52.sos.mqtt.decode.AdsbDecoder;
import org.n52.sos.mqtt.decode.FifaDecoder;

import com.google.common.collect.ImmutableSet;


public class MqttSettings implements SettingDefinitionProvider {
    
    public static final String MQTT_HOST = "mqtt.host";
    public static final String MQTT_PORT = "mqtt.port";
    public static final String MQTT_TOPIC = "mqtt.topic";
    public static final String MQTT_DECODER = "mqtt.decoder";
    public static final String MQTT_PROTOCOL = "mqtt.protocol";
    public static final SettingDefinitionGroup GROUP = new SettingDefinitionGroup().setTitle("MQTT").setOrder(10);
    
    public static final StringSettingDefinition MQTT_HOST_DEFINITION =
            new StringSettingDefinition()
                    .setGroup(GROUP)
                    .setOrder(ORDER_0)
                    .setKey(MQTT_HOST)
                    .setDefaultValue("ows.dev.52north.org")
                    .setOptional(true)
                    .setTitle("MQTT broker host")
                    .setDescription("");
    
    public static final StringSettingDefinition MQTT_PORT_DEFINITION =
            new StringSettingDefinition()
                    .setGroup(GROUP)
                    .setOrder(ORDER_1)
                    .setKey(MQTT_PORT)
                    .setDefaultValue("1883")
                    .setOptional(true)
                    .setTitle("MQTT broker port")
                    .setDescription("");
    
    public static final StringSettingDefinition MQTT_TOPIC_DEFINITION =
            new StringSettingDefinition()
                    .setGroup(GROUP)
                    .setOrder(ORDER_2)
                    .setKey(MQTT_TOPIC)
                    .setDefaultValue("n52.adsb")
                    .setOptional(true)
                    .setTitle("MQTT broker topic")
                    .setDescription("");
    
    public static final ChoiceSettingDefinition MQTT_PROTOCOL_DEFINITION =
            new ChoiceSettingDefinition()
                    .setGroup(GROUP)
                    .setOrder(ORDER_3)
                    .setKey(MQTT_PROTOCOL)
                    .setDefaultValue("tcp")
                    .addOption("tcp")
                    .addOption("ws")
                    .setOptional(false)
                    .setTitle("MQTT protocol")
                    .setDescription("Select the protocol");
    
    public static final ChoiceSettingDefinition MQTT_DECODER_DEFINITION =
            new ChoiceSettingDefinition()
                    .setGroup(GROUP)
                    .setOrder(ORDER_4)
                    .setKey(MQTT_DECODER)
                    .setDefaultValue(AdsbDecoder.class.getName())
                    .addOption(AdsbDecoder.class.getName(), AdsbDecoder.class.getSimpleName())
                    .addOption(FifaDecoder.class.getName(), FifaDecoder.class.getSimpleName())
                    .setOptional(false)
                    .setTitle("MQTT decoder")
                    .setDescription("Select the decoder");
    

    private static final Set<SettingDefinition<?, ?>> DEFINITIONS = ImmutableSet.<SettingDefinition<?, ?>> of(
            MQTT_HOST_DEFINITION, MQTT_PORT_DEFINITION, MQTT_TOPIC_DEFINITION, MQTT_PROTOCOL_DEFINITION, MQTT_DECODER_DEFINITION);

    @Override
    public Set<SettingDefinition<?, ?>> getSettingDefinitions() {
        return Collections.unmodifiableSet(DEFINITIONS);
    }

}
