/*******************************************************************************
 * Copyright (c) 2015 Ericsson, Inc. All Rights Reserved.
 *
 * LICENSED MATERIAL - PROPERTY OF ERICSSON
 * Possession and/or use of this material is subject to the provisions
 * of a written license agreement with Ericsson
 *
 * ERICSSON CONFIDENTIAL - RESTRICTED ACCESS
 * This document and the confidential information it contains shall
 * be distributed, routed or made available solely to authorized persons
 * having a need to know within Ericsson, except with written
 * permission of Ericsson.
 *
 *******************************************************************************/

package com.ericsson.eea.ark.offline.rules.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.ToString;
import lombok.AllArgsConstructor;

import com.ericsson.bigdata.common.identifier.CellIdentifier;

/**
 * This class it is used to create Timer objects which prevent events from firing too frequently.
 */
@ToString
@AllArgsConstructor
public class Event_Timer {

   @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(Event_Timer.class.getName());

    @Getter private final String         blockingcat;
    @Getter private final String         ruleid;
    @Getter private final String         textid;
    @Getter private final CellIdentifier cellIdentifier;
    @Getter private final long           ts;
    @Getter private final String         dimensions;
}
