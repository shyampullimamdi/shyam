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

package com.ericsson.eea.ark.offline.rules;

import lombok.ToString;
import lombok.AllArgsConstructor;

import com.ericsson.eea.ark.offline.rules.events.Event;



/** This class represents a generic queue item where each item is represented as time stamp and Object.*/
@ToString
@AllArgsConstructor
public class QueueItem {

    public final String    timestamp;
    public final Event<?>  item;
}
