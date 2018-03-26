/*******************************************************************************
 * Copyright (c) 2014 Ericsson, Inc. All Rights Reserved.
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

package com.ericsson.eea.ark.sli.grouping.util;

import java.io.File;

import java.util.concurrent.Future;



/** This class is used to differentiate the events received by the {@linkplain DirectoryMonitor} class.*/
@SuppressWarnings("rawtypes")
public class DirectorySubscriber {

   public Future path_created (File    path              ) { return null; }
   public Future path_modified(File    path              ) { return null; }
   public Future path_deleted (File    path              ) { return null; }
   public Future path_renamed (File oldPath, File newPath) { return null; }
}
