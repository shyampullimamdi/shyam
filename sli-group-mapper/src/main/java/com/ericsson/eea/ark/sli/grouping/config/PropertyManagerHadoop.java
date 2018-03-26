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

package com.ericsson.eea.ark.sli.grouping.config;

import lombok.Getter;

import org.apache.hadoop.conf.Configuration;

import com.ericsson.eea.ark.offline.config.PropertyManager;

@Getter
public class PropertyManagerHadoop extends PropertyManager {

   private Configuration config;


   /** Extract bootstrapping parameters from Hadoop's {@linkplain Configuration}.*/
   public PropertyManagerHadoop(Configuration config) {

      this.config = config;
      PropertyManager.setConfigDir (config.get("group-mapper.configurationDir" ));
      PropertyManager.setConfigFile(config.get("group-mapper.configurationFile"));
   }


//   public PropertyManagerHadoop(String propertyFile) {
//
//      super(propertyFile);
//   }
//
//
//   public PropertyManagerHadoop(String propertyFile, char separator) {
//
//      super(propertyFile, separator);
//   }
//
//
//   public PropertyManagerHadoop(String propertyFile, char separator, String bundleName) {
//
//      super(propertyFile, separator, bundleName);
//   }
//
//
//   public PropertyManagerHadoop(String propertyFile, String envVarName) {
//
//      super(propertyFile, envVarName);
//   }
//
//
//   public PropertyManagerHadoop(String propertyFile, String envVarName, String bundleName) {
//
//      super(propertyFile, envVarName, bundleName);
//   }
//
//
//   public PropertyManagerHadoop(String propertyFile, String envVarName, Package pkg) {
//
//      super(propertyFile, envVarName, pkg);
//   }
//
//
//   public PropertyManagerHadoop(String propertyFile, String envVarName, Class clazz) {
//
//      super(propertyFile, envVarName, clazz);
//   }
//
//
//   public PropertyManagerHadoop(Class<?> clazz, String configDir, String propertyFile, String fileSuffix, String fileExtension) {
//
//      super(clazz, configDir, propertyFile, fileSuffix, fileExtension);
//   }
}
