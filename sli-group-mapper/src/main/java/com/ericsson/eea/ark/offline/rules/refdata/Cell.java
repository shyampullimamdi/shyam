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

package com.ericsson.eea.ark.offline.rules.refdata;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;

//import java.sql.ResultSet;
//import java.sql.Statement;

import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bigdata.common.column.Dimension;
//import com.ericsson.bigdata.ruleengine.RuleEngine;
//import com.ericsson.bigdata.ruleengine.utils.NGEEAlarmHelper;
//import com.ericsson.bigdata.ruleengine.utils.SQLConnection;

import com.ericsson.eea.ark.common.service.model.CellLocationMapper;


/*
 *  1.1 new Cell_location_table definition:
 *
 * drop table if exists cell_location;
 *
 * create table cell_location (rat               text
 *   ,                         cell_name         text
 *   ,                         cid               integer
 *   ,                         mcc               integer
 *   ,                         mnc               integer
 *   ,                         lat               double precision not null
 *   ,                         lon               double precision not null
 *   ,                         beam_direction    double precision
 *   ,                         cell_type         text
 *   ,                         cell_range        integer
 *   ,                         tilt              double precision
 *   ,                         height            double precision
 *   ,                         sector            text
 *   ,                         carrier           text
 *   ,                         region1           text
 *   ,                         lac               integer
 *   ,                         rac               integer
 *   ,                         msc               text
 *   ,                         gsm_bsc           text
 *   ,                         gsm_bts           text
 *   ,                         wcdma_sac         integer
 *   ,                         wcdma_rnc         text
 *   ,                         wcdma_nodeb_name  text
 *   ,                         lte_enodeb_id     integer
 *   ,                         lte_enodeb_name   text
 *   ,                         lte_tac           integer
 *   ,                         lte_eci           integer
 *   ,                         region2           text
 *   ,                         region3           text
 *   ,                         region4           text
 * )
 * without oids;
 *
 * alter table cell_location add constraint pk_cell_location primary key (rat,cell_name);
 */


/**
 * Cell: maintain local cache of cell information. Note: This
 *         may be deprecated in favor of dataGrid interface.  This is maintained
 *         for development purpose and because this interface was previously exposed via Rules.
 */
@ToString
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Cell {

   private static final Logger log = LoggerFactory.getLogger(Cell.class.getName());

   @Getter @Setter private String  location_identifier = null;
   @Getter @Setter private String  rat                 = null;
   @Getter @Setter private String  cell_name           = null;
   @Getter @Setter private Integer cid                 = null;
   @Getter @Setter private Integer mcc                 = null;
   @Getter @Setter private Integer mnc                 = null;
   @Getter @Setter private Double  lat                 = null;
   @Getter @Setter private Double  lon                 = null;
   @Getter @Setter private Double  beam_direction      = null;
   @Getter @Setter private String  cell_type           = null;
   @Getter @Setter private Integer cell_range          = null;
   @Getter @Setter private Double  tilt                = null;
   @Getter @Setter private Double  height              = null;
   @Getter @Setter private String  sector              = null;
   @Getter @Setter private String  carrier             = null;
   @Getter @Setter private String  region1             = null;
   @Getter @Setter private Integer lac                 = null;
   @Getter @Setter private Integer rac                 = null;
   @Getter @Setter private String  msc                 = null;
   @Getter @Setter private String  gsm_bsc             = null;
   @Getter @Setter private String  gsm_bts             = null;
   @Getter @Setter private Integer wcdma_sac           = null;
   @Getter @Setter private String  wcdma_rnc           = null;
   @Getter @Setter private String  wcdma_nodeb_name    = null;
   @Getter @Setter private Integer lte_enodeb_id       = null;
   @Getter @Setter private String  lte_enodeb_name     = null;
   @Getter @Setter private Integer lte_tac             = null;
   @Getter @Setter private Integer lte_eci             = null;
   @Getter @Setter private String  region2             = null;
   @Getter @Setter private String  region3             = null;
   @Getter @Setter private String  region4             = null;


   public Cell(Cell c) {

      super();

      location_identifier = c.getLocation_identifier();
      rat                 = c.getRat();
      cell_name           = c.getCell_name();
      cid                 = c.getCid();
      mcc                 = c.getMcc();
      mnc                 = c.getMnc();
      lat                 = c.getLat();
      lon                 = c.getLon();
      beam_direction      = c.getBeam_direction();
      cell_type           = c.getCell_type();
      cell_range          = c.getCell_range();
      tilt                = c.getTilt();
      height              = c.getHeight();
      sector              = c.getSector();
      carrier             = c.getCarrier();
      region1             = c.getRegion1();
      lac                 = c.getLac();
      rac                 = c.getRac();
      msc                 = c.getMsc();
      gsm_bsc             = c.getGsm_bsc();
      gsm_bts             = c.getGsm_bts();
      wcdma_sac           = c.getWcdma_sac();
      wcdma_rnc           = c.getWcdma_rnc();
      wcdma_nodeb_name    = c.getWcdma_nodeb_name();
      lte_enodeb_id       = c.getLte_enodeb_id();
      lte_enodeb_name     = c.getLte_enodeb_name();
      lte_tac             = c.getLte_tac();
      lte_eci             = c.getLte_eci();
      region2             = c.getRegion2();
      region3             = c.getRegion3();
      region4             = c.getRegion4();
   }


   public Cell(CellLocationMapper c) {

      if (c == null) return;

      location_identifier = c.getLocationIdentifier();
      rat                 = c.getRat();
      cell_name           = c.getCellName();
      cid                 = c.getCid();
      mcc                 = c.getMcc();
      mnc                 = c.getMnc();
      lat                 = c.getLat();
      lon                 = c.getLon();
      beam_direction      = c.getBeamDirection();
      cell_type           = c.getCellType();
      cell_range          = c.getCellRange();
      tilt                = c.getTilt();
      height              = c.getHeight();
      sector              = c.getSector();
      carrier             = c.getCarrier();
      region1             = c.getRegion1();
      lac                 = c.getLac();
      rac                 = c.getRac();
      msc                 = c.getMsc();
      gsm_bsc             = c.getGsmBsc();
      gsm_bts             = c.getGsmBts();
      wcdma_sac           = c.getWcdmaSac();
      wcdma_rnc           = c.getWcdmaRnc();
      wcdma_nodeb_name    = c.getWcdmaNodebName();
      lte_enodeb_id       = c.getLteEnodebId();
      lte_enodeb_name     = c.getLteEnodebName();
      lte_tac             = c.getLteTac();
      lte_eci             = c.getLteEci();
      region2             = c.getRegion2();
      region3             = c.getRegion3();
      region4             = c.getRegion4();
   }


   /**
    * @param cell_location_row
    *            Sets the cell location, it expects 30 fields. if a field is
    *            missing it expected to be set as \N
    */
   //revised to use new cell_location_mapper
   public Cell(String cell_location_row) {

      String[] row = cell_location_row.replaceAll("\\\\N", "NULL").split("\t");

      int rowLen = row.length; if (rowLen < 28) {

         log.error("invalid number of fields( at leat 28 fields expected): " + cell_location_row);
         return;
      }

      int n = 0; try {

         if (!row[n].equals("NULL")) location_identifier =                     row[n] ; n++;
         if (!row[n].equals("NULL")) rat                 =                     row[n] ; n++;
         if (!row[n].equals("NULL")) cell_name           =                     row[n] ; n++;
         if (!row[n].equals("NULL")) cid                 = Integer.parseInt   (row[n]); n++;
         if (!row[n].equals("NULL")) mcc                 = Integer.parseInt   (row[n]); n++;
         if (!row[n].equals("NULL")) mnc                 = Integer.parseInt   (row[n]); n++;
         if (!row[n].equals("NULL")) lat                 =  Double.parseDouble(row[n]); n++;
         if (!row[n].equals("NULL")) lon                 =  Double.parseDouble(row[n]); n++;
         if (!row[n].equals("NULL")) beam_direction      =  Double.parseDouble(row[n]); n++;
         if (!row[n].equals("NULL")) cell_type           =                     row[n] ; n++;
         if (!row[n].equals("NULL")) cell_range          = Integer.parseInt   (row[n]); n++;
         if (!row[n].equals("NULL")) tilt                =  Double.parseDouble(row[n]); n++;
         if (!row[n].equals("NULL")) height              =  Double.parseDouble(row[n]); n++;
         if (!row[n].equals("NULL")) sector              =                     row[n] ; n++;
         if (!row[n].equals("NULL")) carrier             =                     row[n] ; n++;
         if (!row[n].equals("NULL")) region1             =                     row[n] ; n++;
         if (!row[n].equals("NULL")) lac                 = Integer.parseInt   (row[n]); n++;
         if (!row[n].equals("NULL")) rac                 = Integer.parseInt   (row[n]); n++;
         if (!row[n].equals("NULL")) msc                 =                     row[n] ; n++;
         if (!row[n].equals("NULL")) gsm_bsc             =                     row[n] ; n++;
         if (!row[n].equals("NULL")) gsm_bts             =                     row[n] ; n++;
         if (!row[n].equals("NULL")) wcdma_sac           = Integer.parseInt   (row[n]); n++;
         if (!row[n].equals("NULL")) wcdma_rnc           =                     row[n] ; n++;
         if (!row[n].equals("NULL")) wcdma_nodeb_name    =                     row[n] ; n++;
         if (!row[n].equals("NULL")) lte_enodeb_id       = Integer.parseInt   (row[n]); n++;
         if (!row[n].equals("NULL")) lte_enodeb_name     =                     row[n] ; n++;
         if (!row[n].equals("NULL")) lte_tac             = Integer.parseInt   (row[n]); n++;
         if (!row[n].equals("NULL")) lte_eci             = Integer.parseInt   (row[n]); n++;
         if (!row[n].equals("NULL")) region2             =                     row[n] ; n++;
         if (!row[n].equals("NULL")) region3             =                     row[n] ; n++;
         if (!row[n].equals("NULL")) region4             =                     row[n] ; n++;
      }
      catch (NumberFormatException ex) {

         log.error("Number format excewption: ", ex);
      }
   }


//   /**
//    * query(): queries DB to obtain Cell location information.
//    * @param cellhash
//    */
//   public static void query(HashMap<String, Cell> cellhash, SQLConnection reSqlConnection) {
//
//      // clear the map before loading
//      if (cellhash != null)
//         cellhash.clear();
//      if (cellhash == null) {
//         //just in case. you can never be too safe.
//         cellhash = new HashMap<String, Cell>();
//      }
//
//      if (reSqlConnection == null) {
//         log.error("reSqlConnection is null, query returning");
//         return;
//      }
//
//      try {
//         String query = "select * from "
//               + RuleEngine.config.databaseSchema() + "." + RuleEngine.config.celllocationtablename();
//         Statement sql = reSqlConnection.getNewStatement(RuleEngine.config
//               .databaseName());
//         ResultSet resultset = sql.executeQuery(query);
//
//         while (resultset.next()) {
//            String location_identifier = resultset.getString("location_identifier");
//            String rat = resultset.getString("rat");
//            String cell_name = resultset.getString("cell_name");
//            Integer cid =  resultset.getInt("cid");
//            Integer mcc = resultset.getInt("mcc");
//            Integer mnc = resultset.getInt("mnc");
//            Double lat = resultset.getDouble("lat");
//            Double lon = resultset.getDouble("lon");
//            Double beam_direction = resultset.getDouble("beam_direction");
//            String cell_type = resultset.getString("cell_type");
//            Integer cell_range = resultset.getInt("cell_range");
//            Double tilt = resultset.getDouble("tilt");
//            Double height = resultset.getDouble("height");
//            String sector = resultset.getString("sector");
//            String carrier = resultset.getString("carrier");
//            String region1 = resultset.getString("region1");
//            Integer lac = resultset.getInt("lac");
//            Integer rac = resultset.getInt("rac");
//            String msc = resultset.getString("msc");
//            String gsm_bsc = resultset.getString("gsm_bsc");
//            String gsm_bts = resultset.getString("gsm_bts");
//            Integer wcdma_sac = resultset.getInt("wcdma_sac");
//            String wcdma_rnc = resultset.getString("wcdma_rnc");
//            String wcdma_nodeb_name = resultset
//                  .getString("wcdma_nodeb_name");
//            Integer lte_enodeb_id = resultset.getInt("lte_enodeb_id");
//            String lte_enodeb_name = resultset.getString("lte_enodeb_name");
//            Integer lte_tac = resultset.getInt("lte_tac");
//            Integer lte_eci = resultset.getInt("lte_eci");
//            String region2 = resultset.getString("region2");
//            String region3 = resultset.getString("region3");
//            String region4 = resultset.getString("region4");
//            Cell c = new Cell(location_identifier, rat, cell_name, cid, mcc, mnc, lat,
//                  lon, beam_direction, cell_type, cell_range, tilt,
//                  height, sector, carrier, region, lac, rac,
//                  msc, gsm_bsc, gsm_bts, wcdma_sac,
//                  wcdma_rnc, wcdma_nodeb_name,
//                  lte_enodeb_id, lte_enodeb_name,lte_tac, lte_eci);
//            //String key = rat + "." + cell_name;
//            String key = location_identifier;
//            cellhash.put(key, c);
//            //log.debug("query db " + c.toString());
//         }
//         resultset.close();
//         sql.getConnection().close();
//         log.info(cellhash.size() + " read Cell from table ");
//         // System.out.println(RuleEngine.cellhash +
//         // " read Cell from table ");
//      } catch (Exception e) {
//         log.error("Exception loading from table"
//               + RuleEngine.config.celllocationtablename() + " "
//               + e.getMessage());
//         // send exception to log to get stack trace in log file
//         log.error("loading error", e);
//         NGEEAlarmHelper.logAlarm(NGEEAlarmHelper.DATA_BASE_ERROR, e.getMessage());
//      }
//   }


   /**
    * @param cellColmap: Map of column vale
    */
   @Deprecated
   public Cell(Map<Dimension, Object> cellColmap) {

      super();

      Iterator<Entry<Dimension, Object>> it = cellColmap.entrySet().iterator();

      while (it.hasNext()) {

         Entry<Dimension, Object> pairs = it.next();
         Dimension                key   = (Dimension) pairs.getKey();
         Object                   value = (Object)    pairs.getValue();

         if (value == null) continue;

         String keyName = key.getName(); try {

            if      (keyName.equalsIgnoreCase("rat"                ) && (value instanceof String )) rat                 =                  (String ) value;
            else if (keyName.equalsIgnoreCase("location_identifier") && (value instanceof String )) location_identifier =                  (String ) value;
            else if (keyName.equalsIgnoreCase("cell_name"          ) && (value instanceof String )) cell_name           =                  (String ) value;
            else if (keyName.equalsIgnoreCase("cid"                ) && (value instanceof Integer)) mcc                 =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("mcc"                ) && (value instanceof Integer)) mcc                 =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("mnc"                ) && (value instanceof String )) mnc                 = Integer.parseInt((String ) value);
            else if (keyName.equalsIgnoreCase("lat"                ) && (value instanceof Double )) lat                 =                  (Double ) value;
            else if (keyName.equalsIgnoreCase("lon"                ) && (value instanceof Double )) lon                 =                  (Double ) value;
            else if (keyName.equalsIgnoreCase("beam_direction"     ) && (value instanceof Double )) beam_direction      =                  (Double ) value;
            else if (keyName.equalsIgnoreCase("cell_type"          ) && (value instanceof String )) cell_type           =                  (String ) value;
            else if (keyName.equalsIgnoreCase("cell_range"         ) && (value instanceof Integer)) cell_range          =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("tilt"               ) && (value instanceof Double )) tilt                =                  (Double ) value;
            else if (keyName.equalsIgnoreCase("height"             ) && (value instanceof Double )) height              =                  (Double ) value;
            else if (keyName.equalsIgnoreCase("sector"             ) && (value instanceof String )) sector              =                  (String ) value;
            else if (keyName.equalsIgnoreCase("carrier"            ) && (value instanceof String )) carrier             =                  (String ) value;
            else if (keyName.equalsIgnoreCase("region1"            ) && (value instanceof String )) region1             =                  (String ) value;
            else if (keyName.equalsIgnoreCase("lac"                ) && (value instanceof Integer)) lac                 =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("rac"                ) && (value instanceof Integer)) rac                 =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("msc"                ) && (value instanceof String )) msc                 =                  (String ) value;
            else if (keyName.equalsIgnoreCase("gsm_bsc"            ) && (value instanceof String )) gsm_bsc             =                  (String ) value;
            else if (keyName.equalsIgnoreCase("gsm_bts"            ) && (value instanceof String )) gsm_bts             =                  (String ) value;
            else if (keyName.equalsIgnoreCase("wcdma_sac"          ) && (value instanceof Integer)) wcdma_sac           =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("wcdma_rnc"          ) && (value instanceof String )) wcdma_rnc           =                  (String ) value;
            else if (keyName.equalsIgnoreCase("wcdma_nodeb_name"   ) && (value instanceof String )) wcdma_nodeb_name    =                  (String ) value;
            else if (keyName.equalsIgnoreCase("lte_enodeb_id"      ) && (value instanceof Integer)) lte_enodeb_id       =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("lte_enodeb_name"    ) && (value instanceof String )) lte_enodeb_name     =                  (String ) value;
            else if (keyName.equalsIgnoreCase("lte_tac"            ) && (value instanceof Integer)) lte_tac             =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("lte_eci"            ) && (value instanceof Integer)) lte_eci             =                  (Integer) value;
            else if (keyName.equalsIgnoreCase("region2"            ) && (value instanceof String )) region2             =                  (String ) value;
            else if (keyName.equalsIgnoreCase("region3"            ) && (value instanceof String )) region3             =                  (String ) value;
            else if (keyName.equalsIgnoreCase("region4"            ) && (value instanceof String )) region4             =                  (String ) value;
         }
         catch (NumberFormatException e) {

            log.error(key.getName() + ": " + e.getMessage(), e);
         }
      }
   }


   /**
    * @param fn: file name
    * @param cellhash: hash table to load hold cell information key by cell_name
    * @throws IOException
    */
   public static void load(String fn, HashMap<String, Cell> cellhash) throws IOException {

      FileReader     fr     = new FileReader(fn);
      BufferedReader reader = new BufferedReader(fr);

      String line = null; while ((line = reader.readLine()) != null) {

         if (!line.startsWith("#")) {

            Cell c = new Cell(line);
            //String key = c.getRat() + "." + c.getCellname();
            String key = c.getLocation_identifier();
            if (key == null || (c.getLat() == null || c.getLon() == null)) continue;

            cellhash.put(key, c);
         }
      }

      fr.close();
      reader.close();
   }


   /**
    * @return true if every item in cell is null
    */
   public boolean isEmpty(){

      boolean rc = rat                 == null
         &&        location_identifier == null
         &&        cell_name           == null
         &&        cid                 == null
         &&        mcc                 == null
         &&        mnc                 == null
         &&        lat                 == null
         &&        lon                 == null
         &&        beam_direction      == null
         &&        cell_type           == null
         &&        cell_range          == null
         &&        tilt                == null
         &&        height              == null
         &&        sector              == null
         &&        carrier             == null
         &&        region1             == null
         &&        lac                 == null
         &&        rac                 == null
         &&        msc                 == null
         &&        gsm_bsc             == null
         &&        gsm_bts             == null
         &&        wcdma_sac           == null
         &&        wcdma_rnc           == null
         &&        wcdma_nodeb_name    == null
         &&        lte_enodeb_id       == null
         &&        lte_enodeb_name     == null
         &&        lte_tac             == null
         &&        lte_eci             == null
         &&        region2             == null
         &&        region3             == null
         &&        region4             == null;

      return rc;
   }
}
