package com.ericsson.eea.ark.offline.rules.incidents;

import java.io.Serializable;
import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import com.ericsson.eea.ark.offline.rules.utils.Util;


@EqualsAndHashCode
@NoArgsConstructor
@SuppressWarnings("unused")
@ToString(exclude={"data"})
public class IncidentReports implements Serializable {

   private static final long   serialVersionUID = 1L;
   private static final Logger log              = LoggerFactory.getLogger(IncidentReports.class.getName());

   @Getter @Setter private BigInteger incidentId;
   @Getter @Setter private Double     start;
   @Getter @Setter private Long       imsi               = null;
   @Getter @Setter private Long       msisdn             = null;
   @Getter @Setter private String     cellname           = null;
   @Getter @Setter private Integer    imeitac            = null;
   @Getter @Setter private String     usercategory       = null;
   @Getter @Setter private Integer    incidentscore      = null;
   @Getter @Setter private String     ruleid             = null;
   @Getter @Setter private String     rulecat            = null;
   @Getter @Setter private String     rulename           = null;
   @Getter @Setter private String     impact             = null;
   @Getter @Setter private String     reason             = null;
   @Getter @Setter private String     messageSoc         = null;
   @Getter @Setter private String     messageCc          = null;
   @Getter @Setter private String     messageCclite      = null;
   @Getter @Setter private String     apn                = null;
   @Getter @Setter private String     rat                = null;
   @Getter @Setter private String     kpiName            = null;
   @Getter @Setter private String     release            = null;
   @Getter @Setter private String     version            = null;
   @Getter @Setter private String     filename           = null;
   @Getter @Setter private String     sgsn               = null;
   @Getter @Setter private Integer    blocking_secs      = null;
   @Getter @Setter private Integer    partition_id       = null;
   @Getter @Setter private Long       incident_id        = null;
   @Getter @Setter private Boolean    incident_published = null;
   @Getter @Setter private String     dimensions         = null;
   @Getter @Setter private String     kpi_source         = null;
   @Getter @Setter private String     service            = null;
   @Getter @Setter private String     arim               = null;
   @Getter @Setter private String     next_best_action   = null;
   @Getter @Setter private Integer    origin;
   @Getter @Setter private Boolean    hidden;

   @Getter @Setter private String     data               = null; // encrypted ESR.


//   public IncidentReports(EventType_KeyIncident incident) {
//
//      imsi               = incident.getImsi();
//      ruleid             = incident.getRuleid();
//      cellname           = incident.getCellname();
//      rat                = incident.getRat();
//      imeitac            = incident.getTac();
//      impact             = incident.getImpact();
//      reason             = incident.getReason();
//      rulecat            = incident.getRule_cat();
//      start              = incident.getStart();
//      origin             = incident.getOrigin();
//      partition_id       = incident.getPartition_id();
//      messageSoc         = incident.getMessageSoc();
//      messageCc          = incident.getMessageCc();
//      messageCclite      = incident.getMessageCclite();
//      incidentscore      = incident.getIncidentscore();
//      rulename           = incident.getRulename();
//      kpiName            = incident.getKpi_name();
//      msisdn             = incident.getMsisdn();
//      usercategory       = incident.getUsercategory();
//      apn                = incident.getApn();
//      release            = incident.getRelease();
//      version            = incident.getVersion();
//      filename           = incident.getFilename();
//      dimensions         = incident.getDimensions();
//      sgsn               = incident.getSgsn();
//      blocking_secs      = incident.getBlocking_secs();
//      incident_published = incident.getIncident_published();
//      hidden             = incident.getHidden();
//      kpi_source         = incident.getKpi_source();
//      service            = incident.getService();
//      arim               = incident.getArim();
//      next_best_action   = incident.getNext_best_action();
//
//      data               = null;  // TODO: get it from the received incident
//
//      try {
//
//         incident_id     = (incident.getKey() == null) ? null : Long.parseLong(incident.getKey().toString());
//      }
//      catch (NumberFormatException ex) {
//
//         log.error("Cannot parse Incident ID: ", ex);
//      }
//   }


   public StringBuffer getCopyMgrReport(boolean appendEOL) {

      StringBuffer buffer = new StringBuffer(); {

         buffer.append(Util.csvColumn(start             ))
            .   append(Util.csvColumn(imsi              ))
            .   append(Util.csvColumn(msisdn            ))
            .   append(Util.csvColumn(cellname          ))
            .   append(Util.csvColumn(imeitac           ))
            .   append(Util.csvColumn(usercategory      ))
            .   append(Util.csvColumn(incidentscore     ))
            .   append(Util.csvColumn(ruleid            ))
            .   append(Util.csvColumn(rulecat           ))
            .   append(Util.csvColumn(rulename          ))
            .   append(Util.csvColumn(impact            ))
            .   append(Util.csvColumn(reason            ))
            .   append(Util.csvColumn(messageSoc        ))
            .   append(Util.csvColumn(messageCc         ))
            .   append(Util.csvColumn(messageCclite     ))
            .   append(Util.csvColumn(apn               ))
            .   append(Util.csvColumn(rat               ))
            .   append(Util.csvColumn(kpiName           ))
            .   append(Util.csvColumn(release           ))
            .   append(Util.csvColumn(version           ))
            .   append(Util.csvColumn(filename          ))
            .   append(Util.csvColumn(sgsn              ))
            .   append(Util.csvColumn(blocking_secs     ))
            .   append(Util.csvColumn(partition_id      ))
            .   append(Util.csvColumn(origin            ))
            .   append(Util.csvColumn(null              )) // incident_id So the ORE ignores it.
            .   append(Util.csvColumn(incident_published))
            .   append(Util.csvColumn(new Boolean(false))) // So the GUI sees it.
            .   append(Util.csvColumn(dimensions        ))
            .   append(Util.csvColumn(kpi_source        ))
            .   append(Util.csvColumn(service           ))
            .   append(Util.csvColumn(arim              ))
            .   append(Util.csvColumn(next_best_action  ))
            .   append(Util.csvColumn(data              , /*no separator*/false));

         if (appendEOL) buffer.append("\n");
      }

      return buffer;
   }
}
