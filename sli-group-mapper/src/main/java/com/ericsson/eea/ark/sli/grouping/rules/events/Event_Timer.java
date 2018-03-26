package com.ericsson.eea.ark.sli.grouping.rules.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.bigdata.common.identifier.CellIdentifier;

/**
 * @author ejoepao EventType_Timer: this class it is used to create Timer object
 *         that prevent Incidents from firing to frequent
 */
public class Event_Timer {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory
    .getLogger(Event_Timer.class.getName());

    private final String blockingcat;
    private final String ruleid;
    private final Long textid;
    private final long ts;
    private final CellIdentifier cellIdentifier;
    private final String dimensions;

    public Event_Timer(String blockingcat, String ruleid, Long textid,
            CellIdentifier cellID, long ts, String dimensions) {

        this.blockingcat = blockingcat;
        this.ruleid = ruleid;
        this.ts = ts;
        this.textid = textid;
        this.cellIdentifier = cellID;
        this.dimensions= dimensions;
    }

    @Override
    public String toString() {
        return "EventType_Timer [blockingcat=" + blockingcat + ", ruleid="
                + ruleid + ", textid=" + textid + ", ts=" + ts
                + ", cellIdentifier=" + cellIdentifier + ", dimensions="
                + dimensions + "]";
    }

    public String getDimensions() {
        return dimensions;
    }

    public String getBlockingcat() {
        return blockingcat;
    }

    public String getRuleid() {
        return ruleid;
    }

    public Long getTextid() {
        return textid;
    }

    public CellIdentifier getCellIdentifier() {
        return cellIdentifier;
    }

    public long getTs() {
        return ts;
    }
}
