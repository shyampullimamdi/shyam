package com.ericsson.eea.ark.sli.grouping.rules.events;

import lombok.Getter;
import lombok.ToString;
import lombok.AllArgsConstructor;



/**
 * Defines event interface as an object with a time-stamp.
 */
@ToString
@AllArgsConstructor
public class Event<T> {

    @Getter private final long timestamp;
    @Getter private final T    event;
}
