package com.ericsson.eea.ark.offline.rules.esr;

import lombok.Getter;

import java.util.List;
import java.util.ArrayList;

import com.ericsson.bigdata.esr.dataexchange.Trigger;


// This is just a wrapper class to get around Drool "functions" deficiency where you can't pass parameterized list.
public class EsrTriggers {

   @Getter private List<Trigger> list = new ArrayList<Trigger>();

    public void add(Trigger t) {

       list.add(t);
    }
}
