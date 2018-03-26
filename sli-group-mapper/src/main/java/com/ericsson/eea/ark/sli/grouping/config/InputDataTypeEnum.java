package com.ericsson.eea.ark.sli.grouping.config;

import java.util.HashMap;

import com.ericsson.eea.ark.sli.grouping.services.hadoop.GroupMapperMapper;
import com.ericsson.eea.ark.sli.grouping.services.hadoop.GroupMapperSncdMapper;

public enum InputDataTypeEnum {

    dpa("dpa", GroupMapperMapper.class,"baseInputDir"), sncd("sncd", GroupMapperSncdMapper.class,"sncdInputDir");

    /**
     * Mapper Input data type passed from console
     */
    String mapperName;
    /**
     *
     */
    Class mapperClass;
    String inputDir;

    static HashMap<String, InputDataTypeEnum> inputDataTypeByName = new HashMap<>(InputDataTypeEnum.values().length);
    static {
        for (InputDataTypeEnum inDataType : InputDataTypeEnum.values()) {
            inputDataTypeByName.put(inDataType.mapperName, inDataType);
        }

    }
//
    private InputDataTypeEnum(String mapperName, Class mapperClass,String inputDir) {
        this.mapperClass = mapperClass;
        this.mapperName = mapperName;
        this.inputDir=inputDir;
    }

    public String getMapperName() {
        return mapperName;
    }

    public void setMapperName(String mapperName) {
        this.mapperName = mapperName;
    }

    public Class getMapperClass() {
        return mapperClass;
    }

    public void setMapperClass(Class mapperClass) {
        this.mapperClass = mapperClass;
    }

    public String getInputDir() {
        return inputDir;
    }

    public void setInputDir(String inputDir) {
        this.inputDir = inputDir;
    }

    public static  InputDataTypeEnum getInputDataTypeByName(String inputDataType) {
        return inputDataTypeByName.get(inputDataType);
    }

    public static void setInputDataTypeByName(HashMap<String, InputDataTypeEnum> inputDataTypeByName) {
        InputDataTypeEnum.inputDataTypeByName = inputDataTypeByName;
    }
}