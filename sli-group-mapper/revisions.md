# Project Information - Revisions

## Versions

### **Latest**

### **1.40 (2017.08.15)**
[EEAEPP-1997] Bug fixes for SLI group mapper

### **1.39 (2017.06.19)**
Group Mapper to Support SNCD data.

### **1.38 (2017.05.25)**
Driver should run in only 1 container.

### **1.37 (2017.05.10)**
fix upgrade to not lose custom configs.
but do upgrade 1.34 group-mapper.conf
to new conventions.

### **1.36 (2017.05.04)**
Change in group-mapper config file and rules

### **1.35 (2017.04.27)**
Support Imeitac data for group mapper

### **1.34 (2017.01.25.)**
Support fake long-to-long encryptor used for unit testing.

### **1.33 (2016.01.12.)**
Added the app_custom_parameters.conf file to the init directory in the app suite zip files.
This will allow the app_custom_parameters.conf file to be removed when the rpm is uninstalled.

### **1.32 (2016.10.12.)**
* No longer depend on cea_product repository.
  The needed classes were moved into this project.
  HBase, hazelcast, old esr encryption is obsolete.