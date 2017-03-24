# Compare Base Recoveries

This utility compares the Estimated Number, Fishery Name, and Tag Code for recoveries in the `CWDBRecovery` table between two CAS databases.

> **Note to DFO Users**: You will need to run the `CreateDFOREnvironFile.cmd` file in the `bin` folder to setup an `.Renviron` file.  This will fix an issue with security policy of DFO machines that command line scripts can not write to the standard windows temporary folder.  

To run the utility, just double click the `CompareBaseRecoveries.cmd` command line script in the `PSC-CAS-R` folder.  

1. The utility should prompt you to select a file, which should be an `.mdb` Access database file.  
2. After that selection, the utility will ask you to select a second `.mdb` Access database file.
3. The utility will output the file comparison to the `report` directory

*This help is a work in progress*
