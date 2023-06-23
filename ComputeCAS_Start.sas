*********************************************************;
* Demo: Summarizing Data and Benchmarking with SAS      *;
*       Compute Server, CAS-enabled Steps and CASL.     *;
*********************************************************;

/************************************************************/
/* Section 1: SAS Program Executed on the Compute Server    */
/************************************************************/

libname mydata "s:\workshop";

data mydata.orders_clean;
	set mydata.orders;
    Name=catx(' ',
              scan(Customer_Name,2,','),
              scan(Customer_Name,1,','));
run;

title "Compute Server Program";

proc contents data=mydata.orders;
run;

proc freq data=mydata.orders;
    tables Country OrderType;
run;

proc means data=mydata.orders;
    var RetailPrice;
    output out=mydata.orders_sum;
run;

title;


/*********************************************************************/
/* Section 2: SAS Program Executed CAS Server with CAS-enabled Steps */
/*     Link to CAS-enabled Procedure Documentation:                  */
/*     https://go.documentation.sas.com/doc/en/pgmsascdc/v_040/procs2actions/p0275qj00ns5pen16ijvuz8f8j5k.htm */
/*********************************************************************/

cas mysession;

* Load SAS table via Compute server;

libname mydata "/home/student/S23HMODV";
proc casutil;
    load data=mydata.orders casout="orders" outcaslib="casuser" replace;
quit;

* Load SASHDAT file directly via CAS server;

caslib mycas path="/home/student/S23HMODV";
proc casutil;
    load casdata="orders.sashdat" incaslib="mycas" casout="orders" outcaslib="casuser" replace;
quit;

* Modify and load SAS table via DATA step;
libname casuser cas caslib=casuser;

data casuser.orders_clean;
	set casuser.orders;
    Name=catx(' ',
              scan(Customer_Name,2,','),
              scan(Customer_Name,1,','));
run;

title "CAS-Enabled Program";

proc contents data=casuser.orders;
run;

proc freqtab data=casuser.orders;
    tables Country OrderType;
run;

proc mdsummary data=casuser.orders;
    var RetailPrice;
    output out=casuser.orders_sum;
run;

title;

cas mySession terminate;


/************************************************************/
/* Section 3: SAS Program Executed on CAS Server with CASL */
/************************************************************/

cas mySession;

title "CASL Program";
proc cas;
  * Create dictionary to reference orders table in Casuser;
    tbl={name='orders', caslib='casuser'};

  * Create CASL variable named DS to store DATA step code. Both 
      input and output tables must be in-memory;
    source ds;
        data casuser.orders_clean;
	        set casuser.orders;
            Name=catx(' ',
                 scan(Customer_Name,2,','),
                 scan(Customer_Name,1,','));
        run;
    endsource;

  * Drop orders from casuser if it exists;
    table.dropTable / name="orders", 
                      caslib="casuser", 
                      quiet=true;

  * Define caslib pointing to workship files and load orders.sashdat to casuser;
   table.addCaslib / 
         name="mycas",
         path="/home/student/S23HMODV";

    table.loadTable / 
        path="orders.sashdat", caslib="mycas", 
        casOut={name="orders", caslib="casuser", replace=true};

  * Execute DATA step code;
    dataStep.runCode / code=ds;

  * List orders column attributes, similar to PROC CONTENTS;
    table.columnInfo / 
        table=tbl;

  * Generate frequency report, similar to PROC FREQ;
    simple.freq / 
        table=tbl, 
        inputs={'Country', 'OrderType'};

  * Generate summary table, similar to PROC MEANS;
    simple.summary / 
        table=tbl, 
        input={'RetailPrice'}, 
        casOut={name='orders_sum', replace=true};
quit;
title;

cas mySession terminate;