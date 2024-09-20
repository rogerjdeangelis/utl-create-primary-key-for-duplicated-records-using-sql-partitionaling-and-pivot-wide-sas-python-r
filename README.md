# utl-create-primary-key-for-duplicated-records-using-sql-partitionaling-and-pivot-wide-sas-python-r
Create primary key for duplicated records using sql partitionaling and pivot wide sas python r
    %let pgm=utl-create-primary-key-for-duplicated-records-using-sql-partitionaling-and-pivot-wide-sas-python-r;

    Create primary key for duplicated records using sql partitionaling and pivot wide sas python r

    github
    https://tinyurl.com/3khm73b2
    https://github.com/rogerjdeangelis/utl-create-primary-key-for-duplicated-records-using-sql-partitionaling-and-pivot-wide-sas-python-r

    Duplicates cause all kinds of problems. Here ia way to deal with them
    in normalized(skinny) and non-normalized(transpose-fat) data structures using just sql.

           SOLUTIONS (all sql)

               1 sas primary key
               2 sas hash primary key
               3 r primary key
               4 python primary key
               5 sas pivot wide
               6 r pivot wide
               7 python pivot wide
               8 related repos

    SQL partitioning has many applications, especially when pivoting tables and first x last x.

    stackoverflow r
    https://tinyurl.com/bd9fredh
    https://stackoverflow.com/questions/79002983/grouping-data-and-changing-parts-of-the-group

    /*               _     _
     _ __  _ __ ___ | |__ | | ___ _ __ ___
    | `_ \| `__/ _ \| `_ \| |/ _ \ `_ ` _ \
    | |_) | | | (_) | |_) | |  __/ | | | | |
    | .__/|_|  \___/|_.__/|_|\___|_| |_| |_|
    |_|
    */


    /**************************************************************************************************************************/
    /*                                   |                                                                                    */
    /* TWO PARTS                         |                                                                                    */
    /*                                   |                                                                                    */
    /* 1. ADD PRIMARY KEY TO DUP GROUPS  |                                                                                    */
    /* ================================  |                                                                                    */
    /*                                   |                                                                                    */
    /*           INPUT                   |                OUTPUT                                                              */
    /*                                   |                                                                                    */
    /*   SD1.HAVE total obs=7            |  Obs     NAM     SCORE    PARTITION                                                */
    /*                                   |                                                                                    */
    /*    Obs     NAM     SCORE          |   1     dave      128         1                                                    */
    /*                                   |   2     dave      128         2                                                    */
    /*     1     dave      128           |                                                                                    */
    /*     2     john      123           |   3     harry     130         1                                                    */
    /*     3     steve     111           |   4     harry     130         2                                                    */
    /*     4     dave      128           |                                                                                    */
    /*     5     harry     130           |   5     john      123         1                                                    */
    /*     6     harry     130           |   6     steve     111         1                                                    */
    /*     7     will      110           |   7     will      110         1                                                    */
    /*                                   |                                                                                    */
    /*                                   |                                                                                    */
    /*------------------------------------------------------------------------------------------------------------------------*/
    /*                                   |                                                                                    */
    /* 2. PIVOT WIDER                    |     NAM     SCORE1    SCORE2                                                       */
    /* ==============                    |                                                                                    */
    /*                                   |    dave       128       128                                                        */
    /*                                   |    harry      130       130                                                        */
    /*                                   |    john       123         .                                                        */
    /*                                   |    steve      111         .                                                        */
    /*                                   |    will       110         .                                                        */
    /*                                   |                                                                                    */
    /**************************************************************************************************************************/


    /*   _                   _
    / | (_)_ __  _ __  _   _| |_
    | | | | `_ \| `_ \| | | | __|
    | | | | | | | |_) | |_| | |_
    |_| |_|_| |_| .__/ \__,_|\__|
                |_|
    */
    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.have;
      input nam$ score;
    cards4;
    dave    128
    john    123
    steve   111
    dave    128
    harry   130
    harry   130
    will    110
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* SD1.HAVE total obs=7                                                                                                   */
    /*                                                                                                                        */
    /*  Obs     NAM     SCORE                                                                                                 */
    /*                                                                                                                        */
    /*   1     dave      128                                                                                                  */
    /*   2     john      123                                                                                                  */
    /*   3     steve     111                                                                                                  */
    /*   4     dave      128                                                                                                  */
    /*   5     harry     130                                                                                                  */
    /*   6     harry     130                                                                                                  */
    /*   7     will      110                                                                                                  */
    /*                                                                                                                        */
    /**************************************************************************************************************************/
    /*                              _                              _
    / |  ___  __ _ ___   _ __  _ __(_ )_ __ ___   __ _ _ __ _   _  | | _____ _   _
    | | / __|/ _` / __| | `_ \| `__| | `_ ` _ \ / _` | `__| | | | | |/ / _ \ | | |
    | | \__ \ (_| \__ \ | |_) | |  | | | | | | | (_| | |  | |_| | |   <  __/ |_| |
    |_| |___/\__,_|___/ | .__/|_|  |_|_| |_| |_|\__,_|_|   \__, | |_|\_\___|\__, |
                        |_|                                |___/            |___/
    */

    proc sql;
      create
         table want as
      select
         nam
        ,score
        ,partition
      from
        %sqlpartition(sd1.have,by=nam)
    ;quit;


    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  Obs     NAM     SCORE    PARTITION                                                                                    */
    /*                                                                                                                        */
    /*   1     dave      128         1                                                                                        */
    /*   2     dave      128         2                                                                                        */
    /*                                                                                                                        */
    /*   3     harry     130         1                                                                                        */
    /*   4     harry     130         2                                                                                        */
    /*                                                                                                                        */
    /*   5     john      123         1                                                                                        */
    /*   6     steve     111         1                                                                                        */
    /*   7     will      110         1                                                                                        */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                    _               _
    |___ \   ___  __ _ ___  | |__   __ _ ___| |__
      __) | / __|/ _` / __| | `_ \ / _` / __| `_ \
     / __/  \__ \ (_| \__ \ | | | | (_| \__ \ | | |
    |_____| |___/\__,_|___/ |_| |_|\__,_|___/_| |_|

    */

    * no sort needed */

    data want;

      set sd1.have;

      if _n_ = 1 then do;
        declare hash h();
        h.definekey('nam');
        h.definedata('nam','score');
        h.definedone();
      end;

      rc = h.find();
      if rc ne 0 then count = 0;
      count + 1;
      rc = h.replace();

      seq_no = count;

      output;
    run;

    proc print data=want;
    run;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*    NAM     SCORE    RC    COUNT    SEQ_NO                                                                              */
    /*                                                                                                                        */
    /*   dave      128      0      1         1                                                                                */
    /*   john      123      0      1         1                                                                                */
    /*   steve     111      0      1         1                                                                                */
    /*   dave      128      0      2         2                                                                                */
    /*   harry     130      0      1         1                                                                                */
    /*   harry     130      0      2         2                                                                                */
    /*   will      110      0      1         1                                                                                */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____                     _                              _
    |___ /   _ __   _ __  _ __(_)_ __ ___   __ _ _ __ _   _  | | _____ _   _
      |_ \  | `__| | `_ \| `__| | `_ ` _ \ / _` | `__| | | | | |/ / _ \ | | |
     ___) | | |    | |_) | |  | | | | | | | (_| | |  | |_| | |   <  __/ |_| |
    |____/  |_|    | .__/|_|  |_|_| |_| |_|\__,_|_|   \__, | |_|\_\___|\__, |
                   |_|                                |___/            |___/
    */

    %utl_rbeginx;
    parmcards4;
    library(haven)
    library(sqldf)
    source("c:/oto/fn_tosas9x.R")
    have<-read_sas("d:/sd1/have.sas7bdat")
    print(have)
    want<-sqldf('
       select
         *
       from
         ( select nam, score, row_number() OVER (PARTITION BY nam) as partition from have )
       ')
    want
    fn_tosas9x(
          inp    = want
         ,outlib ="d:/sd1/"
         ,outdsn ="rwant"
         )
    ;;;;
    %utl_r endx;

    libname sd1 "d:/sd1";
    proc print data=sd1.rwant;
    run;quit;

    /**************************************************************************************************************************/
    /*                             |                                                                                          */
    /*   R                         |               SAS                                                                        */
    /*                             |                                                                                          */
    /*   > want                    |                                                                                          */
    /*       nam score partition   |  ROWNAMES     NAM     SCORE    PARTITION                                                 */
    /*                             |                                                                                          */
    /*   1  dave   128         1   |      1       dave      128         1                                                     */
    /*   2  dave   128         2   |      2       dave      128         2                                                     */
    /*   3 harry   130         1   |      3       harry     130         1                                                     */
    /*   4 harry   130         2   |      4       harry     130         2                                                     */
    /*   5  john   123         1   |      5       john      123         1                                                     */
    /*   6 steve   111         1   |      6       steve     111         1                                                     */
    /*   7  will   110         1   |      7       will      110         1                                                     */
    /*                             |                                                                                          */
    /**************************************************************************************************************************/

    * save all this stupp in c:/oto/fn_python.py;

    filename ft15f001 "c:/oto/fn_python.py";
    parmcards4;
    import pyarrow.feather as feather;
    import tempfile;
    import pyperclip;
    import os;
    import sys;
    import subprocess;
    import time;
    import pandas as pd;
    import pyreadstat as ps;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    exec(open('c:/oto/fn_tosas9x.py').read());
    ;;;;
    run;quit;


    %utl_pybeginx;
    parmcards4;
    exec(open('c:/oto/fn_python.py').read())
    have, meta = ps.read_sas7bdat('d:/sd1/have.sas7bdat')
    print(have);
    want=pdsql('''
       select
         *
       from
         ( select nam, score, row_number() OVER (PARTITION BY nam) as partition from have )
       ''')
    print(want);
    fn_tosas9x(want,outlib='d:/sd1/',outdsn='pywant',timeest=3)
    ;;;;
    %utl_pyendx;

    proc print data=sd1.pywant;
    run;quit;

    /**************************************************************************************************************************/
    /*                             |                                                                                          */
    /*                             |                                                                                          */
    /*  PYTHON                     |   SAS                                                                                    */
    /*                             |                                                                                          */
    /*       nam  score  partition |    NAM     SCORE    PARTITION                                                            */
    /*                             |                                                                                          */
    /*  0   dave  128.0          1 |   dave      128         1                                                                */
    /*  1   dave  128.0          2 |   dave      128         2                                                                */
    /*  2  harry  130.0          1 |   harry     130         1                                                                */
    /*  3  harry  130.0          2 |   harry     130         2                                                                */
    /*  4   john  123.0          1 |   john      123         1                                                                */
    /*  5  steve  111.0          1 |   steve     111         1                                                                */
    /*  6   will  110.0          1 |   will      110         1                                                                */
    /*                             |                                                                                          */
    /**************************************************************************************************************************/

    /*___                          _            _              _     _
    | ___|   ___  __ _ ___   _ __ (_)_   _____ | |_  __      _(_) __| | ___
    |___ \  / __|/ _` / __| | `_ \| \ \ / / _ \| __| \ \ /\ / / |/ _` |/ _ \
     ___) | \__ \ (_| \__ \ | |_) | |\ V / (_) | |_   \ V  V /| | (_| |  __/
    |____/  |___/\__,_|___/ | .__/|_| \_/ \___/ \__|   \_/\_/ |_|\__,_|\___|
                            |_|
    */


    proc sql;
      create
         table seq as
      select
         nam
        ,max(case when partition=1 then score else . end) as score1
        ,max(case when partition=2 then score else . end) as score2
      from
        %sqlpartition(sd1.have,by=nam)
      group
        by nam
    ;quit;

    /*         _
     ___  __ _| |   __ _ _ __ _ __ __ _ _   _ ___
    / __|/ _` | |  / _` | `__| `__/ _` | | | / __|
    \__ \ (_| | | | (_| | |  | | | (_| | |_| \__ \
    |___/\__, |_|  \__,_|_|  |_|  \__,_|\__, |___/
            |_|                         |___/
    */

    %array(_ary,values=1-2);

    proc sql;
      create
         table seq as
      select
         nam
        ,%do_over(_ary,phrase=
            max(case when partition=? then score else . end) as score?, between=comma
         )
      from
        %sqlpartition(sd1.have,by=nam)
      group
        by nam
    ;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  WORK SEQ total obs=5                                                                                                  */
    /*                                                                                                                        */
    /*  Obs     NAM     SCORE1    SCORE2                                                                                      */
    /*                                                                                                                        */
    /*   1     dave       128       128                                                                                       */
    /*   2     harry      130       130                                                                                       */
    /*   3     john       123         .                                                                                       */
    /*   4     steve      111         .                                                                                       */
    /*   5     will       110         .                                                                                       */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*__                  _            _              _     _
     / /_    _ __   _ __ (_)_   _____ | |_  __      _(_) __| | ___
    | `_ \  | `__| | `_ \| \ \ / / _ \| __| \ \ /\ / / |/ _` |/ _ \
    | (_) | | |    | |_) | |\ V / (_) | |_   \ V  V /| | (_| |  __/
     \___/  |_|    | .__/|_| \_/ \___/ \__|   \_/\_/ |_|\__,_|\___|
                   |_|
    */

    %utl_rbeginx;
    parmcards4;
    library(haven)
    library(sqldf)
    source("c:/oto/fn_tosas9x.R")
    have<-read_sas("d:/sd1/have.sas7bdat")
    print(have)
    want<-sqldf('
       select
          nam
         ,max(case when partition=1 then score else NULL end) as score1
         ,max(case when partition=2 then score else NULL end) as score2
       from
         ( select nam, score, row_number() OVER (PARTITION BY nam) as partition from have )
       group
         by nam
       ')
    want
    fn_tosas9x(
          inp    = want
         ,outlib ="d:/sd1/"
         ,outdsn ="rwant"
         )
    ;;;;
    %utl_rendx;

    proc print data=sd1.rwant;
    run;quit;

    /**************************************************************************************************************************/
    /*                          |                                                                                             */
    /*  > want                  |                                                                                             */
    /*      nam score1 score2   |  ROWNAMES     NAM     SCORE1    SCORE2                                                      */
    /*                          |                                                                                             */
    /*  1  dave    128    128   |      1       dave       128       128                                                       */
    /*  2 harry    130    130   |      2       harry      130       130                                                       */
    /*  3  john    123     NA   |      3       john       123         .                                                       */
    /*  4 steve    111     NA   |      4       steve      111         .                                                       */
    /*  5  will    110     NA   |      5       will       110         .                                                       */
    /*                          |                                                                                             */
    /**************************************************************************************************************************/

    /*____               _   _                         _            _              _     _
    |___  |  _ __  _   _| |_| |__   ___  _ __    _ __ (_)_   _____ | |_  __      _(_) __| | ___
       / /  | `_ \| | | | __| `_ \ / _ \| `_ \  | `_ \| \ \ / / _ \| __| \ \ /\ / / |/ _` |/ _ \
      / /   | |_) | |_| | |_| | | | (_) | | | | | |_) | |\ V / (_) | |_   \ V  V /| | (_| |  __/
     /_/    | .__/ \__, |\__|_| |_|\___/|_| |_| | .__/|_| \_/ \___/ \__|   \_/\_/ |_|\__,_|\___|
            |_|    |___/                        |_|
    */


    %utl_pybeginx;
    parmcards4;
    exec(open('c:/oto/fn_python.py').read())
    have, meta = ps.read_sas7bdat('d:/sd1/have.sas7bdat')
    print(have);
    want=pdsql('''
       select
          nam
         ,max(case when partition=1 then score else NULL end) as score1
         ,max(case when partition=2 then score else NULL end) as score2
       from
         ( select nam, score, row_number() OVER (PARTITION BY nam) as partition from have )
       group
         by nam
       ''')
    print(want);
    fn_tosas9x(want,outlib='d:/sd1/',outdsn='pywant',timeest=3)
    ;;;;
    %utl_pyendx;

    proc print data=sd1.pywant;
    run;quit;

    /**************************************************************************************************************************/
    /*                             |                                                                                          */
    /* PYTHON                      |    SAS                                                                                   */
    /*                             |                                                                                          */
    /*       nam  score1  score2   |    NAM     SCORE1    SCORE2                                                              */
    /*                             |                                                                                          */
    /*  0   dave   128.0   128.0   |   dave       128       128                                                               */
    /*  1  harry   130.0   130.0   |   harry      130       130                                                               */
    /*  2   john   123.0     NaN   |   john       123         .                                                               */
    /*  3  steve   111.0     NaN   |   steve      111         .                                                               */
    /*  4   will   110.0     NaN   |   will       110         .                                                               */
    /*                             |                                                                                          */
    /**************************************************************************************************************************/

    /*___
     ( _ )   _ __ ___ _ __   ___  ___
     / _ \  | `__/ _ \ `_ \ / _ \/ __|
    | (_) | | | |  __/ |_) | (_) \__ \
     \___/  |_|  \___| .__/ \___/|___/
                     |_|
    */

    https://github.com/rogerjdeangelis/utl-adding-sequence-numbers-and-partitions-in-SAS-sql-without-using-monotonic
    https://github.com/rogerjdeangelis/utl-find-first-n-observations-per-category-using-proc-sql-partitioning
    https://github.com/rogerjdeangelis/utl-macro-to-enable-sql-partitioning-by-groups-montonic-first-and-last-dot
    https://github.com/rogerjdeangelis/utl-pivot-long-pivot-wide-transpose-partitioning-sql-arrays-wps-r-python
    https://github.com/rogerjdeangelis/utl-pivot-transpose-by-id-using-wps-r-python-sql-using-partitioning
    https://github.com/rogerjdeangelis/utl-top-four-seasonal-precipitation-totals--european-cities-sql-partitions-in-wps-r-python
    https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
    https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning
    https://github.com/rogerjdeangelis/utl-transposing-words-into-sentences-using-sql-partitioning-in-r-and-python
    https://github.com/rogerjdeangelis/utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students-partitioning

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
