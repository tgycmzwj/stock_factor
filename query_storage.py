class query_storage:
    query_bank={

        "utils":{
            "delete_table":"DROP TABLE IF EXISTS {table_name};",
            "delete_column":"ALTER TABLE {table_name} DROP COLUMN {column_name};",
            "rename_table":"ALTER TABLE {table_name_old} RENAME TO {table_name_new}",
            "rename_column":"ALTER TABLE {table_name} RENAME COLUMN {column_name_old} TO {column_name_new};",
            "return_column":"SELECT {column_name} FROM {table_name};",
            "sort_table":"""CREATE TABLE {table_name}_sorted AS 
                            SELECT *
                            FROM {table_name}
                            ORDER BY {sortvar};""",
            "sort_and_remove_duplicates":"""CREATE TABLE {table_in}_sorted AS 
                                            SELECT * FROM (
                                                SELECT *, ROW_NUMBER() OVER (
                                                PARTITION BY {idvar} 
                                                ORDER BY {sortvar}) 
                                                AS row_number    
                                                FROM {table_in}) 
                                            AS rows WHERE row_number=1;""",
            "duplicate_on":"""CREATE TABLE {table_out} AS
                              WITH RECURSIVE counter(value) AS (
                              SELECT 1
                              UNION ALL
                              SELECT value + 1 
                              FROM counter LIMIT (SELECT MAX({num}) FROM {table_in}))
                              SELECT * FROM {table_in} JOIN counter ON value <= {num}""",
        },


        "main":{
            "query1":"""CREATE TABLE world_msf2 AS
                        SELECT a.*, b.gics AS gics, coalesce(b.sic, c.sic) AS sic, coalesce(b.naics, c.naics) AS naics
	                    FROM world_msf1 AS a
	                    LEFT JOIN comp_ind AS b ON a.gvkey=b.gvkey AND a.eom=b.date
	                    LEFT JOIN crsp_ind AS c ON a.permco=c.permco AND a.permno=c.permno AND a.eom=c.date;""",
            "query2":"""CREATE TABLE world_msf AS
                        SELECT 
                        CASE
                            WHEN a.me IS NULL THEN ('')
			                WHEN a.me >= b.nyse_p80 THEN 'mega'
			                WHEN a.me >= b.nyse_p50 THEN 'large'
			                WHEN a.me >= b.nyse_p20 THEN 'small'
			                WHEN a.me >= b.nyse_p1 THEN 'micro'
			                ELSE 'nano'
		                END AS size_grp, a.*
	                    FROM world_msf3 AS a 
	                    LEFT JOIN scratch.nyse_cutoffs AS b
	                    ON a.eom=b.eom;""",
            "query3":"""CREATE TABLE world_data_prelim AS
                        SELECT a.*, b.*, c.*
	                    FROM world_msf AS a
	                    LEFT JOIN market_chars_m AS b
	                    ON a.id=b.id AND a.eom=b.eom
	                    LEFT JOIN acc_chars_world AS c
	                    ON a.gvkey=c.gvkey AND a.eom=c.public_date;""",
            "query4":"""CREATE TABLE world_data3 AS
	                    SELECT a.*, b.beta_60m, b.ivol_capm_60m, c.resff3_12_1, d.resff3_6_1, e.mispricing_mgmt, e.mispricing_perf, f.*, g.age
	                    FROM world_data_prelim AS a
	                    LEFT JOIN beta_60m AS b ON a.id=b.id AND a.eom=b.eom
	                    LEFT JOIN resmom_ff3_12_1 AS c ON a.id=c.id AND a.eom=c.eom
	                    LEFT JOIN resmom_ff3_6_1 AS d ON a.id=d.id AND a.eom=d.eom
	                    LEFT JOIN mp_factors AS e ON a.id=e.id AND a.eom=e.eom
	                    LEFT JOIN market_chars_d AS f ON a.id=f.id AND a.eom=f.eom
	                    LEFT JOIN firm_age AS g ON a.id=g.id AND a.eom=g.eom;""",
            "query5":"""CREATE TABLE world_data4 AS 
                        SELECT a.*, b.qmj, b.qmj_prof, b.qmj_growth, b.qmj_safety
	                    FROM world_data3 AS a 
	                    LEFT JOIN scratch.qmj AS b
	                    ON a.excntry=b.excntry AND a.id=b.id AND a.eom=b.eom;""",
        },



        "prepare_crsp_sf":{
            #query1:
            "query1":"""CREATE TABLE __crsp_sf1 AS 
                        SELECT a.permno, a.permco, a.date, (a.prc < 0) AS bidask, abs(a.prc) AS prc, a.shrout/1000 AS shrout, abs(a.prc)*a.shrout/1000 AS me,
	                    a.ret, a.retx, a.cfacshr, a.vol, 
		                CASE WHEN a.prc > 0 AND a.askhi > 0 then a.askhi ELSE NULL END AS prc_high,   
		                CASE WHEN a.prc > 0 AND a.bidlo > 0 then a.bidlo ELSE NULL END AS prc_low,    
		                b.shrcd, b.exchcd, c.gvkey, c.liid AS iid, 
		                b.exchcd in (1, 2, 3) AS exch_main			
		                FROM crsp_{freq}sf AS a 
		                LEFT JOIN crsp_{freq}senames AS b
		                ON a.permno=b.permno AND a.date>=namedt AND a.date<=b.nameendt
		                LEFT JOIN crsp_ccmxpf_lnkhist as c
		                ON a.permno=c.lpermno AND (a.date>=c.linkdt OR (c.linkdt IS NOT NULL)) AND 
		                (a.date<=c.linkenddt or (c.linkenddt IS NOT NULL)) AND c.linktype IN ('LC', 'LU', 'LS');""",
            #query2:
            "query2":"""UPDATE __crsp_sf1
		                SET vol = 
			            CASE 
				            WHEN date < DATE('2001-02-01') THEN vol / 2
				            WHEN date <= DATE('2001-12-31') THEN vol / 1.8
				            WHEN date < DATE('2003-12-31') THEN vol / 1.6
				            ELSE vol
			            END
		                WHERE exchcd = 3;""",
            #query4:
            "query4":"""CREATE TABLE __crsp_sf2 AS
                        SELECT *, ABS(prc) * vol AS dolvol,
                        CASE
                            WHEN ROW_NUMBER() OVER (PARTITION BY permno ORDER BY date) = 1 THEN NULL
                            ELSE (ret - retx) * LAG(prc) OVER (PARTITION BY permno ORDER BY date) * (cfacshr / LAG(cfacshr) OVER (PARTITION BY permno ORDER BY date))
                        END AS div_tot
                        FROM __crsp_sf1;""",
            #query5: format
            "query5_d":"""CREATE TABLE __crsp_sf3 AS 
                          SELECT a.*, b.dlret, b.dlstcd
                          FROM __crsp_sf2 AS a
                          LEFT JOIN crsp_{freq}sedelist AS b
                          ON a.permno = b.permno AND a.date = b.dlstdt;""",
            #query5: format
            "query5_m":"""CREATE TABLE __crsp_sf3 AS
                          SELECT a.*, b.dlret, b.dlstcd
                          FROM __crsp_sf2 AS a
                          LEFT JOIN crsp_{freq}sedelist AS b
                          ON a.permno = b.permno AND strftime('%Y-%m', a.date) = strftime('%Y-%m', b.dlstdt);""",
            #query6:
            "query6":"""CREATE TABLE __crsp_sf4_temp AS 
                        SELECT *,
                        CASE 
                            WHEN dlret IS NULL AND (dlstcd=500 OR (520<=dlstcd AND dlstcd<=583)) THEN -0.3
                            ELSE dlret 
                        END AS dlret_new
                        FROM __crsp_sf3;
                        """,
            "query6_0":"""CREATE TABLE __crsp_sf4 AS 
                          SELECT *,
                          CASE 
                              WHEN ret IS NULL AND dlret_new IS NOT NULL THEN 0
                              ELSE (1+ret)*(1+COALESCE(dlret_new,0))-1
                          END AS ret_new
                          FROM __crsp_sf4_temp;""",
            "query7":"""CREATE TABLE __crsp_sf5 AS
                        SELECT a.*,a.ret-coalesce(b.t30ret,c.rf)/{scale} AS ret_exc
                        FROM __crsp_sf4 AS a
                        LEFT JOIN crsp_mcti AS b
                        ON strftime('%Y-%m',a.date)=strftime('%Y-%m',b.caldt)
                        LEFT JOIN ff_factors_monthly AS c
                        ON strftime('%Y-%m',a.date)=strftime('%Y-%m',c.date);""",
            "query8":"""CREATE TABLE __crsp_sf6 AS 
                        SELECT *,SUM(me) AS me_company
                        FROM __crsp_sf5
                        GROUP BY permco,date;""",
            "query9":"""UPDATE __crsp_sf6
			            SET vol=vol*100,dolvol=dolvol*100;
		                """,
            #query10: format
            "query10":"""CREATE TABLE _crsp_{freq}sf AS
                         SELECT DISTINCT *
                         FROM __crsp_sf6
                         ORDER BY permno, date;""",
        },





        "prepare_comp_sf":{
            "query1":"""CREATE TABLE __firm_shares1 AS 
                        SELECT gvkey, datadate, cshoq AS csho_fund, ajexq AS ajex_fund 
                        FROM comp_fundq 
                        WHERE indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C' AND cshoq IS NOT NULL AND ajexq IS NOT NULL 
                        UNION 
                        SELECT gvkey, datadate, csho AS csho_fund, ajex AS ajex_fund 
                        FROM comp_funda 
                        WHERE indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C' AND csho IS NOT NULL AND ajex IS NOT NULL;
                        """,
            "query2":"""CREATE TABLE __temp1 AS 
                            SELECT *,LAG(datadate) OVER (PARTITION BY gvkey ORDER BY datadate DESC) AS following,
                                ROW_NUMBER() OVER (PARTITION BY gvkey ORDER BY datadate DESC) AS row_number,
                                INTNX_(CAST(datadate AS text),12,'month','end') AS forward_max
                            FROM __temp;""",
            "query3":"""CREATE TABLE __temp2 AS 
                          SELECT *, CASE WHEN row_number=1 THEN NULL ELSE following END AS following_new,
                              INTCK_(CAST(datadate AS text),CAST(MIN(COALESCE(following,'2200-01-01'),COALESCE(forward_max,'2200-01-01')) AS text),'{freq}','discrete') AS n
                          FROM __temp1;""",
            "query4":"""CREATE TABLE __temp4 AS
                        SELECT *, INTNX_(CAST(datadate AS text),value-1,'{freq}','end') AS ddate
                        FROM __temp3
                        ORDER BY gvkey,datadate,ddate;""",
            "query5":"""CREATE TABLE __comp_dsf_na AS
                        SELECT a.gvkey,a.iid,a.datadate,a.tpci,a.exchg,a.prcstd,a.curcdd,a.prccd AS prc_local,a.ajexdi, 
                            CASE WHEN a.prcstd!=5 THEN a.prchd ELSE NULL END AS prc_high_lcl,  
			                CASE WHEN a.prcstd!=5 THEN a.prcld ELSE NULL END AS prc_low_lcl,   
			            cshtrd,COALESCE(a.cshoc/1e6, b.csho_fund*b.ajex_fund/a.ajexdi) AS cshoc, 
		   	            (a.prccd/a.ajexdi*a.trfd) AS ri_local,a.curcddv,a.div,a.divd,a.divsp 
		                FROM comp_secd AS a 
		                LEFT JOIN __firm_shares2 AS b
		                ON a.gvkey=b.gvkey AND a.datadate=b.ddate;""",
            "query6":"""UPDATE __comp_dsf_na
		                SET cshtrd =
	                        CASE
				                WHEN datadate<DATE('2001-02-01') THEN cshtrd/2
				                WHEN datadate<=DATE('2001-12-31') THEN cshtrd/1.8
				                WHEN datadate<DATE('2003-12-31') THEN cshtrd/1.6
				                ELSE cshtrd
			                END
		                WHERE exchg=14;""",
            "query7":"""CREATE TABLE __comp_dsf_global AS
                        SELECT gvkey, iid, datadate, tpci, exchg, prcstd, curcdd,
			                prccd/qunit AS prc_local, ajexdi, cshoc/1e6 AS cshoc,
			                CASE WHEN prcstd!=5 THEN prchd/qunit ELSE NULL END AS prc_high_lcl,  
			                CASE WHEN prcstd!=5 THEN prcld/qunit ELSE NULL END AS prc_low_lcl,  
			                cshtrd, ((prccd/qunit)/ajexdi*trfd) AS ri_local, curcddv, div, divd, divsp
		                FROM comp_g_secd;""",
            "query8":"""CREATE TABLE __comp_dsf1 AS 
                        SELECT * FROM __comp_dsf_na
		                UNION
		                SELECT * FROM __comp_dsf_global;""",
            "query9":"""CREATE TABLE __comp_dsf2 AS
		                SELECT a.*, b.fx AS fx, c.fx AS fx_div
		                FROM __comp_dsf1 AS a
		                LEFT JOIN fx AS b
			            ON a.curcdd=b.curcdd AND a.datadate=b.date
		                LEFT JOIN fx AS c
			            ON a.curcddv=c.curcdd AND a.datadate=c.date;""",
            "query10":"""CREATE TABLE __comp_dsf3 AS 
                         SELECT a.*, prc_local*fx AS prc, prc_high_lcl*fx AS prc_high, prc_low_lcl*fx AS prc_low,
                             prc_local*cshoc AS me, cshtrd*prc_local*fx AS dolvol, ri_local*fx AS ri,
                             COALESCE(div,0)*fx_div AS div_tot, COALESCE(divd,0)*fx_div AS div_cash,
                             COALESCE(divsp,0)*fx_div AS div_spc
                         FROM __comp_dsf2""",
            "query11":"""CREATE TABLE __comp_msf1 AS 
                         SELECT *, date(datadate,'end of month') AS eom, 
                             max(max(prc_high/ajexdi),max(prc/ajexdi))*ajexdi AS prc_highm, 
                             min(min(prc_low/ajexdi),min(prc/ajexdi))*ajexdi AS prc_lowm,
                             sum(div_tot/ajexdi)*ajexdi AS div_totm, 
                             sum(div_cash/ajexdi)*ajexdi AS div_cashm, 
                             sum(div_spc/ajexdi)*ajexdi AS div_spcm,
                             sum(cshtrd/ajexdi)*ajexdi AS cshtrm, 
                             sum(dolvol) AS dolvolm
                         FROM __comp_dsf3;""",
            "query12":"""CREATE TABLE __comp_msf2 AS 
                         SELECT * 
                         FROM __comp_msf1
                         WHERE prc_local IS NOT NULL AND curcdd IS NOT NULL AND prcstd IN (3, 4, 10)
                         ORDER BY gvkey, iid, eom, datadate;""",
            # "query13_1":"ALTER TABLE __comp_msf2 DROP COLUMN cshtrd;",
            # "query13_2":"ALTER TABLE __comp_msf2 DROP COLUMN div_tot;",
            # "query13_3":"ALTER TABLE __comp_msf2 DROP COLUMN div_cash;",
            # "query13_4":"ALTER TABLE __comp_msf2 DROP COLUMN div_spc;",
            # "query13_5":"ALTER TABLE __comp_msf2 DROP COLUMN dolvol;",
            # "query13_6":"ALTER TABLE __comp_msf2 DROP COLUMN prc_high;",
            # "query13_7":"ALTER TABLE __comp_msf2 DROP COLUMN prc_low;",
            # "query13_8":"ALTER TABLE __comp_msf2 RENAME COLUMN div_totm TO div_tot",
            # "query13_9": "ALTER TABLE __comp_msf2 RENAME COLUMN div_cashm TO div_cash",
            # "query13_10": "ALTER TABLE __comp_msf2 RENAME COLUMN div_spcm TO div_spc",
            # "query13_11": "ALTER TABLE __comp_msf2 RENAME COLUMN dolvolm TO dolvol",
            # "query13_12": "ALTER TABLE __comp_msf2 RENAME COLUMN prc_highm TO prc_high",
            # "query13_13": "ALTER TABLE __comp_msf2 RENAME COLUMN prc_lowm TO prc_low",
            "query14":"""CREATE TABLE __comp_msf3 AS 
                         SELECT * 
                         FROM __comp_msf2
                         WHERE rowid IN (
                             SELECT MAX(rowid)
                             FROM __comp_msf2
                             GROUP BY gvkey, iid, eom);""",
            "query15":"""CREATE TABLE __comp_secm1 AS
                         SELECT a.gvkey,a.iid,a.datadate,DATE(a.datadate,'end of month') AS eom,
					         a.tpci,a.exchg,a.curcdm AS curcdd,a.prccm AS prc_local,a.prchm AS prc_high,a.prclm AS prc_low,a.ajexm AS ajexdi,
					         COALESCE(a.cshom/1e6,a.csfsm/1e3,a.cshoq,b.csho_fund*b.ajex_fund/a.ajexm) AS cshoc, 
					         a.dvpsxm,a.cshtrm,a.curcddvm,a.prccm/a.ajexm*a.trfm AS ri_local, 
					         c.fx AS fx, d.fx AS fx_div
				         FROM comp_secm AS a
				         LEFT JOIN __firm_shares2 AS b
					     ON a.gvkey=b.gvkey AND a.datadate=b.ddate
				         LEFT JOIN fx AS c
					     ON a.curcdm=c.curcdd AND a.datadate=c.date
				         LEFT JOIN fx AS d
					     ON a.curcddvm=d.curcdd AND a.datadate=d.date;""",
			"query16":"""UPDATE __comp_secm1
				         SET cshtrm =
					     CASE
						     WHEN datadate<DATE('2001-02-01') then cshtrm/2
						     WHEN datadate<=DATE('2001-12-31') then cshtrm/1.8
						     WHEN datadate<DATE('2003-12-31') then cshtrm/1.6
						     ELSE cshtrm
					     END
				         WHERE exchg = 14;""",
            "query17":"""UPDATE __comp_secm1
                         SET fx=
                         CASE 
                             WHEN curcdd='USD' then 1
                             ELSE fx
                         END;""",
            "query18":"""UPDATE __comp_secm1
                         SET fx_div=
                         CASE 
                             WHEN curcdd='USD' then 1
                             ELSE fx_div
                         END;""",
            "query19":"""CREATE TABLE __comp_secm2 AS 
                         SELECT *,prc_local*fx AS prc,prc_high*fx AS prc_high_new,
                             prc_low*fx AS prc_low_new,prc*cshoc AS me,cshtrm*prc AS dolvol,ri_local*fx AS ri
                             dvpsxm*fx_div AS div_tot,NULL AS div_cash_new,NULL AS div_spc_new
                         FROM __comp_secm1;""",
            # "query20_1":"ALTER TABLE __comp_secm2 DROP COLUMN dvpsxm",
            # "query20_2": "ALTER TABLE __comp_secm2 DROP COLUMN fx_div",
            # "query20_3": "ALTER TABLE __comp_secm2 DROP COLUMN curcddvm",
            # "query20_4": "ALTER TABLE __comp_secm2 DROP COLUMN prc_high",
            # "query20_5": "ALTER TABLE __comp_secm2 DROP COLUMN prc_low",
            # "query20_6": "ALTER TABLE __comp_secm2 DROP COLUMN div_cash",
            # "query20_7": "ALTER TABLE __comp_secm2 DROP COLUMN div_spc",
            # "query20_8": "ALTER TABLE __comp_secm2 RENAME COLUMN prc_high_new TO prc_high",
            # "query20_9": "ALTER TABLE __comp_secm2 RENAME COLUMN prc_low_new TO prc_low",
            # "query20_10": "ALTER TABLE __comp_secm2 RENAME COLUMN div_cash_new TO div_cash",
            # "query20_11": "ALTER TABLE __comp_secm2 RENAME COLUMN div_spc_new TO div_spc",
            "query21":"""CREATE TABLE __comp_msf4 AS
                         SELECT {common_vars},prcstd,'secd' AS source 
                         FROM __comp_msf3
                         UNION
                         SELECT {common_vars},10 AS prcstd,'secm' AS source 
                         FROM __comp_secm2;""",
            "query22":"""CREATE TABLE __comp_msf5 AS
                         SELECT *
                         FROM __comp_msf4
                         GROUP BY gvkey, iid, eom
                         HAVING COUNT(*)=1 OR (COUNT(*)=2 AND source='secd');""",
            "query23":"""CREATE TABLE __comp_msf6 AS 
                         SELECT DISTINCT * 
                         FROM __comp_msf5
                         ORDER BY gvkey,iid,emo;""",
            "query24":"""CREATE TABLE __comp_sf1 AS 
                         SELECT DISTINCT *
                         FROM {base}
                         ORDER BY gvkey,iid,datadate;""",
            "query25":"""CREATE TABLE __returns_temp AS 
                         SELECT *
                         FROM __comp_sf1
                         WHERE ri IS NOT NULL AND prcstd IN (3,4,10);""",
            "query26":"""CREATE TABLE __returns AS 
                         SELECT *,
                             ri/LAG(ri) OVER(PARTITION BY gvkey,iid ORDER BY datadate)-1 AS ret,
                             ri_local/LAG(ri_local) OVER(PARTITION BY gvkey,iid ORDER BY datadate)-1 AS ret_local,
                             JULIANDAY(datadate)-JULIANDAY(LAG(datadate) OVER(PARTITION BY gvkey,iid ORDER BY datadate)) AS ret_lag_dif,
                             ROW_NUMBER() OVER (
                                PARTITION BY gvkey,iid 
                                ORDER BY gvkey,iid) 
                                AS row_number 
                         FROM returns_temp;
                         """,
            "query27":"""UPDATE __returns
                         SET ret=NULL,ret_local=NULL,ret_lag_dif=NULL
                         WHERE row_number=1;""",
            "query28":"""UPDATE __returns
                         SET ret_local=ret
                         WHERE row_number=1 AND curcdd!=LAG(curcdd) OVER(PARTITION BY gvkey,iid ORDER BY gvkey,iid);""",
            "query29":"""CREATE TABLE __sec_info AS
                         SELECT *
                         FROM comp_security
                         UNION
                         SELECT *
                         FROM comp_g_security;""",
            "query30":"""CREATE TABLE __delist1_temp AS
                         SELECT *,ROW_NUMBER() OVER (
                                PARTITION BY gvkey,iid 
                                ORDER BY gvkey,iid,datadate DESC) 
                                AS row_number 
                         FROM(
                             SELECT *
                             FROM __returns
                             WHERE ret_local IS NOT NULL and ret_local!=0);""",
            "query31":"""CREATE TABLE __delist1 AS 
                         SELECT *
                         FROM __delist1_temp
                         WHERE row_number=1""",
            "query70":"""CREATE TABLE __delist2 AS
			             SELECT a.gvkey, a.iid, a.datadate, b.secstat, b.dlrsni
			             FROM __delist1 AS a 
			             LEFT JOIN __sec_info AS b
			             ON a.gvkey=b.gvkey AND a.iid=b.iid;""",
			"query75":"""CREATE TABLE __delist3 AS
			             SELECT gvkey, iid, datadate AS date_delist,
				         CASE WHEN dlrsni in ('02', '03') THEN -0.3 ELSE 0 END AS dlret
			             FROM __delist2
			             WHERE secstat='I';""",
            "query80":"""CREATE TABLE __comp_sf2 AS
			             SELECT a.*, b.ret, b.ret_local, b.ret_lag_dif, c.date_delist, c.dlret
			             FROM {base} AS a
			             LEFT JOIN __returns AS b
				         ON a.gvkey=b.gvkey AND a.iid=b.iid AND a.datadate=b.datadate
			             LEFT JOIN __delist3 AS c
				         ON a.gvkey=c.gvkey AND a.iid=c.iid;""",
            "query85":"""CREATE TABLE __comp_sf3 AS 
                         SELECT *
                         FROM __comp_sf2
                         WHERE datadate<=date_delist OR date_delist IS NULL;""",
            "query86":"""UPDATE __comp_sf2 
                         SET ret=
                         CASE WHEN datadate=date_delist THEN ret=(1+ret)*(1+dlret)-1
                              ELSE ret
                         END;""",
            "query87":"""UPDATE __comp_sf2 
                         SET ret_local=
                         CASE WHEN datadate=date_delist THEN ret_local=(1+ret_local)*(1+dlret)-1
                              ELSE ret_local
                         END;""",
            "query90":"""CREATE TABLE __comp_sf4 AS
                         SELECT a.*, a.ret-coalesce(b.t30ret, c.rf)/{scale} AS ret_exc 
			             FROM __comp_sf3 AS a
			             LEFT JOIN crsp.mcti AS b
				         ON STRFTIME('%Y-%m',a.datadate)=STRFTIME('%Y-%m',b.caldt)
			             LEFT JOIN ff.factors_monthly AS c
				         ON STRFTIME('%Y-%m',a.datadate)=STRFTIME('%Y-%m',c.date);""",
            "query100":"""CREATE TABLE __comp_sf5 AS
                         SELECT a.*, b.excntry, b.exch_main
                         FROM __comp_sf4 AS a 
                         LEFT JOIN __exchanges AS b
                         ON a.exchg=b.exchg;""",
            # "qeury101_1":"DROP TABLE IF EXISTS __firm_shares1",
            # "qeury101_2": "DROP TABLE IF EXISTS __firm_shares2",
            # "qeury101_3": "DROP TABLE IF EXISTS fx",
            # "qeury101_4": "DROP TABLE IF EXISTS __comp_dsf_na",
            # "qeury101_5": "DROP TABLE IF EXISTS __comp_dsf_global",
            # "qeury101_6": "DROP TABLE IF EXISTS __comp_dsf1",
            # "qeury101_7": "DROP TABLE IF EXISTS __comp_dsf2",
            # "qeury101_8": "DROP TABLE IF EXISTS __comp_dsf3",
            # "qeury101_9": "DROP TABLE IF EXISTS __returns",
            # "qeury101_10": "DROP TABLE IF EXISTS __sec_info",
            # "qeury101_11": "DROP TABLE IF EXISTS __delist1",
            # "qeury101_12": "DROP TABLE IF EXISTS __delist2",
            # "qeury101_13": "DROP TABLE IF EXISTS __delist3",
            # "qeury101_14": "DROP TABLE IF EXISTS __comp_sf1",
            # "qeury101_15": "DROP TABLE IF EXISTS __comp_sf2",
            # "qeury101_16": "DROP TABLE IF EXISTS __comp_sf3",
            # "qeury101_17": "DROP TABLE IF EXISTS __comp_sf4",
            # "qeury101_18": "DROP TABLE IF EXISTS __comp_sf5",
            # "qeury101_19": "DROP TABLE IF EXISTS __comp_sf6",
            # "qeury101_20": "DROP TABLE IF EXISTS __exchanges",
        },




        "add_helper_vars": {
            "query1": """CREATE TABLE __comp_dates1 AS
                            SELECT gvkey, curcd, MIN(datadate) AS start_date, MAX(datadate) AS end_date
                            FROM {data}
                            GROUP BY gvkey, curcd;""",
            "query2": """CREATE TABLE __helpers1 AS 
                            SELECT a.gvkey, a.curcd, a.datadate, b.*, 
                                CASE WHEN b.gvkey IS NOT NULL THEN 1 ELSE 0 END AS data_available
                            FROM __comp_dates2 AS a
                            LEFT JOIN {data} AS b ON a.gvkey=b.gvkey AND a.curcd=b.curcd AND a.datadate=b.datadate;""",
            "query3": """CREATE TABLE __helpers1_sorted AS 
                            SELECT * FROM (
                            SELECT *, ROW_NUMBER() OVER (
                                PARTITION BY gvkey,curcd,datadate 
                                ORDER BY gvkey,curcd,datadate) 
                                AS row_number    
                            FROM __helpers1) 
                            AS rows WHERE row_number = 1;""",
            "query4": """CREATE TABLE __helpers2 AS 
                            SELECT *, ROW_NUMBER() OVER (
                                PARTITION BY gvkey,curcd
                                ORDER BY gvkey,curcd
                            ) AS count
                            FROM __helpers1_sorted;""",
            "query5": """UPDATE __helpers2
    		                SET {variable} = 
    			            CASE 
    				            WHEN {varibale}<0 THEN NULL
    				            ELSE {variable}
    			            END;""",
            "query6": """CREATE TABLE {out} AS
                            SELECT 
                            COALESCE(sale,revt) AS sale_x,
                            COALESCE(gp,sale_x-cogs) AS gp_x,
                            coalesce(xopr,cogs+xsga) as opex_x,
                            coalesce(ebitda, oibdp, sale_x-opex_x, gp_x-xsga) as ebitda_x,
    		        		coalesce(ebit, oiadp, ebitda_x-dp) as ebit_x,
    		                ebitda_x + coalesce(xrd, 0) as op_x,
    		                ebitda_x-xint as ope_x,
    		                coalesce(pi, ebit_x-xint+coalesce(spi,0)+coalesce(nopi,0)) as pi_x,
    		                coalesce(xido, xi+coalesce(do, 0)) as xido_x,
    		                coalesce(ib, ni-xido_x, pi_x - txt - coalesce(mii, 0)) as ni_x,
    		                coalesce(ni, ni_x+coalesce(xido_x, 0), ni_x + xi + do) as nix_x,
    		                nix_x+xint as fi_x,  
    		     			coalesce(dvt, dv) as div_x,
    		     			coalesce(prstkc,0)+coalesce(purtshr,0) as eqbb_x,
    		                sstk as eqis_x,
    		                coalesce(eqis_x,0)+coalesce(-eqbb_x,0) as eqnetis_x,
    		                div_x+eqbb_x as eqpo_x,
    		                div_x-eqnetis_x as eqnpo_x,
    		                case 
    		                    when dltis is null and dltr is null and ltdch is null and count<=12 then null
    		                    else coalesce(coalesce(dltis,0)+coalesce(-dltr,0),ltdch, dltt-lag(dltt,12) over(partition by gvkey,curcd order by gvkey curcd)) 
    		                end as dltnetis_x, 
    		                case 
    		                    when dlcch is null and count<=12 then null
    		                    else coalesce(dlcch, dlc-lag(dlc,12) over(partition by gvkey,curcd order by gvkey curcd))
    		                end as dstnetis_x,
    		                coalesce(dstnetis_x,0)+coalesce(dltnetis_x,0) as dbnetis_x,
                            eqnetis_x+dbnetis_x as netis_x,
                            coalesce(fincf, netis_x-dv+coalesce(fiao, 0)+coalesce(txbcof, 0)) as fincf_x,

                            coalesce(dltt,0)+coalesce(dlc,0) as debt_x
                            coalesce(pstkrv, pstkl, pstk) as pstk_x,
                            coalesce(seq, ceq+coalesce(pstk_x, 0), at-lt) as seq_x,
                            coalesce(at, seq_x + dltt + coalesce(lct, 0) + coalesce(lo, 0) + coalesce(txditc, 0)) as at_x, 
                            coalesce(act, rect+invt+che+aco) as ca_x,
                            at_x-ca_x as nca_x,
                            coalesce(lct, ap+dlc+txp+lco) as cl_x,
                            lt-cl_x as ncl_x,

                            debt_x - coalesce(che, 0) as netdebt_x,
                            coalesce(txditc, sum(txdb, itcb)) as txditc_x,
                            seq_x+coalesce(txditc_x, 0)-coalesce(pstk_x, 0) as be_x,
                            coalesce(icapt+coalesce(dlc, 0)-coalesce(che, 0), netdebt_x+seq_x+coalesce(mib, 0)) as bev_x,

                            ca_x-che as coa_x,
                            cl_x-coalesce(dlc, 0) as col_x,
                            coa_x-col_x as cowc_x,
                            at_x-ca_x-coalesce(ivao, 0) as ncoa_x,
                            lt-cl_x-dltt as ncol_x,
                            ncoa_x-ncol_x as nncoa_x,
                            coalesce(ivst,0)+coalesce(ivao,0) as fna_x,
                            debt_x+coalesce(pstk_x,0) as fnl_x,
                            fna_x-fnl_x as nfna_x,
                            coa_x+ncoa_x as oa_x,
                            col_x+ncol_x as ol_x,
                            oa_x-ol_x as noa_x,
                            ppent+intan+ao-lo+dp as lnoa_x,

                            coalesce(ca_x-invt, che+rect) as caliq_x,
                            ca_x-cl_x as nwc_x,
                            ppegt + invt as ppeinv_x,
                            che+0.75*coa_x+0.5*(at_x-ca_x-coalesce(intan,0)) as aliq_x,

                            case 
                                when count<=12 then null
                                else coalesce(ni_x-oancf, cowc_x-lag(cowc_x,12) over(partition by gvkey,curcd order by gvkey curcd)+nncoa_x-lag(nncoa_x) over(partition by gvkey,curcd order by gvkey curcd)) 
                            end as oacc_x,
                            case 
                                when count<=12 then null
                                else oacc_x+nfna_x-lag(nfna_x) over(partition by gvkey,curcd order by gvkey curcd) 
                            end as tacc_x,
                            coalesce(oancf,ni_x-oacc_x,ni_x+dp-coalesce(wcapt,0)) as ocf_x, 
                            ocf_x-capx as fcf_x,
                            bitda_x + coalesce(xrd, 0) - oacc_x as cop_x
                            FROM __helpers2""",
            "query7": """ALTER TABLE {out} DROP COLUMN count;""",
            "query8_1": """DROP TABLE IF EXISTS __comp_dates1;""",
            "query8_2": """DROP TABLE IF EXISTS __comp_dates2;""",
            "query8_3": """DROP TABLE IF EXISTS __helpers1;""",
            "query8_4": """DROP TABLE IF EXISTS __helpers2;""",
        },








        "add_primary_sec":{
            "query1":"""CREATE TABLE __prihistrow AS 
                        SELECT gvkey,itemvalue AS prihistrow,effdate,thrudate 
                        FROM comp_g_sec_history 
                        WHERE item = 'PRIHISTROW';""",
            "query2":"""CREATE TABLE __prihistusa AS 
                        SELECT gvkey,itemvalue AS prihistusa,effdate,thrudate
                        FROM comp_sec_history 
                        WHERE item='PRIHISTUSA';""",
            "query3":"""CREATE TABLE __prihistcan AS 
                        SELECT gvkey, itemvalue AS prihistcan, effdate, thrudate
                        FROM comp_sec_history 
                        WHERE item = 'PRIHISTCAN';""",
            "query4":""" CREATE TABLE __header AS
                         SELECT gvkey, MAX(prirow) AS prirow, MAX(priusa) AS priusa, MAX(prican) AS prican 
                         FROM (
                         SELECT gvkey, prirow, priusa, prican FROM comp_company
                         UNION ALL
                         SELECT gvkey, prirow, priusa, prican FROM comp_g_company
                         )
                         GROUP BY gvkey;""",
            "query5":"""CREATE TABLE __header_sorted AS 
                        SELECT DISTINCT gvkey, prirow, priusa, prican 
                        FROM __header 
                        ORDER BY gvkey;""",
            "query6":"""CREATE TABLE __data1 AS 
                        SELECT DISTINCT data_a.*,
                        coalesce(data_b.prihistrow,data_e.prirow) AS prihistrow,
                        coalesce(data_c.prihistusa,data_e.priusa) AS prihistusa,
                        coalesce(data_d.prihistcan,data_e.prican) AS prihistcan 
                        FROM {data} AS data_a
                        LEFT JOIN __prihistrow AS data_b
                            ON data_a.gvkey=data_b.gvkey AND data_a.{date_var}>=data_b.effdate AND (data_a.{date_var}<=data_b.thrudate OR data_b.thrudate IS NULL)
                        LEFT JOIN __prihistusa AS data_c 
                            ON data_a.gvkey=data_c.gvkey AND data_a.{date_var}>=data_c.effdate AND (data_a.{date_var}<=data_c.thrudate OR data_c.thrudate IS NULL)
                        LEFT JOIN __prihistcan AS data_d
                            ON data_a.gvkey=data_d.gvkey AND data_a.{date_var}>=data_d.effdate AND (data_a.{date_var}<=data_d.thrudate OR data_d.thrudate IS NULL)
                        LEFT JOIN __header AS data_e
                            ON data_a.gvkey=data_e.gvkey;""",
            "query7":"""CREATE TABLE __data2 AS 
                        SELECT *,(iid IS NOT NULL AND (iid=prihistrow OR iid=prihistusa OR iid=prihistcan)) AS primary_sec
                        FROM __data1;""",
            "query8":"""CREATE TABLE {out} AS 
                        SELECT * FROM __data2;""",
            "query8_1":"""ALTER TABLE {out} DROP COLUMN prihistrow;""",
            "query8_2":"""ALTER TABLE {out} DROP COLUMN prihistusa;""",
            "query8_1":"""ALTER TABLE {out} DROP COLUMN prihistcan;""",
            "query9_1":"""DROP TABLE IF EXISTS __prihistrow;""",
            "query9_2":"""DROP TABLE IF EXISTS __prihistusa;""",
            "query9_3":"""DROP TABLE IF EXISTS __prihistcan;""",
            "query9_4":"""DROP TABLE IF EXISTS __header;""",
            "query9_5":"""DROP TABLE IF EXISTS __data1;""",
            "query9_6":"""DROP TABLE IF EXISTS __data2;""",
        },



        "ap_factors":{
            "query1":"""CREATE TABLE world_sf1 AS
			            SELECT excntry, id, date, eom, ret_exc
			            FROM {sf}
			            WHERE ret_lag_dif<=5 AND not ret_exc IS NOT NULL;""",
            "query2":"""CREATE TABLE world_sf1 AS
			            SELECT excntry, id, eom, ret_exc
			            FROM {sf}
			            WHERE ret_lag_dif=1 AND ret_exc IS NOT NULL;""",
            "query3":"""CREATE TABLE base1 AS
                        SELECT id, eom, size_grp, excntry, me, market_equity, be_me, at_gr1, niq_be,
			                source_crsp, exch_main, obs_main, common, comp_exchg, crsp_exchcd, primary_sec, ret_lag_dif
		                FROM {mchars};""",
            "query4":"""CREATE TABLE base2 AS 
                        SELECT * 
                        FROM base1
                        ORDER BY id,eom;""",
            "query5":"""CREATE TABLE base3 AS 
                        SELECT *,{new_cols},
                            LAG(id) OVER(PARITION BY id,eom ORDER BY id,eom) AS id_l,
                            LAG(eom) OVER(PARITION BY id,eom ORDER BY id,eom) AS eom_l,
                            LAG(source_crsp) OVER(PARITION BY id,eom ORDER BY id,eom) AS srouce_crsp_l
                        FROM base2""",
            "query6":"""UPDATE base3
                        SET {col}_l=
                        CASE WHEN id!=LAG(id) OR source_crsp!=source_crsp_l OR INTCK_(eom_l,eom,'month','discrete') THEN NULL
                             ELSE THEN LAG({col}) OVER (PARTITION BY id,eom ORDER BY id,eom)
                        END;""",
            "query7":"""CREATE TABLE base4 AS
		                SELECT *,  
		                    CASE
					            WHEN size_grp_l IS NULL then ''
					            WHEN size_grp_l IN ('large', 'mega') THEN 'big'
					            ELSE 'small'
				            END AS size_pf
		                FROM base3
		                WHERE obs_main_l=1 AND exch_main_l=1 AND common_l=1 AND primary_sec_l=1 AND ret_lag_dif=1 AND me_l IS NOT NULL
		                ORDER BY excntry_l, size_grp_l, eom;""",

            "query10":"""CREATE TABLE bp_stocks AS
			             SELECT *
			             FROM base4
			             WHERE ((size_grp_l IN ('small','large','mega') AND excntry_l!='USA') OR ((crsp_exchcd_l=1 OR comp_exchg_l=11) AND excntry_l='USA')) and {char}_l IS NOT NULL
			             ORDER BY eom, excntry_l;""",
            "query11":"""CREATE TABLE bps AS
                         SELECT eom, excntry_l, COUNT(*) AS n,
                             percentile_cont(0.3) within group (order by {char}_l) AS bp_p30,
                             percentile_cont(0.7) within group (order by {char}_l) AS bp_p70
                         FROM bp_stocks
                         GROUP BY eom, excntry_l;""",
            "query12":"""CREATE TABLE weights1 AS
			             SELECT a.excntry_l, a.id, a.eom, a.size_pf, a.me_l,
				             CASE
					             WHEN a.{char}_l >= b.bp_p70 THEN 'high'
					             WHEN a.{char}_l >= b.bp_p30 THEN 'mid'
					             ELSE 'low'
				             END AS char_pf
			             FROM base4 AS a 
			             LEFT JOIN bps AS b
			             ON a.excntry_l = b.excntry_l AND a.eom = b.eom
			             WHERE b.n>={min_stocks_bp} AND a.{char}_l IS NOT NULL AND size_pf!='';""",
			"query13":"""CREATE TABLE weights2 AS
			             SELECT *, me_l/SUM(me_l) AS w
			             FROM weights1
			             GROUP BY excntry_l, size_pf, char_pf, eom
			             HAVING COUNT(*)>={min_stocks_pf};""",
            "query14":"""CREATE TABLE returns AS
			             SELECT a.*, b.w, b.size_pf, b.char_pf
			             FROM world_sf2 AS a 
			             INNER JOIN weights2 AS b
			             ON a.id = b.id AND a.eom=b.eom AND a.excntry=b.excntry_l;""",
            "query15":"""CREATE TABLE pfs1 AS
			             SELECT {char} AS characteristic, excntry, size_pf, char_pf, {__date_col}, SUM(ret_exc*w) AS ret_exc
			             FROM returns
			             GROUP BY excntry, size_pf, char_pf, {__date_col};""",
            "query16":"""CREATE TABLE pfs2 AS 
                         SELECT * 
                         FROM pfs1
                         ORDER BY characteristic excntry {__date_col};""",
            "query17":"""CREATE TABLE pfs3 AS
                         SELECT characteristic, excntry,
                             SUBSTR(column_name, 1, INSTR(column_name, '_') - 1) AS size_pf,
                             SUBSTR(column_name, INSTR(column_name, '_') + 1) AS char_pf,
                             CAST(GROUP_CONCAT(ret_exc, '|') AS TEXT) AS ret_exc
                         FROM (
                             SELECT characteristic,excntry,size_pf || '_' || char_pf AS column_name,ret_exc
                             FROM pfs2)
                         GROUP BY characteristic, excntry, column_name;""",
            "query18":"""CREATE TABLE {out} AS 
                         SELECT characteristic, excntry, {__date_col}, 
                             (small_high+big_high)/2-(small_low+big_low)/2 AS lms, 
                             (small_high+small_mid+small_low)/3-(big_high+big_mid+big_low)/3 AS smb
                         FROM pfs3;
                         """,
            "query19":"""CREATE TABLE hxz1 AS
                         SELECT * 
                         FROM asset_growth
                         UNION
                         SELECT *
                         FROM roeq;""",
            "query19_1":"""CREATE TABLE hxz2 AS 
                           SELECT characteristic, excntry,
                               MAX(CASE WHEN __date_col = 'lms' THEN lms END) AS lms,
                               MAX(CASE WHEN __date_col = 'smb' THEN smb END) AS smb
                           FROM hxz1
                           GROUP BY characteristic, excntry;""",
            "query20":"""CREATE TABLE ff AS 
                         SELECT * 
                         FROM book_to_market;""",
            "query21":"""CREATE TABLE hxz4 AS
                         SELECT excntry, __date_col,
                             MAX(CASE WHEN characteristic = 'col1' THEN col1 END) AS col1
                         FROM hxz3
                         GROUP BY excntry, __date_col;""",
            "query29":"""CREATE TABLE hxz AS 
                         SELECT *,(at_gr1_smb+niq_be_smb)/2 AS smb+hxz,-at_gr1_lms AS inv
                         FROM hxz4;""",
            "query30":"""CREATE TABLE {out} AS
		                 SELECT a.excntry, a.{__date_col}, a.mkt_vw_exc AS mktrf, b.hml, b.smb_ff, c.roe, c.inv, c.smb_hxz
		                 FROM {mkt} AS a
		                 LEFT JOIN ff AS b 
		                 ON a.excntry = b.excntry AND a.{__date_col} = b.{__date_col}
		                 LEFT JOIN hxz AS c 
		                 ON a.excntry = c.excntry AND a.{__date_col} = c.{__date_col};"""
        },




        "bidask_hl":{
            "query1":"""CREATE TABLE __dsf1 AS 
                        SELECT a.id, a.date, a.eom, a.bidask, a.tvol, 
                            a.prc/a.adjfct AS prc, a.prc_high/a.adjfct AS prc_high, a.prc_low/a.adjfct AS prc_low
		                FROM {data} AS a 
		                LEFT JOIN market_returns_daily AS b
		                ON a.excntry=b.excntry AND a.date=b.date
		                WHERE b.mkt_vw_exc IS NOT NULL
		                ORDER BY id, date;"""
        },



        "clean_comp_msf": {
            "query1": """UPDATE {data}
    		                SET ret=NULL, ret_local=NULL, ret_exc=NULL
    		                WHERE gvkey='002137' AND iid='01C' AND eom IN ('31DEC1983'd, '31JAN1984'd);""",
            "query2": """update {data}
    		                SET ret=NULL, ret_local=NULL, ret_exc=NULL
    		                WHERE gvkey='013633' AND iid='01W' and eom IN ('28FEB1995'd);"""
        },




        "combine_ann_qtr_chars":{
            "query1":"""CREATE TABLE __acc_chars1 AS
                        SELECT a.*, b.*
		                FROM {ann_data} AS a 
		                LEFT JOIN {qtr_data} AS b
		                ON a.gvkey=b.gvkey AND a.public_date=b.public_date;""",
            "query2":"""CREATE TABLE __acc_chars2 AS
                        SELECT *
                        FROM __acc_chars1;""",
            "query3":"""UPDATE __acc_chars2
                        SET {ann_var}=
                            CASE WHEN {ann_var} IS NULL OR ({qtr_var} IS NOT NULL AND datadate&{q_suffix}>datadate) THEN {qtr_var}
                                 ELSE {ann_var}
                            END;"""
        },




        "combine_crsp_comp_sf": {
            "query1": """CREATE TABLE __msf_world1 AS
                                    SELECT permno AS id, PERMNO AS permno, PERMCO AS permco, GVKEY AS gvkey, iid, 
                                           'USA' AS excntry, exch_main, CASE WHEN shrcd IN (10, 11, 12) THEN 1 ELSE 0 END AS common, 
                                           1 AS primary_sec, bidask, shrcd AS crsp_shrcd, exchcd AS crsp_exchcd, 
                                           '' AS comp_tpci, NULL AS comp_exchg, 'USD' AS curcd, 1 AS fx, date, 
                                           strftime('%Y%m%e', date(date, 'start of month', '+1 month')) AS eom, 
                                           cfacshr AS adjfct, shrout AS shares, me, me_company, prc, prc AS prc_local, 
                                           prc_high, prc_low, dolvol, vol AS tvol, RET AS ret, ret AS ret_local, ret_exc, 
                                           1 AS ret_lag_dif, div_tot, NULL AS div_cash, NULL AS div_spc, 1 AS source_crsp 
                                    FROM {crsp_msf}
                                    UNION ALL 
                                    SELECT 
                                        CASE 
                                            WHEN instr(iid,'W')>0 THEN CAST('3'||gvkey||substr(iid,1,2) AS INTEGER) 
                                            WHEN instr(iid,'C')>0 THEN CAST('2'||gvkey||substr(iid,1,2) AS INTEGER) 
                                            ELSE CAST('1'||gvkey||substr(iid,1,2) AS INTEGER) 
                                        END AS id, 
                                    NULL AS permno, NULL AS permco, gvkey, iid, excntry, exch_main, 
                                    CASE WHEN tpci = '0' THEN 1 ELSE 0 END AS common, primary_sec, 
                                    CASE WHEN prcstd = 4 THEN 1 ELSE 0 END AS bidask, NULL AS crsp_shrcd, NULL AS crsp_exchcd, 
                                    comp_tpci, exchg AS comp_exchg, curcdd AS curcd, fx, datadate AS date, 
                                    strftime('%Y%m%e', date(datadate, 'start of month', '+1 month')) AS eom, 
                                    ajexdi AS adjfct, cshoc AS shares, me, me AS me_company, prc, prc_local, 
                                    prc_high, prc_low, dolvol, cshtrm AS tvol, ret_local, ret, ret_exc, ret_lag_dif, 
                                    div_tot, div_cash, div_spc, 0 AS source_crsp 
                                    FROM {comp_msf};""",
            "query2": """CREATE TABLE __msf_world2 AS 
                            SELECT *, LAG(ret_exc) AS ret_exc_lead1m,LAG(id) AS id_lead1m,LAG(reg_lag_dif) AS reg_lag_dif_lead1m 
                            FROM __msf_world1
                            ORDER BY id eom DESC;""",
            "query3": """UPDATE TABLE __msf_world2 
                            SET ret_exc_lead1m=
                            CASE 
                                WHEN id_lead1m!=id AND ret_lag_dif_lead1m!=ret_lag_dif THEN NULL
                                ELSE ret_exc_lead1m
                            END;""",
            "query4": """CREATE TABLE __dsf_world1 AS
    		                SELECT permno AS id,'USA' AS excntry,exch_main, 
    		                    CASE WHEN shrcd in (10, 11, 12) THEN 1 ELSE 0 AS common, 1 AS primary_sec,
    		                    bidask,'USD' AS curcd,1 AS fx,DATE AS date, DATE(DATE,'end of month') AS eom,
    		   	                cfacshr AS adjfct,shrout AS shares,me,dolvol,vol AS tvol,prc,prc_high,prc_low,
    		   	                ret AS ret_local,RET AS ret,ret_exc,1 AS ret_lag_dif,1 AS source_crsp 
    		                FROM {crsp_dsf}
    		                UNION
    		                SELECT 
    		                    CASE
    		                        WHEN iid LIKE '/W/%' THEN input(cats('3', gvkey, substr(iid, 1, 2)), 9.0)
    				                WHEN iid LIKE '/W/%' THEN input(cats('2', gvkey, substr(iid, 1, 2)), 9.0)
    				                ELSE input(cats('1', gvkey, SUBSTR(iid, 1, 2)), 9.0)                           
    			                END AS id, excntry, exch_main, 
    			                CASE WHEN tpci='0' THEN 1 ELSE 0 AS common, primary_sec,
    			                CASE WHEN prcstd=4 THEN 1 ELSE 0 as bidask, curcdd AS curcd, fx, 
    			                datadate AS date, DATE(datadate,'end of month') as eom, ajexdi AS adjfct,
    			                cshoc AS shares, me, dolvol, cshtrd AS tvol, prc, prc_high, prc_low,
    		   	                ret_local, ret, ret_exc, ret_lag_dif, 0 as source_crsp
    		                FROM {comp_dsf};""",
            "query5": """CREATE TABLE __obs_main AS
    		                SELECT id,gvkey,iid,eom, 
    		                    CASE WHEN (COUNT(gvkey) IN (0,1) OR (COUNT(gvkey)>1 AND source_crsp=1)) THEN 1 ELSE 0 AS obs_main
    		                FROM __msf_world2
    		                GROUP BY gvkey, iid, eom;""",
            "query6": """CREATE TABLE __msf_world3 AS
    		                SELECT a.*, b.obs_main
    		                FROM __msf_world2 AS a 
    		                LEFT JOIN __obs_main AS b
    		                ON a.id=b.id AND a.eom=b.eom;""",
            "query7": """CREATE TABLE __dsf_world2 AS
    		                SELECT a.*, b.obs_main
    		                FROM __dsf_world1 AS a 
    		                LEFT JOIN __obs_main AS b
    		                ON a.id=b.id and a.eom=b.eom;""",
            "query7_1": """CREATE TABLE {out_msf} AS 
                              SELECT DISTINCT * 
                              FROM __msf_world3
                              ORDER BY id eom;""",
            "query7_2": """CREATE TABLE {out_dsf} AS 
                              SELECT DISTINCT * 
                              FROM __dsf_world3
                              ORDER BY id eom;""",
            "query8_1": "DROP TABLE IF EXISTS __msf_world1;",
            "query8_2": "DROP TABLE IF EXISTS __msf_world2;",
            "query8_3": "DROP TABLE IF EXISTS __msf_world3;",
            "query8_4": "DROP TABLE IF EXISTS __dsf_world1;",
            "query8_5": "DROP TABLE IF EXISTS __dsf_world2;",
            "query8_6": "DROP TABLE IF EXISTS __obs_main;",
        },





        "comp_exchanges": {
            "query1": """CREATE TABLE __ex_country1 AS
                                SELECT DISTINCT exchg,excntry FROM comp_g_security
                                UNION
                                SELECT DISTINCT exchg,excntry FROM comp_security;""",
            "query2": """CREATE TABLE __ex_country2 AS
        		                SELECT DISTINCT exchg,
        			            CASE
        				            WHEN COUNT(excntry)>1 THEN 'multi_national' 
        				            ELSE excntry
        			            END AS excntry
        		                FROM __ex_country1
        		                WHERE excntry IS NOT NULL AND NOT exchg IS NOT NULL
        		                GROUP BY exchg;""",
            "query3": """CREATE TABLE __ex_country3 AS
        		                SELECT a.*, b.exchgdesc
        		                FROM __ex_country2 AS a 
        		                LEFT JOIN comp_r_ex_codes AS b
        		                ON a.exchg=b.exchgcd;""",
            "query4": """CREATE TABLE {out} AS
        		                SELECT *, (excntry!='multi_national' AND exchg NOT IN {special_exchanges}) AS exch_main
        		                FROM __ex_country3;""",
            "query5_1": """DROP TABLE IF EXISTS __ex_country1;""",
            "query5_2": """DROP TABLE IF EXISTS __ex_country2;""",
            "query5_3": """DROP TABLE IF EXISTS __ex_country3;""",
        },




        "comp_hgics":{
            "query1":"""CREATE TABLE gic1 AS
                        SELECT DISTINCT gvkey, indfrom, indthru, gsubind AS gics
                        FROM comp_{lib}
		                WHERE gvkey IS NOT NULL
		                ORDER BY gvkey,indfrom;""",
            "query2":"""CREATE TABLE gic2 AS
                        SELECT gvkey,indfrom,indthru,
                            CASE WHEN gics IS NULL THEN -999
                                 ELSE gics
                            END AS gics,
                            ROW_NUMBER() OVER(PARTITION BY gvkey ORDER BY indfrom DESC, indthru DESC) AS row_number
                        FROM gic1;""",
            "query3":"""CREATE TABLE gic3 AS
                        SELECT gvkey,indthru,gics,
                            CASE WHEN row_number=1 AND indthru IS NULL THEN '2022-12-31'
                                 ELSE indthru
                            END AS indthru
                        FROM gic2;""",
            "query4":"""CREATE TABLE gic4 AS
                        SELECT *,INTCK_(indfrom,indthru,'day','discrete') AS gic_diff
                        FROM gic3
                        ORDER BY gvkey,indfrom,indthru;""",
        },




        "comp_industry":{
            "query1":"""CREATE TABLE join1 AS
                        SELECT *
                        FROM comp_gics
                        JOIN comp_other 
                        ON comp_gics.gvkey = comp_other.gvkey AND comp_gics.date = comp_other.date
                        ORDER BY gvkey,date;""",
            "query2":"""CREATE TABLE join2 AS 
                        SELECT *,LAG(date) OVER(PARTITION BY gvkey ORDER BY date) AS lag_date,
                            INTNX_(date,-1,'day') AS date_1, 0 AS gap,
                            ROW_NUMBER() OVER(PARTITION BY gvkey ORDER BY date) AS row_number
                        FROM join1;""",
            "query3":"""UPDATE join2
                        SET gap=
                            CASE WHEN row_number!=1 AND lagdate!=date_1 THEN 1
                                 ELSE gap
                            END;""",
            "query4":"""CREATE TABLE gap1 AS
                        SELECT *, INTCK_(lagdate,date,'day','discrete') AS diff
		                FROM join3
		                WHERE gap = 1;""",
            "query7":"""CREATE TABLE joined1 AS 
                        SELECT *
                        FROM join1
                        JOIN gap3 
                        ON join1.gvkey = gap3.gvkey AND join1.date = gap3.date;"""
        },




        "comp_sic_naics":{
            "query1":"""CREATE TABLE comp1 AS
		                SELECT DISTINCT gvkey, datadate, sich AS sic, naicsh AS naics
		                FROM COMP_FUNDA;""",
            "query2":"""CREATE TABLE comp2 AS 
                        SELECT * 
                        FROM comp1
                        WHERE gvkey!='175650' OR datadate!=DATE('2005-12-31') OR naics IS NOT NULL;""",
            "query3":"""CREATE TABLE comp3 AS
		                SELECT DISTINCT gvkey, datadate, sich AS sic, naicsh AS naics
		                FROM COMP.G_FUNDA;""",
		    "query4":"""CREATE TABLE comp4 AS
		                SELECT a.gvkey AS gvkeya, a.datadate AS datea, a.sic AS sica, a.naics AS naicsa, 
			                b.gvkey AS gvkeyb, b.datadate AS dateb, b.sic AS sicb, b.naics AS naicsb
		                FROM comp2 AS a 
		                FULL OUTER JOIN comp3 AS b
		                ON a.gvkey = b.gvkey AND a.datadate = b.datadate;""",
            "query5":"""CREATE TABLE comp5 AS 
                        SELECT *,coalesce(gvkeya, gvkeyb) AS gvkey, coalesce(datea, dateb) AS date,
                            coalesce(sica, sicb) AS sic, coalesce(naicsa, naicsb) AS naics
                        FROM comp4;""",
            "query6":"""CREATE TABLE comp5_sorted AS
                        SELECT *
                        FROM comp5
                        ORDER BY gvkey,date DESC;""",
            "query7":"""CREATE TABLE comp6 AS
                        SELECT *,LAG(date) OVER (GROUP BY gvkey ORDER BY gvkey,date DESC) AS date_l,
                            NULL AS valid_to
                        FROM comp5_sorted;""",
            "query8":"""UPDATE comp6
                        SET valid_to=
                        CASE WHEN date_l IS NOT NULL THEN INTNX_('day',date_l,-1)
                             ELSE date 
                        END;""",
            "query9":"""CREATE TABLE comp7 AS
                        SELECT *,INTCK_('day',date,valid_to,'discrete') AS comp_diff
                        ORDER BY gvkey,date,valid_to;""",
        },





        "compustat_fx":{
            "query1":"""CREATE TABLE __fx1 AS
                        SELECT DISTINCT a.tocurd AS curcdd, a.datadate, b.exratd/a.exratd AS fx 
                        FROM comp_exrt_dly a, comp_exrt_dly b
		                WHERE a.fromcurd='GBP' and b.tocurd='USD' AND a.fromcurd=b.fromcurd AND a.datadate=b.datadate;""",
            "query2":"""INSERT INTO __fx1 (curcdd,datadate,fx)
                        VALUES ('USD',DATE('1950-01-01'),1);""",
            "query3":"""CREATE TABLE __fx2 AS
                        SELECT * 
                        FROM __fx1
                        ORDER BY curcdd,datadate DESC;""",
            "query4":"""CREATE TALE __fx3 AS
                        SELECT *,datadate AS date,
                            LAG(datadate) OVER(PARTITION BY curcdd ORDER BY datadate DESC) AS following
                        FROM __fx2;""",
            "query5":"""UPDATE __fx3
                        SET following=DATE(date,'+1 day')
                        WHERE following IS NULL;""",
            "query6":"""UPDATE __fx3
                        SET n=JULIANDAY(following)-JULIANDAY(date);""",
            "query7":"""UPDATE __fx3
                        SET date=DATE(datadate,'+' || n || ' days');""",
            "query8_1":"""ALTER TABLE __fx3 DROP COLUMN datadate;""",
            "query8_2": """ALTER TABLE __fx3 DROP COLUMN following;""",
            "query8_3": """ALTER TABLE __fx3 DROP COLUMN n;""",
            "query9":"""CREATE TABLE {out} AS
                        SELECT DISTINCT *
                        FROM __fx3
                        ORDER BY curcdd date;""",
            "query10_1":"""DROP TABLE IF EXISTS __fx1;""",
            "query10_2": """DROP TABLE IF EXISTS __fx2;""",
            "query10_3": """DROP TABLE IF EXISTS __fx3;""",
        },





        "create_acc_chars": {
            "query1": """CREATE TABLE __chars3 AS 
                                SELECT * 
                                FROM {data}
                                ORDER BY gvkey,curcd,datadate;""",
            "query2": """CREATE TABLE __chars4 AS
                                SELECT *, ROW_NUMBER() OVER (
                                    PARTITION BY gvkey,curcd
                                    ORDER BY gvkey,curcd
                                ) AS count
                                FROM __chars3;""",
            "query3": """CREATE TABLE __chars5 AS
                                SELECT at_x AS assets,
                                       sale_x AS sales,
                                       be_x AS book_equity,
                                       ni_x AS net_income,

                                       capx/at_x AS capx_at,
                                       xrd/at_x AS rd_at,

                                       spi/at_x AS spi_at,
                                       xido_x/at_x AS xido_at,
                                       (spi+xido_x)/at_x AS nri_at,

                                       gp_x/sale_x AS gp_sale,
                                       ebitda_x/sale_x AS ebitda_sale,
                                       ebit_x/sale_x AS ebit_sale,
                                       pi_x/sale_x AS pi_sale,
                                       ni_x/sale_x AS ni_sale,
                                       ni/sale_x AS nix_sale,
                                       ocf_x/sale_x AS ocf_sale,
                                       fcf_x/sale_x AS fcf_sale,

                                       gp_x/at_x AS gp_at,
                                       ebitda_x/at_x AS ebitda_at,
                                       ebit_x/at_x AS ebit_at,
                                       fi_x/at_x AS fi_at,
                                       cop_x/at_x AS cop_at,
                                       ni_x/at_x AS ni_at,

        		                       ope_x/be_x AS ope_be,												
        		                       ni_x/be_x AS ni_be,						
        		                       nix_x/be_x AS nix_be,
        		                       ocf_x/be_x AS ocf_be,
        		                       fcf_x/be_x AS fcf_be,

        		                       gp_x/bev_x AS gp_bev,
        		                       ebitda_x/bev_x AS ebitda_bev,
        		                       ebit_x/bev_x AS ebit_bev,				
        		                       fi_x/bev_x AS fit_bev,					
        		                       cop_x/bev_x AS cop_bev,			

        		                       gp_x/ppent AS gp_ppen,
        		                       ebitda_x/ppent AS ebitda_ppen,
        		                       fcf_x/ppent AS fcf_ppen,

        		                       fincf_x/at_x AS fincf_at,
        		                       netis_x/at_x AS netis_at,
        		                       eqnetis_x/at_x AS eqnetis_at,
        		                       eqis_x/at_x AS eqis_at,
        		                       dbnetis_x/at_x AS dbnetis_at,
        		                       dltnetis_x/at_x AS dltnetis_at,
        		                       dstnetis_x/at_x AS dstnetis_at,

        		                       eqnpo_x/at_x AS eqnpo_at,
        		                       eqbb_x/at_x AS eqbb_at,
        		                       div_x/at_x AS div_at,

        		                       be_x/bev_x AS be_bev,
        		                       debt_x/bev_x AS debt_bev,
        		                       che/bev_x AS cash_bev,
        		                       pstk_x/bev_x AS pstk_bev,
        		                       dltt/bev_x AS debtlt_bev,
        		                       dlc/bev_x AS debtst_bev,

        		                       xint/debt_x AS int_debt,
        		                       xint/dltt AS int_debtlt,
        		                       ebitda_x/debt_x AS ebitda_debt,
        		                       ebitda_x/cl_x AS profit_cl,
        		                       ocf_x/cl_x AS ocf_cl,
        		                       ocf_x/debt_x AS ocf_debt,
        		                       che/lt AS cash_lt,
        		                       invt/act AS inv_act,
        		                       rect/act AS rec_act,
        		                       dlc/debt_x AS debtst_debt,
        		                       cl_x/lt AS cl_lt,
        		                       dltt/debt_x AS debtlt_debt,
        		                       lt/ppent AS lt_ppen,
        		                       dltt/be_x AS debtlt_be,
        		                       opex_x/at_x AS opex_at,
        		                       nwc_x/at_x AS nwc_at,

        		                       debt_x/at_x AS debt_at,
        		                       debt_x/be_x AS debt_be,
        		                       ebit_x/xint AS ebit_int
                                FROM __chars4;""",
        },






        "crsp_industry": {
            "query1": """CREATE TABLE permno0 AS
                            SELECT DISTINCT permno,permco,namedt,nameendt,siccd AS sic,cast(naics AS INTEGER) AS naics
                            FROM crsp.dsenames
                            ORDER BY permno, namedt, nameendt;""",
            "query2": """UPDATE permno0
                            SET sic=
                            CASE 
                                WHEN sic IS NULL THEN -999
                                WHEN sic=0 THEN -999
                                WHEN naics IS NULL THEN -999
                                ELSE sic
                            END;""",
            "query3": """UPDATE permno0
                            SET permno_diff=julianday(nameendt)-julianday(namedt)
                            END;""",
            "query4": """CREATE TABLE permno2 AS 
                            SELECT * FROM permno0
                            ORDER BY permno,namedt,nameendt""",
            "query5_1": """CREATE TABLE permno3 AS
                            SELECT * FROM permno2;""",
            "query5_2": """UPDATE permno3
                              SET namedt=DATE(namedt,'{} days')""",
            "query5_3": """ALTER TABLE permno3 DROP COLUMN nameendt;""",
            "query5_4": """ALTER TABLE permno3 DROP COLUMN nameendt;""",
            "query6": """CREATE TABLE permno4 AS
                            SELECT *, CASE WHEN sic = -999 THEN NULL ELSE sic END AS sic, 
                            CASE WHEN naics = -999 THEN NULL ELSE naics END AS naics,
                            date=namedt
                            FROM permno0;""",
            "query6_1": """ALTER TABLE permno4 DROP COLUMN namedt;""",
            "query7": """CREATE TABLE {out} AS 
                            SELECT DISTINCT * FROM permno4
                            ORDER BY permno,date;""",
            "query8_1": """DROP TABLE IF EXISTS permno0;""",
            "query8_2": """DROP TABLE IF EXISTS permno2;""",
            "query8_3": """DROP TABLE IF EXISTS permno3;""",
            "query8_4": """DROP TABLE IF EXISTS permno4;""",
        },







        "earnings_persistence":{
            "query1":"""CREATE TABLE __acc2 AS
                        SELECT *, ROW_NUMBER() OVER (PARTITION BY gvkey, curcd) AS count
                        FROM __acc1;""",
            "query2":"""CREATE TABLE __acc3 AS 
                        SELECT *, 
                            CASE WHEN at_x>0 THEN ni_x/at_x 
                                 ELSE NULL
                            END AS __ni_at, 
                            CASE WHEN count>12 THEN LAG(__ni_at,12) OVER(PARTITION BY gvkey) 
                                 ELSE NULL
                            END AS __ni_at_l1
                        FROM __acc2;""",
            "query3":"""CREATE TABLE __acc4 AS 
                        SELECT gvkey, curcd, datadate, __ni_at, __ni_at_l1
		                FROM __acc3
		                WHERE __ni_at IS NOT NULL AND __ni_at_l1 IS NOT NULL;""",
            "query4":"""CREATE TABLE month_ends AS 
                        SELECT DISTINCT datadate
                        FROM __acc4
                        ORDER BY datadate;""",
            "query5":"""CREATE TABLE dates_apply AS 
                        SELECT *, mod(monotonic(), {__months}) AS grp
		                FROM month_ends;""",
            "query6":"""CREATE TABLE calc_dates AS
			            SELECT a.datadate, b.datadate AS calc_date
			            FROM dates_apply AS a 
			            LEFT JOIN dates_apply(where=(grp={__grp})) AS b
			            ON a.datadate>INTNX_("year",b.datadate,-{__n}, "e") AND a.datadate<=b.datadate AND month(a.datadate) = month(b.datadate);""",
            "query7":"""CREATE TABLE calc_data AS 
                        SELECT a.*, b.calc_date
                        FROM __acc4 AS a 
                        LEFT JOIN calc_dates AS b
                        ON a.datadate = b.datadate
                        WHERE b.calc_date IS NOT NULL
			            GROUP BY a.gvkey, a.curcd, b.calc_date
			            HAVING count(*) >= &__min.
			            ORDER BY a.gvkey, b.calc_date;""",
            "query8":""" """,
            "query9":"""CREATE TABLE __earn_pers2 AS
			            SELECT gvkey, curcd, calc_date AS datadate, __ni_at_l1 AS ni_ar1, sqrt(_rmse_**2*_edf_/(_edf_+1)) AS ni_ivol
			            FROM __earn_pers1
			            WHERE (_edf_ + 2) >= {__min};""",
        },









        "ff_ind_class":{
            "query1":"""CREATE TABLE {out} AS 
                        SELECT *,
                            CASE 
                                WHEN sic BETWEEN 100 AND 999 THEN 1
                                WHEN sic BETWEEN 1000 AND 1299 THEN 2
                                WHEN sic BETWEEN 1300 AND 1300 THEN 3
                                WHEN sic BETWEEN 1400 AND 1499 THEN 4
                                WHEN sic BETWEEN 1500 AND 1799 THEN 5
                                WHEN sic BETWEEN 2000 AND 2099 THEN 6
                                WHEN sic BETWEEN 2100 AND 2199 THEN 7
                                WHEN sic BETWEEN 2200 AND 2299 WHEN 8
                                WHEN sic BETWEEN 2300 AND 2399 THEN 9
                                WHEN sic BETWEEN 2400 AND 2499 THEN 10
                                WHEN sic BETWEEN 2500 AND 2599 THEN 11
                                WHEN sic BETWEEN 2600 AND 2661 THEN 12
                                WHEN sic BETWEEN 2700 AND 2799 THEN 13
                                WHEN sic BETWEEN 2800 AND 2899 THEN 14
                                WHEN sic BETWEEN 2900 AND 2999 THEN 15
                                WHEN sic BETWEEN 3000 AND 3099 THEN 16
                                WHEN sic BETWEEN 3100 AND 3199 THEN 17
                                WHEN sic BETWEEN 3200 AND 3299 THEN 18
                                WHEN sic BETWEEN 3300 AND 3399 THEN 19
                                WHEN sic BETWEEN 3400 AND 3499 THEN 20
                                WHEN sic BETWEEN 3500 AND 3599 THEN 21
                                WHEN sic BETWEEN 3600 AND 3699 THEN 22
                                WHEN sic BETWEEN 3700 AND 3799 THEN 23
                                WHEN sic BETWEEN 3800 AND 3879 THEN 24
                                WHEN sic BETWEEN 3900 AND 3999 THEN 25
                                WHEN sic BETWEEN 4000 AND 4799 THEN 26
                                WHEN sic BETWEEN 4800 AND 4829 THEN 27
                                WHEN sic BETWEEN 4830 AND 4899 THEN 28
                                WHEN sic BETWEEN 4900 AND 4949 THEN 29
                                WHEN sic BETWEEN 4950 AND 4959 THEN 30
                                WHEN sic BETWEEN 4960 AND 4969 THEN 31
                                WHEN sic BETWEEN 4970 AND 4979 THEN 32
                                WHEN sic BETWEEN 5000 AND 5199 THEN 33
                                WHEN sic BETWEEN 5200 AND 5999 THEN 34
                                WHEN sic BETWEEN 6000 AND 6999 THEN 35
                                WHEN sic BETWEEN 7000 AND 8999 THEN 36 
                                WHEN sic BETWEEN 9000 AND 9999 THEN 37
                                ELSE NULL
                            END AS ff38
                        FROM {data}""",
            "query2":"""CREATE TABLE {out} AS 
                        SELECT *,
                            CASE 
                                WHEN (sic = 2048) OR (sic BETWEEN 100 AND 299) OR (sic BETWEEN 700 AND 799) 
                                    OR (sic BETWEEN 910 AND 919) THEN 1
                                WHEN (sic IN (2095, 2098, 2099)) OR (sic BETWEEN 2000 AND 2046) OR (sic BETWEEN 2050 AND 2063) 
                                    OR (sic BETWEEN 2070 AND 2079) OR (sic BETWEEN 2090 AND 2092) THEN 2
                                WHEN (sic IN (2086, 2087, 2096, 2097)) OR (sic BETWEEN 2064 AND 2068) THEN 3
                                WHEN (sic = 2080) OR (sic BETWEEN 2082 AND 2085) THEN 4
                                WHEN (sic BETWEEN 2100 AND 2199) THEN 5
                                WHEN (sic IN (3732, 3930, 3931)) OR (sic BETWEEN 920 AND 999) OR (sic BETWEEN 3650 AND 3652) 
                                    OR (sic BETWEEN 3940 AND 3949) THEN 6
                                WHEN (sic IN (7840, 7841, 7900, 7910, 7911, 7980)) OR (sic BETWEEN 7800 AND 7833) 
                                    OR (sic BETWEEN 7920 AND 7933) OR (sic BETWEEN 7940 AND 7949) OR (sic BETWEEN 7990 AND 7999) THEN 7
                                WHEN (sic IN (2770, 2771)) OR (sic BETWEEN 2700 AND 2749) OR (sic BETWEEN 2780 AND 2799) THEN 8
                                WHEN (sic IN (2047, 2391, 2392, 3160, 3161, 3229, 3260, 3262, 3263, 3269, 3230, 3231, 3750, 3751, 3800, 3860, 3861, 3910, 3911, 3914, 3915, 3991, 3995)) OR (sic BETWEEN 2510 AND 2519) OR (sic BETWEEN 2590 AND 2599) OR (sic BETWEEN 2840 AND 2844) OR (sic BETWEEN 3170 AND 3172) OR (sic BETWEEN 3190 AND 3199) OR (sic BETWEEN 3630 AND 3639) OR (sic BETWEEN 3870 AND 3873) OR (sic BETWEEN 3960 AND 3962) THEN 9
                                WHEN (sic IN (3020, 3021, 3130, 3131, 3150, 3151)) OR (sic BETWEEN 2300 AND 2390) OR (sic BETWEEN 3100 AND 3111) OR (sic BETWEEN 3140 AND 3149) OR (sic BETWEEN 3963 AND 3965) THEN 10
                                WHEN (sic BETWEEN 8000 AND 8099) THEN 11
                                WHEN (sic IN (3693, 3850, 3851)) OR (sic BETWEEN 3840 AND 3849) THEN 12
                                WHEN (sic IN (2830, 2831)) OR (sic BETWEEN 2833 AND 2836) THEN 13
                                WHEN (sic BETWEEN 2800 AND 2829) OR (sic BETWEEN 2850 AND 2879) OR (sic BETWEEN 2890 AND 2899) THEN 14
                                when (sic in (3031, 3041)) or (sic BETWEEN 3050 AND 3053) or (sic BETWEEN 3060 AND 3099) then 15
                                when (sic BETWEEN 2200 AND 2284) or (sic BETWEEN 2290 AND 2295) or (sic BETWEEN 2297 AND 2299) or (sic BETWEEN 2393 AND 2395) or (sic BETWEEN 2397 AND 2399) then 16
                                when (sic in (2660, 2661, 3200, 3210, 3211, 3240, 3241, 3261, 3264, 3280, 3281, 3446, 3996)) or 
                                    (sic BETWEEN 800 AND 899) or (sic BETWEEN 2400 AND 2439) or (sic BETWEEN 2450 AND 2459) or (sic BETWEEN 2490 AND 2499) or 
                                    (sic BETWEEN 2950 AND 2952) or (sic BETWEEN 3250 AND 3259) or (sic BETWEEN 3270 AND 3275) or (sic BETWEEN 3290 AND 3293) or 
                                    (sic BETWEEN 3295 AND 3299) or (sic BETWEEN 3420 AND 3429) or (sic BETWEEN 3430 AND 3433) or (sic BETWEEN 3440 AND 3442) or 
                                    (sic BETWEEN 3448 AND 3452) or (sic BETWEEN 3490 AND 3499) then 17
                                when (sic BETWEEN 1500 AND 1511) or (sic BETWEEN 1520 AND 1549) or (sic BETWEEN 1600 AND 1799) then 18
                                when (sic = 3300) or (sic BETWEEN 3310 AND 3317) or (sic BETWEEN 3320 AND 3325) or (sic BETWEEN 3330 AND 3341) or (sic BETWEEN 3350 AND 3357) or (sic BETWEEN 3360 AND 3379) or (sic BETWEEN 3390 AND 3399) then 19
                                when (sic in (3400, 3443, 3444)) or (sic BETWEEN 3460 AND 3479) then 20
                                when (sic in (3538, 3585, 3586)) or (sic BETWEEN 3510 AND 3536) or (sic BETWEEN 3540 AND 3569) or (sic BETWEEN 3580 AND 3582) or (sic BETWEEN 3589 AND 3599 then 21
					            when (sic in (3600, 3620, 3621, 3648, 3649, 3660, 3699)) or (sic BETWEEN 3610 AND 3613) or (sic BETWEEN 3623 AND 3629) or (sic BETWEEN 3640 AND 3646 or (sic BETWEEN 3690 AND 3692) then 22
					            when (sic in (2296, 2396, 3010, 3011, 3537, 3647, 3694, 3700, 3710, 3711, 3799)) or (sic BETWEEN 3713 AND 3716) or (sic BETWEEN 3790 AND 3792) then 23
					            when (sic in (3720, 3721, 3728, 3729)) or (sic BETWEEN 3723 AND 3725) then 24
					            when (sic in (3730, 3731)) or (sic BETWEEN 3740 AND 3743) then 25
					            when (sic = 3795) or (sic BETWEEN 3760 AND 3769) or (sic BETWEEN 3480 AND 3489) then 26
					            when (sic = 3795) or (sic BETWEEN 3760 AND 3769) or (sic BETWEEN 3480 AND 3489) then 26
					            when (sic BETWEEN 1040 AND 1049) then 27
					            when (sic BETWEEN 1000 AND 1039) or (sic BETWEEN 1050 AND 1119) or (sic BETWEEN 1400 AND 1499) then 28
					            when (sic BETWEEN 1200 AND 1299) then 29
					            when (sic in (1300, 1389)) or (sic BETWEEN 1310 AND 1339) or (sic BETWEEN 1370 AND 1382) or (sic BETWEEN 2900 AND 2912) or (sic BETWEEN 2990 AND 2999) then 30
					            when (sic in (4900, 4910, 4911, 4939)) or (sic BETWEEN 4920 AND 4925) or (sic BETWEEN 4930 AND 4932 or (sic BETWEEN 4940 AND 4942) then 31
					            when (sic in (4800, 4899)) or (sic BETWEEN 4810 AND 4813) or (sic BETWEEN 4820 AND 4822) or (sic BETWEEN 4830 AND 4841) or (sic BETWEEN 4880 AND 4892) then 32
					            when (sic in (7020, 7021, 7200, 7230, 7231, 7240, 7241, 7250, 7251, 7395, 7500, 7600, 7620, 7622, 7623, 7640, 7641)) or (sic BETWEEN 7030 AND 7033) or (sic BETWEEN 7210 AND 7212) or (sic BETWEEN 7214 AND 7217) or (sic BETWEEN 7219 AND 7221) or (sic BETWEEN 7260 AND 7299) or (sic BETWEEN 7520 AND 7549) or (sic BETWEEN 7629 AND 7631) or (sic BETWEEN 7690 AND 7699) or (sic BETWEEN 8100 AND 8499) or (sic BETWEEN 8600 AND 8699) or (sic BETWEEN 8800 AND 8899) or (sic BETWEEN 7510 AND 7515) then 33
					            when (sic in (3993, 7218, 7300, 7374, 7396, 7397, 7399, 7519, 8700, 8720, 8721)) or (sic BETWEEN 2750 AND 2759) or (sic BETWEEN 7310 AND 7342) or (sic BETWEEN 7349 AND 7353) or (sic BETWEEN 7359 AND 7369) or (sic BETWEEN 7376 AND 7385) or (sic BETWEEN 7389 AND 7394) or (sic BETWEEN 8710 AND 8713) or (sic BETWEEN 8730 AND 8734) or (sic BETWEEN 8740 AND 8748) or (sic BETWEEN 8900 AND 8911) or (sic BETWEEN 8920 AND 8999) or (sic BETWEEN 4220 AND 4229) then 34
					            when (sic = 3695) or (sic BETWEEN 3570 AND 3579) or (sic BETWEEN 3680 AND 3689) then 35
					            when (sic = 7375) or (sic BETWEEN 7370 AND 7373) then 36
					            when (sic in (3622, 3810, 3812)) or (sic BETWEEN 3661 AND 3666) or (sic BETWEEN 3669 AND 3679) then 37
					            when (sic = 3811) or (sic BETWEEN 3820 AND 3827) or (sic BETWEEN 3829 AND 3839) then 38
					            when (sic in (2760, 2761)) or (sic BETWEEN 2520 AND 2549) or (sic BETWEEN 2600 AND 2639) or (sic BETWEEN 2670 AND 2699) or (sic BETWEEN 3950 AND 3955) then 39
					            when (sic in (3220, 3221)) or (sic BETWEEN 2440 AND 2449) or (sic BETWEEN 2640 AND 2659) or (sic BETWEEN 3410 AND 3412) then 40
					            when (sic in (4100. 4130, 4131, 4150, 4151, 4230, 4231, 4780, 4789)) or (sic BETWEEN 4000 AND 4013) or (sic BETWEEN 4040 AND 4049) or (sic BETWEEN 4110 AND 4121) or (sic BETWEEN 4140 AND 4142) or (sic BETWEEN 4170 AND 4173) or (sic BETWEEN 4190 AND 4200) or (sic BETWEEN 4210 AND 4219) or (sic BETWEEN 4240 AND 4249) or (sic BETWEEN 4400 AND 4700) or (sic BETWEEN 4710 AND 4712) or (sic BETWEEN 4720 AND 4749) or (sic BETWEEN 4782 AND 4785) then 41
					            when (sic in (5000, 5099, 5100)) or (sic BETWEEN 5010 AND 5015) or (sic BETWEEN 5020 AND 5023) or (sic BETWEEN 5030 AND 5060) or (sic BETWEEN 5063 AND 5065) or (sic BETWEEN 5070 AND 5078) or (sic BETWEEN 5080 AND 5088) or (sic BETWEEN 5090 AND 5094) or (sic BETWEEN 5110 AND 5113) or (sic BETWEEN 5120 AND 5122) or (sic BETWEEN 5130 AND 5172) or (sic BETWEEN 5180 AND 5182) or (sic BETWEEN 5190 AND 5199) then 42
					            when (sic in (5200, 5250, 5251, 5260, 5261, 5270, 5271, 5300, 5310, 5311, 5320, 5330, 5331, 5334, 5900, 5999)) or (sic BETWEEN 5210 AND 5231) or (sic BETWEEN 5340 AND 5349) or (sic BETWEEN 5390 AND 5400) or (sic BETWEEN 5410 AND 5412) or (sic BETWEEN 5420 AND 5469) or (sic BETWEEN 5490 AND 5500) or (sic BETWEEN 5510 AND 5579) or (sic BETWEEN 5590 AND 5700) or (sic BETWEEN 5710 AND 5722) or (sic BETWEEN 5730 AND 5736) or (sic BETWEEN 5750 AND 5799) or (sic BETWEEN 5910 AND 5912) or (sic BETWEEN 5920 AND 5932) or (sic BETWEEN 5940 AND 5990) or (sic BETWEEN 5992 AND 5995) then 43 
					            when (sic in (7000, 7213)) or (sic BETWEEN 5800 AND 5829) or (sic BETWEEN 5890 AND 5899) or (sic BETWEEN 7010 AND 7019) or (sic BETWEEN 7040 AND 7049) then 44
					            when (sic = 6000) or (sic BETWEEN 6010 AND 6036) or (sic BETWEEN 6040 AND 6062) or (sic BETWEEN 6080 AND 6082) or (sic BETWEEN 6090 AND 6100) or (sic BETWEEN 6110 AND 6113) or (sic BETWEEN 6120 AND 6179) or (sic BETWEEN 6190 AND 6199) then 45
					            when (sic in (6300, 6350, 6351, 6360, 6361)) or (sic BETWEEN 6310 AND 6331) or (sic BETWEEN 6370 AND 6379) or (sic BETWEEN 6390 AND 6411) then 46
                                when (sic in (6500, 6510, 6540, 6541, 6610, 6611)) or (sic BETWEEN 6512 AND 6515) or (sic BETWEEN 6517 AND 6532) or (sic BETWEEN 6550 AND 6553) or (sic BETWEEN 6590 AND 6599) then 47
					            when (sic in (6700, 6798, 6799)) or (sic BETWEEN 6200 AND 6299) or (sic BETWEEN 6710 AND 6726) or (sic BETWEEN 6730 AND 6733) or (sic BETWEEN 6740 AND 6779) or (sic BETWEEN 6790 AND 6795) then 48
					            when (sic in (4970, 4971, 4990, 4991)) or (sic BETWEEN 4950 AND 4961) then 49
					            else NULL
					        END AS ff49
					    FROM {data}""",
        },






        "firms_age":{
            "query1":"""CREATE TABLE crsp_age1 AS
                        SELECT permco, MIN(date) AS crsp_first
                        FROM crsp.msf
                        GROUP BY permco;""",
            "query2":"""CREATE TABLE comp_acc_age1 AS
                        SELECT gvkey, datadate FROM comp_funda
                        UNION
                        SELECT gvkey, datadate FROM comp_g_funda;""",
            "query3":"""CREATE TABLE comp_acc_age2 AS
                        SELECT gvkey, MIN(datadate) AS comp_acc_first
                        FROM comp_acc_age1
                        GROUP BY gvkey;""",
            "query4":"""UPDATE comp_acc_age2
                        SET comp_acc_first = strftime('%Y%m%d', date(comp_acc_first, '-1 year'))""",
            "query5":"""CREATE TABLE comp_acc_age1 AS 
                        SELECT gvkey, datadate FROM comp_funda 
                        UNION 
                        SELECT gvkey, datadate FROM comp_g_funda;""",
            "query6":"""CREATE TABLE comp_acc_age2 AS 
                        SELECT gvkey, MIN(datadate) AS comp_acc_first 
                        FROM comp_acc_age1 
                        GROUP BY gvkey;""",
            "query7":"""UPDATE comp_acc_age2 
                        SET comp_acc_first = date(comp_acc_first, '-1 year'); """,
            "query8":"""CREATE TABLE comb1 AS
                        SELECT a.id, a.eom, MIN(b.crsp_first, c.comp_acc_first, d.comp_ret_first) AS first_obs
                        FROM {data} AS a
                        LEFT JOIN crsp_age1 AS b ON a.permco = b.permco
                        LEFT JOIN comp_acc_age2 AS c ON a.gvkey = c.gvkey
                        LEFT JOIN comp_ret_age2 AS d ON a.gvkey = d.gvkey;""",
            "query9":"""CREATE TABLE comb2 AS
                        SELECT *, MIN(eom) AS first_alt 
                        FROM comb1
                        GROUP BY id;""",
            "query10":"""CREATE TABLE comb3 AS
                         SELECT *, (JULIANDAY(MIN(first_obs,first_alt))-JULIANDAY(emo))/30 AS age
                         FROM comb2;""",
            "query11_1":"""ALTER TABLE comb3 DROP COLUMN first_obs;""",
            "query11_2":"""ALTER TABLE comb3 DROP COLUMN first_alt;""",
            "query12":"""CREATE TABLE {out} AS 
                         SELECT * 
                         FROM comb3
                         ORDER BY id, eom;"""
        },





        "hgics_join":{
            "query1":"""CREATE TABLE gjoin1 AS
                        SELECT a.gvkey AS na_gvkey, a.gics AS na_gics, a.date AS na_date, b.gvkey AS g_gvkey, b.gics AS g_gics, b.date AS g_date
		                FROM na_hgics AS a 
		                FULL JOIN g_hgics AS b 
		                ON a.gvkey=b.gvkey AND a.date=b.date;""",
            "query2":"""CREATE TABLE gjoin2 AS 
                        SELECT *,coalesce(na_gvkey, g_gvkey) AS gvkey, coalesce(na_date, g_date) AS date, coalesce(na_gics, g_gics) AS gics
                        FROM gjoin1;"""
        },






        "market_returns": {
            "query1": """CREATE TABLE __common_stocks1 AS
    		                SELECT DISTINCT source_crsp, id, date, eom, excntry, obs_main, exch_main, primary_sec, common, ret_lag_dif, me, dolvol, ret, ret_local, ret_exc
    		                FROM {data}
    		                ORDER BY id, {dt_col};""",
            "query2": """CREATE TABLE __common_stocks2 AS 
                            SELECT *,LAG(me) OVER(PARTITION BY id ORDER BY date) AS me_lag1,
                                LAG(dolvol) OVER(PARTITION BY id ORDER BY date) AS dolvol_lag1
                            FROM __common_stocks1
                            ORDER BY id,date
                            """,
            "query3": """CREATE TABLE __common_stocks3 AS
    				        SELECT a.*,b.ret_exc_0_1,b.ret_exc_99_9,b.ret_0_1,b.ret_99_9,b.ret_local_0_1,b.ret_local_99_9
    				        FROM __common_stocks2 AS a
            				LEFT JOIN {wins_data} AS b
    				        ON a.eom=b.eom;""",
            "query4": """CREATE TABLE __common_stocks3 AS
    				        SELECT a.*,b.ret_exc_0_1,b.ret_exc_99_9,b.ret_0_1,b.ret_99_9,b.ret_local_0_1,b.ret_local_99_9
    				        FROM __common_stocks2 AS a
    				        LEFT JOIN {wins_data} AS b
    				        ON STRFTIME('%Y-%m',a.date)=STRFTIME('%Y-%m',b.date);""",
            "query5": """UPDATE __common_stocks3
    			            SET ret = 
    			            CASE 
    			                WHEN ret>ret_99_9 AND source_crsp=0 AND ret IS NOT NULL THEN ret_99_9
    			                WHEN reg<ret_0_1 AND source_crsp=0 AND ret IS NOT NULL THEN ret_0_1
    			                ELSE ret
    			            END;""",
            "query6": """UPDATE __common_stocks3
    			            SET ret_local=
    			            CASE
    			                WHEN ret_local>ret_local_99_9 AND source_crsp=0 AND ret_local IS NOT NULL THEN ret_local_99_9
    			                WHEN ret_local<ret_local_0_1 and source_crsp=0 AND ret_local IS NOT NULL THEN ret_local_0_1
    			                ELSE ret_local
    			            END;""",
            "query7": """UPDATE __common_stocks3
    			            SET ret_exc=ret_exc_99_9
    			            CASE
    			                WHEN ret_exc>ret_exc_99_9 AND source_crsp=0 AND ret_exc IS NOT NULL THEN ret_exc_99_9
    			                WHEN ret_exc<ret_exc_0_1 AND source_crsp=0 AND ret_exc IS NOT NULL THEN ret_exc_0_1
    			                ELSE ret_exc
    			            END;""",
            "query8_1": """ALTER TABLE __common_stocks3 DROP COLUMN ret_exc_0_1;""",
            "query8_2": """ALTER TABLE __common_stocks3 DROP COLUMN ret_exc_99_9;""",
            "query8_3": """ALTER TABLE __common_stocks3 DROP COLUMN ret_0_1;""",
            "query8_4": """ALTER TABLE __common_stocks3 DROP COLUMN ret_99_9;""",
            "query8_5": """ALTER TABLE __common_stocks3 DROP COLUMN ret_local_0_1;""",
            "query8_6": """ALTER TABLE __common_stocks3 DROP COLUMN ret_local_99_9;""",
            "query9": """CREATE TABLE mkt1 AS
    		                SELECT excntry,{dt_col},COUNT(*) AS stocks,SUM(me_lag1) AS me_lag1,
    			                SUM(dolvol_lag1) AS dolvol_lag1,SUM(ret_local*me_lag1)/SUM(me_lag1) AS mkt_vw_lcl,
    			                AVG(ret_local) AS mkt_ew_lcl,SUM(ret*me_lag1)/SUM(me_lag1) AS mkt_vw,AVG(ret) AS mkt_ew,
    			                SUM(ret_exc*me_lag1)/SUM(me_lag1) AS mkt_vw_exc,AVG(ret_exc) as mkt_ew_exc
    		                FROM __common_stocks3
    		                WHERE obs_main=1 AND exch_main=1 AND primary_sec=1 AND common=1 AND ret_lag_dif<={max_date_lag} AND me_lag1 IS NOT NULL AND ret_local IS NOT NULL
    		                GROUP BY excntry, {dt_col};""",
            "query10": """CREATE TABLE {out} AS
    			             SELECT *
    			             FROM mkt1
    			             GROUP BY excntry,STRFTIME('%Y-%m',date)
    			             HAVING stocks/MAX(stocks)>=0.25;""",
            "query11_1": "DROP TABLE IF EXISTS __common_stocks1;",
            "query11_1": "DROP TABLE IF EXISTS __common_stocks2;",
            "query11_1": "DROP TABLE IF EXISTS __common_stocks3;",
            "query11_1": "DROP TABLE IF EXISTS mkt1;",
        },




        "mispricing_factors": {
            "query1": """CREATE TABLE chars1 AS 
                            SELECT id, eom, excntry, chcsho_12m, eqnpo_12m, oaccruals_at, noa_at, at_gr1, 
                                ppeinv_gr1a, o_score, ret_12_1, gp_at, niq_at
                            FROM data
                            WHERE common=1 AND primary_sec=1 AND obs_main=1 AND exch_main = 1 AND ret_exc IS NOT NULL AND me IS NOT NULL
                            ORDER BY excntry, eom;""",
            "query2": """CREATE TEMP TABLE __subset AS
                            SELECT *
                            FROM chars1
                            GROUP BY excntry, eom
                            HAVING COUNT({v}) >= {min_stks};""",
            "query3": """CREATE TABLE chars{nums1} AS 
                            SELECT a.*, b.rank_{v}
                            FROM chars{nums2} AS a
                            LEFT JOIN __ranks AS b 
                            ON a.id=b.id AND a.eom=b.eom;""",
        },





        "nyse_size_cutoff":{
            "query1":"""CREATE TABLE nyse_stocks AS
                        SELECT *
                        FROM your_table_name
                        WHERE crsp_exchcd=1 AND obs_main=1 AND exch_main=1 AND primary_sec=1 AND common=1 AND me IS NOT NULL
                        ORDER BY eom;""",
            "query2":"""SELECT eom, COUNT(me) as n, 
                            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY me) as nyse_p1,
                            PERCENTILE_CONT(0.20) WITHIN GROUP (ORDER BY me) as nyse_p20,
                            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY me) as nyse_p50,
                            PERCENTILE_CONT(0.80) WITHIN GROUP (ORDER BY me) as nyse_p80
                        FROM nyse_stocks;"""
        },





        "prepare_daily":{
            "query1":"""CREATE TABLE dsf1 AS 
                       SELECT a.excntry, a.id, a.date, a.eom, a.prc/a.adjfct AS prc_adj, 
                           a.ret, a.ret_exc, a.dolvol AS dolvol_d, a.shares, a.tvol, b.mktrf, 
                           b.hml, b.smb_ff, b.roe, b.inv, b.smb_hxz, a.ret_lag_dif, a.bidask,
                           SUM(CASE WHEN a.ret_local = 0 THEN 1 ELSE 0 END) AS zero_obs
                        FROM {data} AS a 
                        LEFT JOIN {fcts} AS b 
                        ON a.excntry = b.excntry AND a.date = b.date
                        WHERE b.mktrf IS NOT NULL
                        GROUP BY a.id, a.eom;""",
            "query2":"""UPDATE dsf1 SET ret_exc = NULL, ret = NULL
                        WHERE ret_lag_dif > 14;""",
            "query3_1":"""ALTER TABLE dsf1 DROP COLUMN ret_lag_dif;""",
            "query3_2":"""ALTER TABLE dsf1 DROP COLUMN bidask;""",
            "query4":"""CREATE TABLE dsf1_sorted AS 
                        SELECT * FROM dsf1 ORDER BY id,date;""",
            "query5":"""CREATE TABLE mkt_lead_lag1 AS 
                        SELECT excntry, date, date(date,'start of month','+1 month','-1 day') AS eom, mktrf
                        FROM {fcts}
                        ORDER BY excntry, date DESC;""",
            "query6":"""CREATE TABLE mkt_lead_lag2 AS  
                        SELECT *, 
                            CASE 
                                WHEN excntry=LAG(excntry) OR eom=LAG(eom) THEN LAG(mktrf) OVER (ORDER BY date) 
                                ELSE NULL
                            END AS mktrf_ld1
                        FROM mkt_lead_lag1;""",
            "query7":"""CREATE TABLE mkt_lead_lag3 AS
                        SELECT * FROM mkt_lead_lag2 
                        ORDER BY excntry,date;""",
            "query8":"""CREATE TABLE mkt_lead_lag4 AS 
                        SELECT *,
                            CASE 
                                WHEN excntry=LAG(excntry) when LAG(mktrf) 
                                ELSE NULL
                            END AS mktrf_lg1
                        FROM mkt_lead_lag3;""",
            "query9":"""CREATE TABLE corr_data AS 
                        SELECT id,eom,zero_obs,
                            CASE 
                                WHEN ID=LAG(ID,2) THEN ret_exc+LAG(ret_exc)+LAG(ret_exc,2)
                                ELSE NULL
                            END AS ret_exc_31,
                            CASE 
                                WHEN ID=LAG(ID,2) THEN mktrf+LAG(mktrf)+LAG(mktrf,2)
                                ELSE NULL
                            END AS mkt_exc_31,
                        FROM dsf1;""",
            "query10":"""CREATE TABLE month_ends AS 
                         SELECT DISTINCT eom
                         FROM dsf1
                         ORDER BY eno;""",
        },





        "quarterize":{

        },




        "return_cutoffs":{
            "query1":"""CREATE TABLE base AS 
                        SELECT *, 
                        FROM {data}
                        WHERE source_crsp=1 AND common=1 AND obs_main=1 AND exch_main=1 AND primary_sec=1 AND excntry!='ZWE' AND ret_exc IS NOT NULL
                        ORDER BY {date_var};""",
            "query2":"""CREATE TABLE base AS 
                        SELECT *
                        FROM {data}
                        where common=1 AND obs_main=1 AND exch_main=1 AND primary_sec=1 AND excntry!='ZWE' AND ret_exc IS NOT NULL
                        ORDER BY {date_var};""",
            "query3":"""CREATE TABLE cutoffs AS
                        SELECT t1.{by_vars}, t1.{ret_type}, COUNT(*) AS n,
                            PERCENTILE_CONT(0.001) WITHIN GROUP (ORDER BY t1.{ret_type}) AS {ret_type}_p1,
                            PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY t1.{ret_type}) AS {ret_type}_p10,
                            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY t1.{ret_type}) AS {ret_type}_p99,
                            PERCENTILE_CONT(0.999) WITHIN GROUP (ORDER BY t1.{ret_type}) AS {ret_type}_p999
                        FROM base AS t1
                        GROUP BY t1.{by_vars};""",
            "query4":"""CREATE TABLE {out} AS 
                        SELECT *
                        FROM cutoffs;""",
            "query5":"""CREATE TABLE {out} AS
		  			    SELECT a.*, b.{ret_type}_0_1, b.{ret_type}_1, b.{ret_type}_99, b.{ret_type}_99_9
		  			    FROM {out} AS a
		  			    LEFT JOIN cutoffs AS b
		  			    ON a.eom=b.eom;""",
            "query6":"""CREATE TABLE {out} AS
		  			    SELECT a.*, b.{ret_type}_0_1, b.{ret_type}_1, b.{ret_type}_99, b.{ret_type}_99_9
		  			    FROM {out} AS a
		  			    LEFT JOIN cutoffs AS b
		  			    ON a.year=b.year AND a.month=b.month;"""
        },





        "standardized_accounting_data":{
            "query1":"""SELECT DISTINCT LOWER(name) AS qvars_q
                        FROM pragma_table_info('COMP_FUNDQ') 
                        WHERE LOWER(name) LIKE '%q'
                        UNION 
                        SELECT DISTINCT LOWER(name) AS qvars_q
                        FROM pragma_table_info('COMP_G_FUNDQ') 
                        WHERE LOWER(name) LIKE '%q';""",
            "query2":"""SELECT DISTINCT LOWER(name) AS qvars_q
                        FROM pragma_table_info('COMP_FUNDQ') 
                        WHERE LOWER(name) LIKE '%y'
                        UNION 
                        SELECT DISTINCT LOWER(name) AS qvars_q
                        FROM pragma_table_info('COMP_G_FUNDQ') 
                        WHERE LOWER(name) LIKE '%y';""",
            "query3":"""CREATE TABLE g_funda1 AS 
                        SELECT gvkey,datadate,indfmt,curcd,{keep_list},'GLOBAL' AS source,
                            ib+COALESCE(xi,0)+COALESCE(do,0) AS ni,
                            NULL AS gp,NULL AS pstkrv,NULL AS pstkl,NULL AS itcb,NULL AS xad,NULL AS txbcof
                        FROM comp_g_funda
                        WHERE {compcond} AND datadate>={start_date}""",
            "query4":"""CREATE TABLE {aname} AS
			            SELECT *
			            FROM g_funda1
			            GROUP BY gvkey, datadate
			            HAVING COUNT(*)=1 or (COUNT(*)=2 AND indfmt='INDL');""",
			"query5":"""ALTER TABLE {aname} DROP COLUMN indfmt;""",
            "query6":"""CREATE TABLE g_fundq1 AS 
                        SELECT *,'GLOBAL' AS source, ibq+COALESCE(xiq,0) AS niq,
                            ppentq+dpactq AS ppegtq,NULL AS icaptq,NULL AS niy,NULL AS txditcq,
                            NULL AS txpq,NULL AS xidoq,NULL AS xidoy,NULL AS xrdq,NULL AS xrdy,
                            NULL AS txbcofy
                        FROM comp_g_fundq 
                        WHERE {compcond} AND datadate>={start_date};""",
            "query7":"""CREATE TABLE {qname} AS
			            SELECT *
			            FROM g_fundq1
			            GROUP BY gvkey, datadate
			            HAVING COUNT(*)=1 or (COUNT(*)=2 AND indfmt='INDL');""",
			"query8":"""ALTER TABLE {qname} DROP indfmt;""",
            "query9":"""CREATE TABLE {aname} AS 
                        SELECT gvkey,datadate,curcd,{keep_list},'NA' AS source
                        FROM comp_funda
                        WHERE {compconda} AND datadate>={start_date};""",
            "query10":"""CREATE TABLE {aname} AS 
                         SELECT gvkey,datadate,fyr,fyearq,fqtr,curcdq,{keep_list},'NA' AS source
                         FROM comp_funda
                         WHERE {compconda} AND datadate>={start_date};""",
            "query11":"""CREATE TABLE __tempa AS
                         SELECT a.*, b.fx
			             FROM {aname} AS a 
			             LEFT JOIN fx AS b
			             ON a.datadate=b.date AND a.curcd=b.curcdd;""",
			"query12":"""CREATE TABLE __tempq AS 
			             SELECT a.*, b.fx
			             FROM {qname} AS a 
			             LEFT JOIN fx AS b
			             ON a.datadate=b.date AND a.curcdq=b.curcdd;""",
            "query13":"""CREATE TABLE {} AS 
                         SELECT {}
                         FROM {};""",
            "query14":"""CREATE TABLE __compq3 AS 
                         SELECT *
                         FROM __compq2;""",
            "query15":"""UPDATE __compq3_1
                         SET {var}q=
                             CASE WHEN {var}q IS NULL THEN {var}y_q
                                  ELSE {var}q
                             END;""",
            "query16":"""CREATE TABLE __compq3_2 AS
                         SELECT *, ibq AS ni_qtr, saleq AS sale_qtr, 
                             coalesce(oancfq, ibq + dpq - coalesce(wcaptq, 0)) AS ocf_qtr, 
                             {new_vars}
                         FROM __compq3_1;""",
            "query17":"""UPDATE __compq3_2 
                         SET {var}_=
                         CASE WHEN gvkey=LAG(gvkey,3) OVER(PARTITION BY gvkey) OR fyr=LAG(fyr,3) OVER(PARTITION BY gvkey) OR curcdq!=LAG(curcdq,3) OVER(PARTITION BY gvkey) OR fqtr+LAG(fqtr,1) OVER(PARTITION BY gvkey)+LAG(fqtr,2) OVER(PARTITION BY gvkey)+LAG(fqtr,3) OVER(PARTITION BY gvkey)!=10 THEN {var}+LAG({var},1) OVER(PARTITION BY gvkey)+LAG({var},2) OVER(PARTITION BY gvkey)+LAG({var},3) OVER(PARTITION BY gvkey)
                              WHEN fqtr=4 THEN {var}_y
                              ELSE NULL
                         END;""",
            "query18":"""CREATE TABLE __compq4_1 AS
                         SELECT *,ROW_NUMBER OVER(PARITION BY gvkey ORDER BY datadate DESC) AS row_number
                         FROM __compq3_2;""",
            "query19":"""CREATE TABLE __compq4_2 AS
                         SELECT *
                         FROM __compq4_1
                         WHERE row_number!=1;""",
            "query20":"""CREATE TABLE __compa2 AS
                         SELECT *, NULL AS ni_qtr, NULL AS sale_qtr, NULL AS ocf_qtr
                         FROM __compa1;""",
            "query21":"""CREATE TABLE __me_data AS
                         SELECT DISTINCT gvkey, eom, me_company AS me_fiscal 
		                 FROM {me_data}
		                 WHERE gvkey IS NOT NULL AND primary_sec=1 AND me_company IS NOT NULL AND common=1 AND obs_main=1
		                 GROUP BY gvkey, eom
		                 HAVING me_company=MAX(me_company);""",
            "query22":"""CREATE TABLE __compa3 AS
                         SELECT a.*, b.me_fiscal
		                 FROM __compa2 AS a 
		                 LEFT JOIN __me_data AS b
		                 ON a.gvkey=b.gvkey AND a.datadate=b.eom;""",
            "query23":"""CREATE TABLE __compq5 AS
                         SELECT a.*, b.me_fiscal
		                 FROM __compq4 AS a 
		                 LEFT JOIN __me_data AS b
		                 ON a.gvkey = b.gvkey AND a.datadate = b.eom;"""

        },




        "winsorize_own":{
            "query1":"""CREATE TABLE {inset}_combined AS
                        SELECT *
                        FROM {inset}
                        LEFT JOIN {inset}_percentile ON {sortvar};
                        """,
            "query2":"""UPDATE {inset}_combined
                        SET {var}=
                            CASE WHEN {var}<{var}_lower AND {var} is NOT NULL AND {var}_lower IS NOT NULL THEN NULL
                                 WHEN {var}>{var}_higher AND {var} IS NOT NULL AND {var}_higher IS NOT NULL THEN NULL
                                 ELSE {var}
                            END;""",
            "query3":"""UPDATE {outset}
                        SET {var}=
                            CASE WHEN {var}<{var}_lower AND {var} IS NOT NULL AND {var}_lower IS NOT NULL THEN {var}_lower
                                 WHEN {var}>{var}_higher AND {var} IS NOT NULL AND {var}_lower IS NOT NULL THEN {var}_higher
                                 ELSE {var}
                            END;""",
            "query4_1":"""DROP TABLE IF EXISTS {inset};""",
            "query4_2":"""DROP TABLE IF EXISTS {inset}_percentile;""",
            "query5_1":"""ALTER TABLE {outset} DROP COLUMN {var}_lower;""",
            "query5_2":"""ALTER TABLE {outset} DROP COLUMN {var}_higher;""",
        },


        "z_ranks":{
            "query1":"""CREATE TABLE __subset AS 
			            SELECT *
			            FROM {data}
			            GROUP BY excntry, eom
			            HAVING count({var})>={min};""",
            "query2":"""CREATE TABLE __ranks AS
                        SELECT excntry, id, eom, RANK() OVER (PARTITION BY excntry, eom ORDER BY var) AS rank_var
                        FROM __subset
                        ORDER BY excntry, eom;""",
            "query3":"""CREATE TABLE {out} AS 
			            SELECT excntry, id, eom, (rank_{var}-mean(rank_{var}))/std(rank_{var}) as z_{var}
			            FROM __ranks
			            WHERE rank_{var} IS NOT NULL
			            group by excntry, eom;"""
        },

    }