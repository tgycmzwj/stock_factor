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
            "create_index":"""CREATE INDEX {index_name} ON {table_name}({column_name});""",
            "drop_index":"""DROP INDEX {index_name};""",
            "change_column_type":"""CREATE TABLE {table_name}_new AS 
                                    SELECT *, CAST ({column_name} AS {column_type}) AS {column_name}_new
                                    FROM {table_name};""",
            "list_index":"""SELECT * FROM sqlite_master WHERE type='index';""",
            "list_table":"""SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name;""",
            "list_column":"""PRAGMA table_info({table_name})"""
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
				                WHEN DATE(datadate)<DATE('2001-02-01') THEN cshtrd/2
				                WHEN DATE(datadate)<=DATE('2001-12-31') THEN cshtrd/1.8
				                WHEN DATE(datadate)<DATE('2003-12-31') THEN cshtrd/1.6
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
                         SELECT *, prc_local*fx AS prc, prc_high_lcl*fx AS prc_high, prc_low_lcl*fx AS prc_low,
                             prc_local*cshoc AS me, cshtrd*prc_local*fx AS dolvol, ri_local*fx AS ri,
                             COALESCE(div,0)*fx_div AS div_tot, COALESCE(divd,0)*fx_div AS div_cash,
                             COALESCE(divsp,0)*fx_div AS div_spc
                         FROM __comp_dsf2""",
            "query11":"""CREATE TABLE __comp_msf1_temp AS 
                         SELECT *, INTNX_(datadate,0,'month','end') AS eom
                         FROM __comp_dsf3;""",
            "query12":"""CREATE TABLE __comp_msf1 AS 
                         SELECT *, 
                             max(max(prc_high/ajexdi),max(prc/ajexdi))*ajexdi AS prc_highm, 
                             min(min(prc_low/ajexdi),min(prc/ajexdi))*ajexdi AS prc_lowm,
                             sum(div_tot/ajexdi)*ajexdi AS div_totm, 
                             sum(div_cash/ajexdi)*ajexdi AS div_cashm, 
                             sum(div_spc/ajexdi)*ajexdi AS div_spcm,
                             sum(cshtrd/ajexdi)*ajexdi AS cshtrm, 
                             sum(dolvol) AS dolvolm
                         FROM __comp_msf1_temp
                         GROUP BY gvkey,iid,eom;""",
            "query13":"""CREATE TABLE __comp_msf2 AS 
                         SELECT * 
                         FROM __comp_msf1
                         WHERE prc_local IS NOT NULL AND curcdd IS NOT NULL AND prcstd IN (3, 4, 10)
                         ORDER BY gvkey, iid, eom, datadate;""",
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
						     WHEN DATE(datadate)<DATE('2001-02-01') then cshtrm/2
						     WHEN DATE(datadate)<=DATE('2001-12-31') then cshtrm/1.8
						     WHEN DATE(datadate)<DATE('2003-12-31') then cshtrm/1.6
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
                             prc_low*fx AS prc_low_new,prc*cshoc AS me,cshtrm*prc AS dolvol,ri_local*fx AS ri,
                             dvpsxm*fx_div AS div_tot,NULL AS div_cash_new,NULL AS div_spc_new
                         FROM __comp_secm1;""",
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




        "altman_z":{
            "query1":"""CREATE TABLE {data}_temp1 AS 
                        SELECT *,
                            CASE WHEN at_x<=0 THEN NULL
                                 ELSE (ca_x-cl_x)/at_x
                            END AS __z_wc,
                            CASE WHEN at_x<=0 THEN NULL
                                 ELSE re / at_x
                            END AS __z_re,
                            CASE WHEN at_x<=0 THEN NULL
                                 ELSE ebitda_x/at_x
                            END AS __z_eb,
                            CASE WHEN at_x<=0 THEN NULL
                                 ELSE sale_x/at_x
                            END AS __z_sa,
                            CASE WHEN lt<=0 THEN NULL
                                 ELSE me_fiscal/lt
                            END AS __z_me,
                            NULL AS {name}
                        FROM {data};""",
            "query2":"""UPDATE {data}_temp1
                        SET {name}=1.2*__z_wc+1.4*__z_re+3.3*__z_eb+0.6*__z_me+1.0*__z_sa;""",
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




        "apply_to_last_q":{
            "query1":""""""
        },




        "bidask_hl":{
            "query1":"""CREATE TABLE __dsf1 AS 
                        SELECT a.id, a.date, a.eom, a.bidask, a.tvol, 
                            a.prc/a.adjfct AS prc, a.prc_high/a.adjfct AS prc_high, a.prc_low/a.adjfct AS prc_low
		                FROM {data} AS a 
		                LEFT JOIN market_returns_daily AS b
		                ON a.excntry=b.excntry AND a.date=b.date
		                WHERE b.mkt_vw_exc IS NOT NULL
		                ORDER BY id, date;""",
            "query2":"""CREATE TABLE __dsf2 AS 
                        SELECT *, NULL AS prc_low_r, NULL AS prc_high_r 
                        FROM __dsf1;""",
            "query3":"""UPDATE __dsf2 AS t
                        SET prc_low_in = prc_low,
                            prc_high_in = prc_high,
                            hlreset = 0,
                            prc_high = CASE WHEN bidask = 1 OR prc_low = prc_high OR prc_low <= 0 OR prc_high <= 0 OR tvol = 0 THEN NULL ELSE prc_high END,
                            prc_low = CASE WHEN bidask = 1 OR prc_low = prc_high OR prc_low <= 0 OR prc_high <= 0 OR tvol = 0 THEN NULL ELSE prc_low END,
                            prc_low_r = CASE WHEN first.id THEN NULL ELSE prc_low_r END,
                            prc_high_r = CASE WHEN first.id THEN NULL ELSE prc_high_r END,
                            hlreset = CASE WHEN 0 < prc_low < prc_high THEN 1
                                           WHEN prc_low_r <= prc AND prc <= prc_high_r THEN 1
                                           WHEN prc < prc_low_r THEN 2
                                           WHEN prc > prc_high_r THEN 3
                                           ELSE hlreset END,
                            prc_low = CASE WHEN prc_low ^= 0 AND prc_high / prc_low > 8 THEN NULL ELSE prc_low END,
                            prc_high = CASE WHEN prc_low ^= 0 AND prc_high / prc_low > 8 THEN NULL ELSE prc_high END;""",
            "query7":"""CREATE TABLE __dsf3 AS
                        SELECT *, 0 AS retadj, prc_low AS prc_low_t, prc_high AS prc_high_t,
                            LAG(prc_low) OVER (ORDER BY id, date, eom) AS prc_low_l1,
                            LAG(prc_high) OVER (ORDER BY id, date, eom) AS prc_high_l1,
                            LAG(prc) OVER (ORDER BY id, date, eom) AS prc_l1
                            FROM __dsf2;""",
            "query8":"""UPDATE __dsf3 
                        SET prc_low_l1 = CASE WHEN id!=LAG(id) OVER (ORDER BY id, date, eom) THEN NULL ELSE prc_low_l1 END,
                            prc_high_l1 = CASE WHEN id!=LAG(id) OVER (ORDER BY id, date, eom) THEN NULL ELSE prc_high_l1 END,
                            prc_l1 = CASE WHEN id!=LAG(id) OVER (ORDER BY id, date, eom) THEN NULL ELSE prc_l1 END,
                            retadj = CASE WHEN prc_l1<prc_low AND prc_l1>0 THEN 1 WHEN prc_l1 > prc_high AND prc_l1 > 0 THEN 2 ELSE retadj END,
                            prc_high_t = CASE WHEN prc_l1<prc_low AND prc_l1>0 THEN prc_high - (prc_low - prc_l1) ELSE prc_high_t END,
                            prc_low_t = CASE WHEN prc_l1<prc_low AND prc_l1>0 THEN prc_l1 ELSE prc_low_t END,
                            prc_high_t = CASE WHEN prc_l1>prc_high AND prc_l1>0 THEN prc_l1 ELSE prc_high_t END,
                            prc_low_t = CASE WHEN prc_l1>prc_high AND prc_l1>0 THEN prc_low + (prc_l1 - prc_high) ELSE prc_low_t END;""",
            "query9":"""CREATE TABLE __dsf4 AS
                        SELECT *, PI() AS pi, SQRT(8/PI()) AS k2, 3-2*SQRT(2) AS const, 
                            GREATEST(prc_high_t, prc_high_l1) AS prc_high_2d,
                            LEAST(prc_low_t, prc_low_l1) AS prc_low_2d,
                            CASE WHEN prc_low_t>0 AND prc_low_l1>0 THEN POWER(LOG(prc_high_t/prc_low_t),2)+POWER(LOG(prc_high_l1/prc_low_l1),2)
                                 ELSE NULL
                            END AS beta,
                            CASE WHEN prc_low_2d>0 THEN POWER(LOG(prc_high_2d/prc_low_2d),2)
                                 ELSE NULL
                            END AS gamma,
                            (SQRT(2*beta)-SQRT(beta))/const-SQRT(gamma/const) AS alpha,
                            2*(EXP(alpha)-1)/(1+EXP(alpha)) AS spread,
                            CASE WHEN spread < 0 THEN 0
                                 ELSE spread
                            END AS spread_0,
                            CASE WHEN spread IS NULL THEN NULL
                                 ELSE spread
                            END AS sigma,
                            CASE WHEN sigma < 0 THEN 0 
                                 ELSE sigma
                            END AS sigma_0
                            FROM __dsf3;""",
            "query10":"""CREATE TABLE {out} AS
                         SELECT id, eom, AVG(CASE WHEN spread_0 IS NOT NULL THEN spread_0 END) AS bidaskhl_21d, 
                             AVG(CASE WHEN sigma_0 IS NOT NULL THEN sigma_0 END) AS rvolhl_21d
                         FROM __dsf4
                         GROUP BY id, eom
                         HAVING COUNT(spread_0) > {__min_obs};""",
        },



        "chg_to_assets":{
            "query1":"""CREATE TABLE OutputTable AS
                        SELECT *, CASE WHEN count <= {horizon} OR at_x <= 0 THEN NULL
                                       ELSE ({var_gra}-LAG({var_gra}, {horizon}) OVER (ORDER BY id))/at_x 
                                  AS {var_gra}_gr_{horizon}a
                        FROM InputTable;"""
        },



        "chg_to_exp":{
            "query1":"""CREATE TABLE OutputTable AS
                        SELECT *, CASE WHEN count<=24 OR __expect<=0 THEN NULL
                                       ELSE var_ce/__expect-1
                                  END AS name_ce
                        FROM (SELECT *, (lag(var_ce,12)+lag(var_ce,24))/2 AS __expect
                              FROM InputTable) t;"""
        },




        "chg_var1_to_var2":{
            "query1":"""WITH cte AS (
                        SELECT *, CASE WHEN var2<=0 THEN NULL
                                       ELSE (var1/var2)
                                  END AS __x
                        FROM InputTable)
                        SELECT id, CASE WHEN count<={horizon} THEN NULL
                                        ELSE (__x-LAG(__x,{horizon}) OVER (ORDER BY id))
                                   END AS {name}
                        FROM cte;"""
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
            "query4":"""CREATE TABLE __fx3 AS
                        SELECT *,datadate AS date,
                            LAG(datadate) OVER(PARTITION BY curcdd ORDER BY datadate DESC) AS following,
                            NULL AS n
                        FROM __fx2;""",
            "query5":"""UPDATE __fx3
                        SET following=DATE(date,'+1 day')
                        WHERE following IS NULL;""",
            "query6":"""UPDATE __fx3
                        SET n=JULIANDAY(following)-JULIANDAY(date);""",
            "query7":"""UPDATE __fx3
                        SET date=DATE(datadate,'+' || n || ' days');""",
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
        		                       
        		                       oacc_x / at_x AS oaccruals_at,
        		                       oacc_x / ABS(nix_x) AS oaccruals_ni,
        		                       tacc_x / at_x AS taccruals_at,
        		                       tacc_x / ABS(nix_x) AS taccruals_ni,
        		                       noa_x / LAG(at_x, 12) AS noa_at,
        		                       CASE WHEN count <= 12 OR LAG(at_x, 12)<=0 THEN NULL
        		                            ELSE noa_x / LAG(at_x, 12)
        		                       END AS noa_at,
        		                       CASE WHEN ocf_x > 0 THEN fcf_x / ocf_x
        		                            ELSE NULL
        		                       END AS fcf_ocf,

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
        		                       CASE WHEN ocf_x > 0 THEN fcf_x / ocf_x
        		                            ELSE NULL
        		                       END AS fcf_ocf,

        		                       debt_x/at_x AS debt_at,
        		                       debt_x/be_x AS debt_be,
        		                       ebit_x/xint AS ebit_int,
        		                       
        		                       
        		                       xad/sale_x AS adv_sale,
        		                       xlr/sale_x AS staff_sale,
        		                       sale_x/bev_x AS sale_bev,
        		                       xrd/sale_x AS rd_sale,
        		                       sale_x/be_x AS sale_be,
        		                       CASE WHEN COALESCE(nix_x,ni_x) > 0 THEN div_x/nix_x END AS div_ni,
        		                       CASE WHEN nwc_x>0 THEN sale_x/nwc_x END AS sale_nwc,
        		                       CASE WHEN pi_x>0 THEN txt/pi_x END AS tax_pi,
        		                       
        		                       
        		                       CASE WHEN at_x<=0 THEN NULL ELSE che/at_x END AS cash_at,
        		                       CASE WHEN emp<=0 THEN NULL ELSE ni_x/emp END AS ni_emp,
        		                       CASE WHEN emp<=0 THEN NULL ELSE sale_x/emp END AS sale_emp,
        		                       CASE WHEN count<=12 OR LAG(sale_emp,12)<=0 THEN NULL ELSE (sale_x/emp)/(LAG(sale_x/emp,12) OVER ()-1) END AS sale_emp_gr1,
        		                       CASE WHEN count<=12 OR (emp-LAG(emp,12))=0 or (0.5*emp+0.5*lag12(emp))=0 THEN NULL ELSE (emp-LAG(emp,12) OVER ()/(0.5*emp+0.5*LAG(emp,12) OVER ()) END AS emp_gr1,

        		                       CASE WHEN count<=12 OR LAG(at_x,12)<=0 THEN NULL ELSE op_x/LAG(at_x,12) OVER () END AS op_atl1,
        		                       CASE WHEN count<=12 OR LAG(at_x,12)<=0 THEN NULL ELSE gp_x/LAG(at_x,12) OVER () END AS gp_atl1,
        		                       CASE WHEN count<=12 OR LAG(be_x,12)<=0 THEN NULL ELSE ope_x/LAG(be_x,12) OVER () END AS ope_bel1,
        		                       CASE WHEN count<=12 OR LAG(at_x,12)<=0 THEN NULL ELSE cop_x/LAG(at_x,12) OVER () END AS cop_atl1,
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
            "query8":"""CREATE TABLE __earn_pers1 AS
                        SELECT gvkey, curcd, calc_date,
                            (SUM(__ni_at_l1 * __ni_at_l1) - SUM(__ni_at_l1) * SUM(__ni_at_l1) / COUNT(*)) / (COUNT(*) - 1) AS variance,
                            (SUM(__ni_at_l1 * __ni_at) - SUM(__ni_at_l1) * SUM(__ni_at) / COUNT(*)) / (COUNT(*) - 1) AS covariance,
                            (SUM(__ni_at * __ni_at) - SUM(__ni_at) * SUM(__ni_at) / COUNT(*)) / (COUNT(*) - 1) AS dependent_variance,
                            (SUM(__ni_at_l1 * __ni_at) - SUM(__ni_at_l1) * SUM(__ni_at) / COUNT(*)) / (COUNT(*) - 1) / (SUM(__ni_at_l1 * __ni_at_l1) - SUM(__ni_at_l1) * SUM(__ni_at_l1) / COUNT(*)) AS estimated_coefficient
                        FROM calc_data
                        GROUP BY gvkey, curcd, calc_date;""",
            "query9":"""CREATE TABLE __earn_pers2 AS
			            SELECT gvkey, curcd, calc_date AS datadate, __ni_at_l1 AS ni_ar1, sqrt(_rmse_**2*_edf_/(_edf_+1)) AS ni_ivol
			            FROM __earn_pers1
			            WHERE (_edf_+2) >= {__min};""",
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
                                WHEN (sic=2048) OR (sic BETWEEN 100 AND 299) OR (sic BETWEEN 700 AND 799) 
                                    OR (sic BETWEEN 910 AND 919) THEN 1
                                WHEN (sic IN (2095, 2098, 2099)) OR (sic BETWEEN 2000 AND 2046) OR (sic BETWEEN 2050 AND 2063) 
                                    OR (sic BETWEEN 2070 AND 2079) OR (sic BETWEEN 2090 AND 2092) THEN 2
                                WHEN (sic IN (2086, 2087, 2096, 2097)) OR (sic BETWEEN 2064 AND 2068) THEN 3
                                WHEN (sic=2080) OR (sic BETWEEN 2082 AND 2085) THEN 4
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
                                WHEN (sic IN (3031, 3041)) OR (sic BETWEEN 3050 AND 3053) OR (sic BETWEEN 3060 AND 3099) THEN 15
                                WHEN (sic BETWEEN 2200 AND 2284) OR (sic BETWEEN 2290 AND 2295) OR (sic BETWEEN 2297 AND 2299) OR (sic BETWEEN 2393 AND 2395) OR (sic BETWEEN 2397 AND 2399) THEN 16
                                WHEN (sic IN (2660, 2661, 3200, 3210, 3211, 3240, 3241, 3261, 3264, 3280, 3281, 3446, 3996)) OR 
                                    (sic BETWEEN 800 AND 899) OR (sic BETWEEN 2400 AND 2439) OR (sic BETWEEN 2450 AND 2459) OR (sic BETWEEN 2490 AND 2499) OR 
                                    (sic BETWEEN 2950 AND 2952) OR (sic BETWEEN 3250 AND 3259) OR (sic BETWEEN 3270 AND 3275) OR (sic BETWEEN 3290 AND 3293) OR 
                                    (sic BETWEEN 3295 AND 3299) OR (sic BETWEEN 3420 AND 3429) OR (sic BETWEEN 3430 AND 3433) OR (sic BETWEEN 3440 AND 3442) OR 
                                    (sic BETWEEN 3448 AND 3452) OR (sic BETWEEN 3490 AND 3499) THEN 17
                                WHEN (sic BETWEEN 1500 AND 1511) OR (sic BETWEEN 1520 AND 1549) OR (sic BETWEEN 1600 AND 1799) THEN 18
                                WHEN (sic=3300) OR (sic BETWEEN 3310 AND 3317) OR (sic BETWEEN 3320 AND 3325) OR (sic BETWEEN 3330 AND 3341) OR (sic BETWEEN 3350 AND 3357) OR (sic BETWEEN 3360 AND 3379) OR (sic BETWEEN 3390 AND 3399) THEN 19
                                WHEN (sic IN (3400, 3443, 3444)) OR (sic BETWEEN 3460 AND 3479) THEN 20
                                WHEN (sic IN (3538, 3585, 3586)) OR (sic BETWEEN 3510 AND 3536) OR (sic BETWEEN 3540 AND 3569) OR (sic BETWEEN 3580 AND 3582) OR (sic BETWEEN 3589 AND 3599 WHEN 21
					            WHEN (sic IN (3600, 3620, 3621, 3648, 3649, 3660, 3699)) OR (sic BETWEEN 3610 AND 3613) OR (sic BETWEEN 3623 AND 3629) OR (sic BETWEEN 3640 AND 3646 OR (sic BETWEEN 3690 AND 3692) THEN 22
					            WHEN (sic IN (2296, 2396, 3010, 3011, 3537, 3647, 3694, 3700, 3710, 3711, 3799)) OR (sic BETWEEN 3713 AND 3716) OR (sic BETWEEN 3790 AND 3792) THEN 23
					            WHEN (sic IN (3720, 3721, 3728, 3729)) OR (sic BETWEEN 3723 AND 3725) THEN 24
					            WHEN (sic IN (3730, 3731)) OR (sic BETWEEN 3740 AND 3743) THEN 25
					            WHEN (sic=3795) OR (sic BETWEEN 3760 AND 3769) OR (sic BETWEEN 3480 AND 3489) THEN 26
					            WHEN (sic=3795) OR (sic BETWEEN 3760 AND 3769) OR (sic BETWEEN 3480 AND 3489) THEN 26
					            WHEN (sic BETWEEN 1040 AND 1049) THEN 27
					            WHEN (sic BETWEEN 1000 AND 1039) OR (sic BETWEEN 1050 AND 1119) or (sic BETWEEN 1400 AND 1499) THEN 28
					            WHEN (sic BETWEEN 1200 AND 1299) THEN 29
					            WHEN (sic IN (1300, 1389)) OR (sic BETWEEN 1310 AND 1339) OR (sic BETWEEN 1370 AND 1382) OR (sic BETWEEN 2900 AND 2912) OR (sic BETWEEN 2990 AND 2999) THEN 30
					            WHEN (sic IN (4900, 4910, 4911, 4939)) OR (sic BETWEEN 4920 AND 4925) or (sic BETWEEN 4930 AND 4932 OR (sic BETWEEN 4940 AND 4942) THEN 31
					            WHEN (sic IN (4800, 4899)) OR (sic BETWEEN 4810 AND 4813) OR (sic BETWEEN 4820 AND 4822) OR (sic BETWEEN 4830 AND 4841) OR (sic BETWEEN 4880 AND 4892) THEN 32
					            WHEN (sic IN (7020, 7021, 7200, 7230, 7231, 7240, 7241, 7250, 7251, 7395, 7500, 7600, 7620, 7622, 7623, 7640, 7641)) OR (sic BETWEEN 7030 AND 7033) OR (sic BETWEEN 7210 AND 7212) OR (sic BETWEEN 7214 AND 7217) OR (sic BETWEEN 7219 AND 7221) OR (sic BETWEEN 7260 AND 7299) OR (sic BETWEEN 7520 AND 7549) OR (sic BETWEEN 7629 AND 7631) OR (sic BETWEEN 7690 AND 7699) OR (sic BETWEEN 8100 AND 8499) OR (sic BETWEEN 8600 AND 8699) OR (sic BETWEEN 8800 AND 8899) OR (sic BETWEEN 7510 AND 7515) THEN 33
					            WHEN (sic IN (3993, 7218, 7300, 7374, 7396, 7397, 7399, 7519, 8700, 8720, 8721)) OR (sic BETWEEN 2750 AND 2759) OR (sic BETWEEN 7310 AND 7342) OR (sic BETWEEN 7349 AND 7353) OR (sic BETWEEN 7359 AND 7369) OR (sic BETWEEN 7376 AND 7385) OR (sic BETWEEN 7389 AND 7394) OR (sic BETWEEN 8710 AND 8713) OR (sic BETWEEN 8730 AND 8734) OR (sic BETWEEN 8740 AND 8748) OR (sic BETWEEN 8900 AND 8911) OR (sic BETWEEN 8920 AND 8999) OR (sic BETWEEN 4220 AND 4229) THEN 34
					            WHEN (sic=3695) OR (sic BETWEEN 3570 AND 3579) OR (sic BETWEEN 3680 AND 3689) THEN 35
					            WHEN (sic=7375) OR (sic BETWEEN 7370 AND 7373) THEN 36
					            WHEN (sic IN (3622, 3810, 3812)) OR (sic BETWEEN 3661 AND 3666) OR (sic BETWEEN 3669 AND 3679) THEN 37
					            WHEN (sic=3811) OR (sic BETWEEN 3820 AND 3827) OR (sic BETWEEN 3829 AND 3839) THEN 38
					            WHEN (sic IN (2760, 2761)) OR (sic BETWEEN 2520 AND 2549) OR (sic BETWEEN 2600 AND 2639) OR (sic BETWEEN 2670 AND 2699) OR (sic BETWEEN 3950 AND 3955) THEN 39
					            WHEN (sic IN (3220, 3221)) OR (sic BETWEEN 2440 AND 2449) OR (sic BETWEEN 2640 AND 2659) OR (sic BETWEEN 3410 AND 3412) THEN 40
					            WHEN (sic IN (4100. 4130, 4131, 4150, 4151, 4230, 4231, 4780, 4789)) OR (sic BETWEEN 4000 AND 4013) OR (sic BETWEEN 4040 AND 4049) OR (sic BETWEEN 4110 AND 4121) OR (sic BETWEEN 4140 AND 4142) OR (sic BETWEEN 4170 AND 4173) OR (sic BETWEEN 4190 AND 4200) OR (sic BETWEEN 4210 AND 4219) OR (sic BETWEEN 4240 AND 4249) OR (sic BETWEEN 4400 AND 4700) OR (sic BETWEEN 4710 AND 4712) OR (sic BETWEEN 4720 AND 4749) OR (sic BETWEEN 4782 AND 4785) THEN 41
					            WHEN (sic IN (5000, 5099, 5100)) OR (sic BETWEEN 5010 AND 5015) OR (sic BETWEEN 5020 AND 5023) OR (sic BETWEEN 5030 AND 5060) OR (sic BETWEEN 5063 AND 5065) OR (sic BETWEEN 5070 AND 5078) OR (sic BETWEEN 5080 AND 5088) OR (sic BETWEEN 5090 AND 5094) or (sic BETWEEN 5110 AND 5113) OR (sic BETWEEN 5120 AND 5122) OR (sic BETWEEN 5130 AND 5172) OR (sic BETWEEN 5180 AND 5182) OR (sic BETWEEN 5190 AND 5199) THEN 42
					            WHEN (sic IN (5200, 5250, 5251, 5260, 5261, 5270, 5271, 5300, 5310, 5311, 5320, 5330, 5331, 5334, 5900, 5999)) OR (sic BETWEEN 5210 AND 5231) OR (sic BETWEEN 5340 AND 5349) OR (sic BETWEEN 5390 AND 5400) OR (sic BETWEEN 5410 AND 5412) OR (sic BETWEEN 5420 AND 5469) OR (sic BETWEEN 5490 AND 5500) OR (sic BETWEEN 5510 AND 5579) OR (sic BETWEEN 5590 AND 5700) OR (sic BETWEEN 5710 AND 5722) OR (sic BETWEEN 5730 AND 5736) OR (sic BETWEEN 5750 AND 5799) OR (sic BETWEEN 5910 AND 5912) OR (sic BETWEEN 5920 AND 5932) OR (sic BETWEEN 5940 AND 5990) OR (sic BETWEEN 5992 AND 5995) THEN 43 
					            WHEN (sic IN (7000, 7213)) OR (sic BETWEEN 5800 AND 5829) OR (sic BETWEEN 5890 AND 5899) OR (sic BETWEEN 7010 AND 7019) OR (sic BETWEEN 7040 AND 7049) then 44
					            WHEN (sic=6000) OR (sic BETWEEN 6010 AND 6036) OR (sic BETWEEN 6040 AND 6062) OR (sic BETWEEN 6080 AND 6082) OR (sic BETWEEN 6090 AND 6100) OR (sic BETWEEN 6110 AND 6113) OR (sic BETWEEN 6120 AND 6179) OR (sic BETWEEN 6190 AND 6199) THEN 45
					            WHEN (sic IN (6300, 6350, 6351, 6360, 6361)) OR (sic BETWEEN 6310 AND 6331) OR (sic BETWEEN 6370 AND 6379) OR (sic BETWEEN 6390 AND 6411) THEN 46
                                WHEN (sic IN (6500, 6510, 6540, 6541, 6610, 6611)) OR (sic BETWEEN 6512 AND 6515) OR (sic BETWEEN 6517 AND 6532) OR (sic BETWEEN 6550 AND 6553) OR (sic BETWEEN 6590 AND 6599) THEN 47
					            WHEN (sic IN (6700, 6798, 6799)) OR (sic BETWEEN 6200 AND 6299) OR (sic BETWEEN 6710 AND 6726) OR (sic BETWEEN 6730 AND 6733) OR (sic BETWEEN 6740 AND 6779) OR (sic BETWEEN 6790 AND 6795) THEN 48
					            WHEN (sic IN (4970, 4971, 4990, 4991)) OR (sic BETWEEN 4950 AND 4961) THEN 49
					            ELSE NULL
					        END AS ff49
					    FROM {data}""",
        },






        "finish_daily_chars":{
            "query1":"""CREATE TABLE bidask AS
                        SELECT id, eom, MAX(CASE WHEN stat='col1' THEN value END) AS value
                        FROM corwin_schultz
                        GROUP BY id, eom;""",
            "query2":"""CREATE TABLE daily_chars1 AS
                        SELECT *
                        FROM roll_21d;""",
            "query3":"""INSERT INTO daily_chars1
                        SELECT *
                        FROM roll_126d;""",
            "query4":"""INSERT INTO daily_chars1
                        SELECT *
                        FROM roll_252d;""",
            "query5":"""INSERT INTO daily_chars1
                        SELECT *
                        FROM roll_1260d;""",
            "query6":"""INSERT INTO daily_chars1
                        SELECT *
                        FROM bidask;""",
            "query7":"""CREATE TABLE daily_chars2 AS
                        SELECT id, eom, MAX(CASE WHEN stat = 'value' THEN value END) AS value
                        FROM daily_chars1
                        GROUP BY id, eom;""",
            "query8":"""CREATE TABLE daily_chars3 AS
                        SELECT *, corr_1260d*rvol_252d/__mktvol_252d AS betabab_1260d, rmax5_21d/rvol_252d AS rmax5_rvol_21d
                        FROM daily_chars2;""",
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




        "intrinsic_value":{
            "query1":"""CREATE TABLE {data}_temp1 AS
                        SELECT *,LAG(be_x,12) AS bex_l12
                        FROM {data};""",
            "query2":"""CREATE TABLE {data}_temp2 AS
                        SELECT *,
                            CASE WHEN nix_x<=0 THEN div_x/(at_x*0.06)
                                 ELSE div_x/nix_x
                            END AS __iv_po,
                            CASE WHEN count<=12 OR (be_x+bex_l12)<=0 THEN NULL
                                 ELSE nix_x/((be_x+bex_l12)/2)
                            END AS __iv_roe,
                            NULL AS __iv_be1, NULL AS {name}
                        FROM {data}_temp2;""",
            "query3":"""UPDATE {data}_temp2
                        SET __iv_be1=(1+(1-__iv_po)*__iv_roe)*be_x;""",
            "query4":"""UPDATE {data}_temp2
                        SET {name}=be_x+(__iv_roe-{r})/(1+{r})*be_x+(__iv_roe-{r})/((1+{r})*{r})*__iv_be1;""",
            "query5":"""UPDATE {data}_temp2
                        SET {name}=
                            CASE WHEN {name}<0 THEN NULL
                            ELSE {name}
                            END;""",
        },




        "kz_index":{
            "query1":"""CREATE TABLE {data}_temp1 AS 
                        SELECT *,LAG(ppent,12) AS ppent_l12
                        FROM {data};""",
            "query2":"""CREATE TABLE {data}_temp2 AS 
                        SELECT *,
                            CASE WHEN count<=12 OR ppent_l12<=0 THEN NULL 
                                 ELSE (ni_x+dp)/ppent_l12
                            END AS __kz_cf,
                            CASE WHEN at_x<=0 THEN NULL
                                 ELSE (at_x+me_fiscal-be_x)/at_x
                            END AS __kz_q,
                            CASE WHEN (debt_x+seq_x)=0 THEN NULL
                                 ELSE debt_x/(debt_x+seq_x)
                            END AS __kz_db,
                            CASE WHEN count<=12 OR ppent_l12<=0 THEN NULL
                                 ELSE div_x/ppent_l12
                            END AS __kz_dv,
                            CASE WHEN count<=12 or ppent_l12<=0 THEN NULL
                                 ELSE che/ppent_l12
                            END AS __kz_cs,
                            NULL AS {name}
                        FROM {data}_temp1;""",
            "query3":"""UPDATE {data}__temp2
                        SET {name}=-1.002*__kz_cf+0.283*__kz_q+3.139*__kz_db-39.368*__kz_dv-1.315*__kz_cs;"""
        },





        "market_beta":{
            "query1":"""CREATE TABLE __msf1 AS 
                        SELECT a.id, a.eom, a.ret_exc, a.ret_lag_dif, b.mktrf
		                FROM {data} AS a 
		                LEFT JOIN {fcts} AS b
		                ON a.excntry=b.excntry AND a.eom=b.eom
		                WHERE a.ret_local!=0 AND a.ret_exc IS NOT NULL and a.ret_lag_dif=1 AND b.mktrf IS NOT NULL;""",
            "query2":"""CREATE TABLE month_ends AS 
                        SELECT DISTINCT eom
		                FROM __msf2
		                ORDER BY eom;""",
            "query3":"""CREATE TABLE dates_apply AS 
                        SELECT *, (ROW_NUMBER() OVER()-1) % {__n} AS grp 
                        FROM month_ends;""",
            "query4":"""CREATE TABLE calc_dates AS
                        SELECT a.eom, b.eom AS calc_date
                        FROM dates_apply AS a 
                        LEFT JOIN dates_apply(where=(grp = &__grp.)) AS b
                        ON a.eom>INTNX_("month", b.eom, -{__n}, 'end') AND a.eom<=b.eom;""",
            "query5":"""CREATE TABLE calc_data AS 
                        SELECT a.*, b.calc_date
                        FROM __msf2 AS a 
                        LEFT JOIN calc_dates AS b
                        ON a.eom = b.eom
                        WHERE b.calc_date IS NOT NULL
                        GROUP BY a.id, b.calc_date
                        HAVING count(*) >= {__min}
                        ORDER BY a.id, b.calc_date;""",
            "query6":"""CREATE TABLE __capm1 AS
                        SELECT id, calc_date, SUM(ret_exc*mktrf)/SUM(mktrf*mktrf) AS beta,
                            AVG(ret_exc)-AVG(mktrf)*SUM(ret_exc*mktrf)/SUM(mktrf*mktrf) AS alpha
                        FROM calc_data
                        GROUP BY id, calc_date;""",
            "query7":"""CREATE TABLE __capm2 AS 
                        SELECT id, calc_date AS eom, mktrf AS beta_&__n.m, sqrt(_rmse_**2*_edf_/(_edf_+1)) AS ivol_capm_{__n}m
                        FROM __capm1
			            WHERE (_edf_+2)>={__min};"""
        },





        "market_chars_monthly":{
            "query1":"""CREATE TABLE __monthly_chars1 AS
                        SELECT a.id, a.date, a.eom, a.me, a.shares, a.adjfct, 
                            a.prc, a.ret, a.ret_local, a.&ret_var. as ret_x,  
                            a.div_tot, a.div_cash, a.div_spc, a.dolvol,
                            a.ret_lag_dif, (a.ret_local = 0) AS ret_zero, a.ret_exc, b.mkt_vw_exc											
                        FROM {data} AS a 
                        LEFT JOIN {market_ret} AS b
                        ON a.excntry=b.excntry AND a.eom=b.eom
                        ORDER BY a.id, a.eom;""",
            "query2":"""CREATE TABLE __stock_coverage AS 
                        SELECT id, MIN(eom) AS start_date, MAX(eom) AS end_date
                        FROM __monthly_chars1
                        GROUP BY id;""",
            "query3":"""CREATE TABLE __monthly_chars2 AS
                        SELECT a.id, a.eom, b.id IS NULL AS obs_miss, 
                            b.me, b.shares, b.adjfct, b.prc, b.ret, b.ret_local, b.ret_x, b.ret_lag_dif,
                            b.div_tot, b.div_cash, b.div_spc, b.dolvol, b.ret_zero, b.ret_exc, b.mkt_vw_exc
                        FROM __full_range AS a 
                        LEFT JOIN __monthly_chars1 AS b
                        ON a.id=b.id AND a.eom=b.eom
                        ORDER BY id, eom;"""
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
            "query4":"""CREATE TABLE __ranks AS
                        SELECT excntry, id, eom, RANK() OVER (PARTITION BY excntry, eom ORDER BY {__v}) AS rank_{__v}
                        FROM __subset;""",
            "query5":"""CREATE TABLE {out} AS
                        SELECT id, eom,
                            CASE WHEN COUNT(*)-COUNT(rank_o_score)>{min_fcts} THEN NULL
                                 ELSE (COALESCE(rank_o_score,0)+COALESCE(rank_ret_12_1,0)+COALESCE(rank_gp_at,0)+COALESCE(rank_niq_at,0))/(4-COUNT(*)+COUNT(rank_o_score))
                            END AS mispricing_perf,
                            CASE WHEN COUNT(*)-COUNT(rank_chcsho_12m)>{min_fcts} THEN NULL
                                 ELSE (COALESCE(rank_chcsho_12m,0)+COALESCE(rank_eqnpo_12m,0)+COALESCE(rank_oaccruals_at,0)+COALESCE(rank_noa_at,0)+COALESCE(rank_at_gr1,0)+COALESCE(rank_ppeinv_gr1a,0))/(6-COUNT(*)+COUNT(rank_chcsho_12m))
                            END AS mispricing_mgmt
                            FROM __ranks
                            GROUP BY id, eom;"""
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





        "ohlson_o":{
            "query1":"""CREATE TABLE {name}_temp1 AS
                        SELECT *,
                            CASE WHEN at_x<=0 THEN NULL
                                 ELSE log(at_x)
                            END AS __o_lat,
                            CASE WHEN at_x<=0 THEN NULL
                                 ELSE debt_x/at_x
                            END AS __o_lev,
                            CASE WHEN at_x<=0 THEN NULL
                                 ELSE (ca_x-cl_x)/at_x
                            END AS __o_wc,
                            CASE WHEN at_x<=0 THEN NULL
                                 ELSE nix_x / at_x
                            END AS __o_roe,
                            CASE WHEN ca_x<=0 THEN NULL
                                 ELSE cl_x/ca_x
                            END AS __o_cacl,
                            CASE WHEN lt<=0 THEN NULL
                                 ELSE (pi_x+dp)/lt
                            END AS __o_ffo,
                            CASE WHEN lt IS NULL OR at_x IS NULL THEN  NULL
                                 ELSE lt>at_x
                            END AS __o_neg_eq,
                            CASE WHEN count<=12 OR nix_x IS NULL OR LAG(nix_x,12) IS NULL THEN NULL
                                 ELSE (nix_x<0 AND LAG(nix_x,12)<0)
                            END AS __o_neg_earn,
                            CASE WHEN count<=12 OR (ABS(nix_x)+ABS(LAG(nix_x,12)))=0 IS NULL THEN NULL
                                 ELSE (nix_x-LAG(nix_x,12))/ABS(nix_x)+ABS(LAG(nix_x,12)))
                            END AS __o_nich,
                        FROM {name};""",
            "query2":"""UPDATE {data}_temp1
                        SET {name}=-1.32-0.407*__o_lat+6.03*__o_lev+1.43*__o_wc+0.076*__o_cacl-1.72*__o_neg_eq-2.37*__o_roe-1.83*__o_ffo+0.285*__o_neg_earn-0.52*__o_nich;""",
        },





        "pitroski_f":{
            "query1":"""CREATE TABLE {name}_temp1 AS
                        SELECT *,LAG(at_x,12) OVER() AS atx_l12,LAG(__f,12) OVER() AS atx_l12,
                            LAG(sale_x,12) OVER() AS salex_l12, 
                        FROM {name};""",
            "query2":"""UPDATE {name}__temp1
                        SET __f_roa=
                        CASE WHEN count<=12 OR atxl12<=0 THEN NULL
                             ELSE ni_x/atx_l12
                        END;""",
            "query3":"""UPDATE {name}__temp1
                        SET __f_croa=
                        CASE WHEN count<=12 OR atxl12<=0 THEN NULL
                             ELSE ocf_x/atx_l12
                        END;""",
            "query4":"""UPDATE {name}__temp1
                        SET __f_droa=
                        CASE WHEN count<=12 THEN NULL
                             ELSE __f_roa-LAG(__f_roa,12) OVER()
                        END;""",
            "query5":"""UPDATE {name}__temp1
                        SET __f_acc=__f_croa-__f_roa;""",
            "query6":"""UPDATE {name}__temp1
                        SET __f_lev=
                        CASE WHEN count<=12 OR at_x<=0 OR atx_l12<=0 THEN NULL
                             ELSE dltt/at_x-lag(dltt/at_x,12) OVER()
                        END;""",
            "query7":"""UPDATE {name}__temp1
                        SET __f_liq=
                        CASE WHEN count<=12 OR cl_x<=0 OR LAG(cl_x,12) OVER()<=0 THEN NULL
                             ELSE ca_x/cl_x-lag(ca_x/cl_x,12) OVER()
                        END;""",
            "query8":"""UPDATE {name}__temp1
                        SET __f_gm=
                        CASE WHEN count<=12 OR sale_x<=0 OR salex_l12<=0 THEN NULL
                             ELSE gp_x/sale_x-LAG(gp_x/sale_x,12) OVER()
                        END;""",
            "query9":"""UPDATE {name}__temp1
                        SET __f_gm=
                        CASE WHEN count<=24 OR atx_l12<=0 OR LAG(at_x,25) OVER()<=0 THEN NULL
                             ELSE sale_x/atx_l12-salex_l12/LAG(at_x,24)
                        END;""",
            "query10":"""UPDATE {name}__temp1
                         SET __f_gm=
                         CASE WHEN __f_roa IS NULL OR __f_croa IS NULL OR __f_droa IS NULL OR __f_acc IS NULL OR __f_lev IS NULL OR __f_liq IS NULL OR __f_gm IS NULL OR __f_aturn IS NULL THEN NULL
                              ELSE (__f_roa>0)+(__f_croa>0)+(__f_droa>0)+(__f_acc>0)+(__f_lev<0)+(__f_liq>0)+(coalesce(eqis_x,0)=0)+(__f_gm>0)+(__f_aturn>0)
                         END;"""
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





        "quality_minus_junk":{
            "query1":"""CREATE TABLE qmj1 AS 
		                SELECT id, eom, excntry, COALESCE(roeq_be_std*2, roe_be_std) AS __evol, 
			                gp_at, ni_be, ni_at, ocf_at, gp_sale, oaccruals_at, gpoa_ch5, roe_ch5, roa_ch5, cfoa_ch5, 
			                gmar_ch5, betabab_1260d, debt_at, o_score, z_score
		                FROM {data}
		                WHERE common=1 AND primary_sec=1 AND obs_main=1 AND exch_main=1 AND ret_exc IS NOT NULL and me IS NOT NULL
		                ORDER BY excntry, eom;""",
            "query2":"""CREATE TABLE qmj_{j} AS
			            select a.*, b.z_{__v}
			            FROM qmj{i} AS a 
			            LEFT JOIN __z AS b
			            ON a.id=b.id AND a.eom=b.eom;""",
            "query4":"""CREATE TABLE qmj18 AS
                        SELECT excntry, id, eom,
                           (z_gp_at+z_ni_be+z_ni_at+z_ocf_at+z_gp_sale+z_oaccruals_at)/6 AS __prof,
                           (z_gpoa_ch5+z_roe_ch5+z_roa_ch5+z_cfoa_ch5+z_gmar_ch5)/5 AS __growth,
                           (z_betabab_1260d+z_debt_at+z_o_score+z_z_score+z___evol)/5 AS __safety
                        FROM qmj17;""",
            "query5":"""CREATE TABLE qmj19 AS 
		                SELECT a.excntry, a.id, a.eom, b.z___prof AS qmj_prof, c.z___growth AS qmj_growth, d.z___safety AS qmj_safety
		                FROM qmj18 AS a 
		                LEFT JOIN __prof AS b 
		                ON a.excntry=b.excntry AND a.id=b.id AND a.eom=b.eom
		                LEFT JOIN __growth AS c 
		                ON a.excntry=c.excntry AND a.id=c.id AND a.eom=c.eom
		                LEFT JOIN __safety AS d 
		                ON a.excntry=d.excntry AND a.id=d.id AND a.eom=d.eom;""",
            "query6":"""CREATE TABLE qmj20 AS
                        SELECT *, (qmj_prof+qmj_growth+qmj_safety)/3 AS __qmj
                        FROM qmj19;""",
            "query7":"""CREATE TABLE {out} AS 
                        SELECT a.excntry, a.id, a.eom, a.qmj_prof, a.qmj_growth, a.qmj_safety, b.z___qmj AS qmj
                        FROM qmj20 AS a 
                        LEFT JOIN __qmj AS b 
                        ON a.excntry=b.excntry AND a.id=b.id AND a.eom=b.eom;""",
        },





        "quarterize":{

        },




        "residual_momentum":{
            "query1":"""CREATE TABLE __msf1 AS 
                        SELECT a.id, a.eom, a.ret_exc, a.ret_lag_dif, b.mktrf, b.hml, b.smb_ff, b.roe, b.inv, b.smb_hxz
                        FROM {data} AS a 
                        LEFT JOIN {fcts} AS b
                        ON a.excntry=b.excntry AND a.eom=b.eom
                        WHERE a.ret_local!=0 AND a.ret_exc IS NOT NULL and b.mktrf IS NOT NULL and ret_lag_dif=1;""",
            "query2":"""CREATE TABLE month_ends AS 
                        SELECT DISTINCT eom
		                FROM __msf2
		                ORDER BY eom;""",
            "query3":"""CREATE TABLE dates_apply AS
                        SELECT *, (ROW_NUMBER() OVER (ORDER BY eom)-1) % {__n} AS grp
                        FROM month_ends;""",
            "query4":"""CREATE TABLE calc_dates AS
                        SELECT a.eom, b.eom AS calc_date
                        FROM dates_apply AS a 
                        LEFT JOIN dates_apply AS b
                        ON a.eom>INTNX_("month", b.eom, -{__n}, "e") AND a.eom <= b.eom
                        WHERE grp = {__grp};""",
            "query5":"""CREATE TABLE calc_data AS 
                        SELECT a.*, b.calc_date
                        FROM __msf2 AS a 
                        LEFT JOIN calc_dates AS b
                        ON a.eom = b.eom
                        WHERE b.calc_date IS NOT NULL  
                        GROUP BY a.id, b.calc_date
                        HAVING count(*) >= {__min] 
                        ORDER BY a.id, b.calc_date;""",
            "query6":"""CREATE TABLE __ff3_res1 (id TEXT, calc_date DATE, residual REAL);""",
            "query7":"""INSERT INTO __ff3_res1 (id,calc_date,residual)
                        SELECT id,calc_date,ret_exc-(intercept+mktrf_coeff*mktrf+smb_ff_coeff*smb_ff+hml_coeff*hml) AS residual
                        FROM calc_data
                        WHERE NOT (hml IS NULL OR smb_ff IS NULL);""",
            "query8":"""CREATE TABLE __ff3_res2 AS 
                        SELECT *, (eom>INTNX_("month",calc_date,-{__in},'e') AND eom<=INTNX_("month",calc_date,-{__sk},'e')) AS incl  
                        FROM __ff3_res1 
                        GROUP BY id, calc_date
                        HAVING count(res) >= {__min} 
                        ORDER BY id, calc_date, eom;""",
	 		"query9":"""CREATE TABLE __ff3_res3 AS 
	 		            SELECT id,calc_date AS eom,mean(res)/std(res) AS resff3_{__in}_{__sk}
	 		            FROM __ff3_res2
	 		            WHERE incl = 1
	 				    GROUP BY id, calc_date;"""
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
                        WHERE common=1 AND obs_main=1 AND exch_main=1 AND primary_sec=1 AND excntry!='ZWE' AND ret_exc IS NOT NULL
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





        "roll_apply_daily":{
            "query1":"""CREATE TABLE dates_apply AS
                        SELECT *, (ROW_NUMBER() OVER (ORDER BY eom) - 1) % {__n} AS grp
                        FROM __month_ends;""",
            "query2":"""CREATE TABLE calc_dates AS
                        SELECT a.eom, b.eom AS calc_date
                        FROM dates_apply AS a 
                        LEFT JOIN dates_apply AS b
                        ON a.eom>INTNX_("month",b.eom,-{__n},"e") AND a.eom<=b.eom
                        WHERE grp={__grp};""",
            "query3":"""CREATE TABLE calc_data_raw AS 
                        SELECT a.*, b.calc_date
                        FROM __input AS a 
                        LEFT JOIN calc_dates AS b
                        ON a.eom = b.eom
                        WHERE b.calc_date IS NOT NULL  
                        ORDER BY a.id, b.calc_date;""",
			"query4":"""CREATE TABLE calc_data_screen AS 
			            SELECT *
			            FROM calc_data_raw
			            WHERE ret_exc IS NOT NULL AND zero_obs<10  
			            GROUP BY id, calc_date
			            HAVING count(ret_exc)>={__min};""",
            "query5":"""CREATE TABLE __rvol AS 
				        SELECT id, calc_date AS eom, std(ret_exc) AS rvol{sfx}
				        FROM calc_data_screen
				        GROUP BY id, calc_date
				        HAVING COUNT(ret_exc)>={__min};""",
            "query6":"""CREATE TABLE __rmax1 AS
                        SELECT *,ROW_NUMBER() OVER (PARTITION BY id, calc_date ORDER BY ret DESC) AS ret_rank
                        FROM calc_data_screen;""",
            "query7":"""CREATE TABLE __rmax2 AS 
                        SELECT id, calc_date AS eom, AVG(ret) AS rmax5{sfx}, MAX(ret) AS rmax1{sfx}
                        FROM __rmax1
                        WHERE ret_rank<=5
                        GROUP BY id, calc_date;""",
            "query8":"""CREATE TABLE __skew1 AS
                        SELECT id, calc_date AS eom,
                            (COUNT(ret_exc)*SUM((ret_exc-AVG(ret_exc))*(ret_exc-AVG(ret_exc))*(ret_exc-AVG(ret_exc))))/((COUNT(ret_exc)-1)*(COUNT(ret_exc)-2)*POWER(STDDEV(ret_exc),3)) AS rskew{sfx}
                        FROM calc_data_screen
                        GROUP BY id, calc_date;""",
            "query9":"""CREATE TABLE __skew2 AS
                        SELECT id, calc_date AS eom, rskew{sfx}
                        FROM __skew1
                        WHERE _freq_>={__min};""",
            "query10":"""CREATE TABLE __prc_high AS 
                         SELECT id, calc_date AS eom, prc_adj/MAX(prc_adj) AS prc_highprc{sfx}
                         FROM calc_data_screen
                         GROUP BY id, calc_date
                         HAVING date=max(date) AND count(prc_adj)>={__min};""",
            "query11":"""CREATE TABLE __ami AS 
                         SELECT id, calc_date AS eom, AVG(ABS(ret)/dolvol_d)*1e6 AS ami{sfx}
                         FROM calc_data_screen
                         GROUP BY id, calc_date
                         HAVING COUNT(dolvol_d)>={__min};""",
            "query12":"""CREATE TABLE __capm1 AS
                         SELECT id, calc_date AS eom,
                             ((COUNT(*)*SUM(mktrf*ret_exc)-SUM(mktrf)*SUM(ret_exc))/(COUNT(*)*SUM(mktrf*mktrf)-SUM(mktrf)*SUM(mktrf))) AS beta,
                             (AVG(ret_exc)-((COUNT(*)*SUM(mktrf*ret_exc)-SUM(mktrf)*SUM(ret_exc))/(COUNT(*)*SUM(mktrf*mktrf)-SUM(mktrf)*SUM(mktrf)))*AVG(mktrf)) AS alpha
                         FROM calc_data_screen
                         GROUP BY id, calc_date;""",
            "query13":"""CREATE TABLE __capm2 AS 
                         SELECT id, calc_date AS eom, mktrf AS beta{sfx}, sqrt(_rmse_**2*_edf_/(_edf_+1)) AS ivol_capm{sfx}
                         FROM __capm1
                         WHERE (_edf_+2)>={__min};""",
            "query14":"""CREATE TABLE __capm_ext1 AS
                         SELECT id, calc_date AS eom,
                             ((COUNT(*)*SUM(mktrf*ret_exc)-SUM(mktrf)*SUM(ret_exc))/(COUNT(*)*SUM(mktrf*mktrf)-SUM(mktrf)*SUM(mktrf))) AS beta,
                             (AVG(ret_exc)-((COUNT(*)*SUM(mktrf*ret_exc)-SUM(mktrf)*SUM(ret_exc))/(COUNT(*)*SUM(mktrf*mktrf)-SUM(mktrf)*SUM(mktrf)))*AVG(mktrf)) AS alpha
                         FROM calc_data_screen
                         GROUP BY id, calc_date;""",
            "query15":"""CREATE TABLE __capm_ext_res AS
                         SELECT id, calc_date AS eom, ret_exc-alpha-beta*mktrf AS residual
                         FROM calc_data_screen
                         JOIN __capm_ext1 USING (id, calc_date);""",
            "query16":"""CREATE TABLE __capm_ext2 AS 
                         SELECT id, calc_date AS eom, mktrf AS beta{sfx}, SQRT(_rmse_**2*_edf_/(_edf_+1)) AS ivol_capm{sfx}
                         FROM __capm_ext1
                         WHERE (_edf_+2)>={__min};""",
            "query17":"""CREATE TABLE __capm_ext_summary AS
                         SELECT id, calc_date AS eom, COUNT(*) AS _freq_, AVG(residual) AS _mean_,
                             SUM((residual-AVG(residual))*(residual-AVG(residual))) AS _variance_
                         FROM __capm_ext_res
                         GROUP BY id, calc_date;""",
            "query18":"""SELECT id, calc_date AS eom, (3*SUM((residual-_mean_)*(residual-_mean_)*(residual-_mean_))/(_freq_*POWER(SQRT(_variance_),3))) AS iskew_capm{sfx}
                         FROM __capm_ext_summary
                         WHERE _freq_>={__min}
                         GROUP BY id, calc_date;""",
            "query19":"""CREATE TABLE __capm_ext_coskew1 AS 
                         SELECT id,calc_date,res,mktrf-AVG(mktrf) AS mktrf_dm
                         FROM __capm_ext_res
                         GROUP BY id, calc_date;""",
			"query20":"""CREATE TABLE  __capm_ext_coskew2 AS 
	                     select id,calc_date,AVG(res*mktrf_dm**2)/(SQRT(AVG(res**2))*mean(mktrf_dm**2)) AS coskew{sfx}
	                     FROM __capm_ext_coskew1
	                     GROUP BY id, calc_date
	                     HAVING COUNT(res)>={__min};""",
            "query21":"""CREATE TABLE __capm_ext3 AS 
                         SELECT a.*, b.iskew_capm{sfx}, c.coskew{sfx}
                         FROM __capm_ext2 AS a
                         LEFT JOIN __capm_ext_skew AS b ON a.id=b.id AND a.eom=b.calc_date
                         LEFT JOIN __capm_ext_coskew2 AS c ON a.id=c.id AND a.eom=c.calc_date;""",
            "query22":"""CREATE TABLE filtered_data AS
                         SELECT *
                         FROM calc_data_screen
                         WHERE hml IS NOT NULL AND smb_ff IS NOT NULL;""",
            "query23":"""CREATE TEMPORARY TABLE __ff31 AS
                         SELECT id, calc_date, mktrf, smb_ff, hml,
                             avg(ret_exc) AS intercept,
                             avg(ret_exc) - avg(mktrf) * avg(mktrf) AS mktrf_coef,
                             avg(ret_exc) - avg(smb_ff) * avg(smb_ff) AS smb_ff_coef,
                             avg(ret_exc) - avg(hml) * avg(hml) AS hml_coef,
                             COUNT(*) AS edf
                         FROM filtered_data
                         GROUP BY id, calc_date;""",
            "query24":"""CREATE TABLE __ff3_res AS
                         SELECT id, calc_date, ret_exc-(intercept+mktrf_coef*mktrf+smb_ff_coef*smb_ff+hml_coef*hml) AS residual
                         FROM filtered_data;""",
            "query25":"""CREATE TABLE __ff32 AS 
                         SELECT id, calc_date AS eom, SQRT(_rmse_**2*_edf_/(_edf_+1)) AS ivol_ff3{sfx}
                         FROM __ff31
                         WHERE (_edf_+4)>={__min};""",
            "query26":"""CREATE TABLE __ff3_skew AS
                         SELECT id, calc_date, AVG(res) AS mean_res, SUM((res-AVG(res))*(res-AVG(res))*(res-AVG(res)))/(COUNT(res)*POWER(STDDEV(res),3)) AS skewness
                         FROM __ff3_res
                         GROUP BY id, calc_date
                         WHERE freq_>={__min};""",
            "query27":"""CREATE TABLE __ff33 AS 
                         SELECT a.*, b.iskew_ff3{sfx}
                         FROM __ff32 AS a
                         LEFT JOIN __ff3_skew AS b 
                         ON a.id=b.id AND a.eom=b.calc_date;""",
            "query28":"""CREATE TABLE __hxz4_res AS
                         SELECT t.id, t.calc_date, t.ret_exc - (h.intercept + h.mktrf * t.smb_hxz + t.roe * t.inv) AS residual
                         FROM calc_data_screen_filtered AS t
                         JOIN __hxz41 AS h ON t.id = h.id AND t.calc_date = h.calc_date;""",
            "query29":"""CREATE TABLE __hxz42 AS 
                         SELECT id, calc_date AS eom, SQRT(_rmse_**2*_edf_/(_edf_+1)) AS ivol_hxz4{sfx}
				         FROM __hxz41
				         WHERE (_edf_+5)>={__min};""",
            "query30":"""CREATE TABLE __hxz4_skew AS
                         SELECT id, calc_date, skewness(res) AS iskew_hxz4
                         FROM __hxz4_res
                         GROUP BY id, calc_date
                         HAVING COUNT(res)>={__min};""",
            "query31":"""CREATE TABLE __hxz43 AS 
                         SELECT a.*, b.iskew_hxz4{sfx}
                         FROM __hxz42 AS a
                         LEFT JOIN __hxz4_skew AS b 
                         ON a.id=b.id AND a.eom=b.calc_date;""",
            "query32":"""CREATE TABLE __dimson1 AS 
                         SELECT a.excntry, a.id, a.date, a.eom, a.ret_exc, a.mktrf, b.mktrf_lg1, b.mktrf_ld1
                         FROM calc_data_screen AS a 
                         LEFT JOIN mkt_lead_lag4 AS b
                         ON a.excntry = b.excntry AND a.date = b.date
                         WHERE b.mktrf_lg1 IS NOT NULL AND b.mktrf_ld1 IS NOT NULL;""",
            "query33":"""CREATE TABLE __dimson2 AS 
                         SELECT *
                         FROM __dimson1
                         GROUP BY id, eom
                         HAVING count(*)>=({__min}-1)""",
            "query34":"""CREATE VIEW lagged_mktrf AS
                         SELECT id, eom, mktrf, 
                             LAG(mktrf) OVER (PARTITION BY id ORDER BY eom) AS mktrf_lg1,
                             LAG(mktrf, 2) OVER (PARTITION BY id ORDER BY eom) AS mktrf_ld1
                         FROM __dimson2;""",
            "query35":"""CREATE TABLE __dimson3 AS
                         SELECT id, eom, ret_exc-(intercept+mktrf*mktrf_lg1+mktrf*mktrf_ld1) AS residual
                         FROM (SELECT *,
                             (SELECT intercept FROM regression_results WHERE id=t.id AND eom=t.eom) AS intercept,
                             (SELECT mktrf FROM regression_results WHERE id=t.id AND eom=t.eom) AS mktrf
                             FROM lagged_mktrf AS t);""",
            "query36":"""CREATE TABLE __dimson4 AS 
                         SELECT id,eom,mktrf+mktrf_lg1+mktrf_ld1 AS beta_dimson{sfx}
                         FROM __dimson3;""",
            "query37":"""CREATE VIEW filtered_data AS
                         SELECT *
                         FROM calc_data_screen
                         WHERE mktrf < 0;""",
            "query38":"""CREATE TABLE __downbeta1 AS
                         SELECT id, calc_date, intercept, mktrf AS downbeta
                         FROM (SELECT id, calc_date, AVG(ret_exc) AS intercept, SUM(ret_exc * mktrf) / SUM(mktrf * mktrf) AS mktrf
                               FROM filtered_data
                               GROUP BY id, calc_date);""",
            "query39":"""CREATE TABLE __downbeta2 AS 
                         SELECT id, calc_date AS eom, mktrf AS betadown{sfx}
                         FROM __downbeta1
				         WHERE (_edf_+2)>=({__min}/2);""",
            "query40":"""CREATE TABLE __zero_trades1 AS
                         SELECT id, calc_date AS eom, AVG(tvol=0)*21 as zero_trades, AVG(tvol/(shares*1e6)) AS turnover
                         FROM calc_data_raw
                         WHERE tvol IS NOT NULL
                         GROUP BY id, calc_date
                         HAVING count(tvol)>={__min}
                         ORDER BY eom;""",
            "query41":"""CREATE VIEW filtered_data AS
                         SELECT *
                         FROM __zero_trades1
                         WHERE zero_trades IS NOT NULL AND turnover IS NOT NULL;""",
            "query42":"""CREATE TABLE __zero_trades2 AS
                         SELECT *, RANK() OVER (PARTITION BY eom ORDER BY turnover DESC) AS rank_turnover
                         FROM filtered_data;""",
            "query43":"""CREATE TABLE __zero_trades3 AS
                         SELECT id, eom, zero_trades+rank_turnover/100 AS zero_trades{sfx} 
				         FROM __zero_trades2;""",
            "query44":"""CREATE TABLE __turnover1 AS 
                         SELECT id, date, calc_date, tvol/(shares*1e6) AS turnover_d  
				         FROM calc_data_raw;""",
            "query45":"""CREATE TABLE __turnover2 AS 
                         SELECT id, calc_date as eom, AVG(turnover_d) AS turnover{sfx}, std(turnover_d)/AVG(turnover_d) as turnover_var{sfx} 
                         FROM __turnover1
                         GROUP BY id, calc_date
                         HAVING COUNT(turnover_d)>={__min};""",
            "query46":"""CREATE TABLE __dolvol AS 
                         SELECT id, calc_date AS eom, AVG(dolvol_d) AS dolvol{sfx}, std(dolvol_d)/AVG(dolvol_d) AS dolvol_var{sfx}
                         FROM calc_data_raw
                         GROUP BY id, calc_date
                         HAVING COUNT(dolvol_d) >= {__min};""",
            "query47":"""CREATE TABLE __corr_data1 AS 
                         SELECT a.*, b.calc_date
                         FROM corr_data AS a 
                         LEFT JOIN calc_dates AS b
                         ON a.eom = b.eom
                         WHERE b.calc_date IS NOT NULL AND ret_exc_3l IS NOT NULL AND zero_obs < 10
                         ORDER BY a.id, b.calc_date;""",
            "query48":"""CREATE TABLE __corr_data2 AS 
                         SELECT *
                         FROM __corr_data1
                         GROUP BY id, calc_date
                         HAVING COUNT(ret_exc_3l) >= {__min} AND COUNT(mkt_exc_3l)>={__min};""",
            "query49":"""CREATE VIEW filtered_data AS
                         SELECT id, calc_date, ret_exc_3l, mkt_exc_3l
                         FROM __corr_data2
                         WHERE ret_exc_3l IS NOT NULL AND mkt_exc_3l IS NOT NULL;""",
            "query49":"""CREATE TABLE __corr1 AS
                         SELECT id, calc_date,CORR(ret_exc_3l, mkt_exc_3l) AS corr_ret_mkt
                         FROM filtered_data
                         GROUP BY id, calc_date;""",
            "query50":"""CREATE TABLE __corr2 AS 
                         SELECT id, calc_date AS eom, ret_exc_3l AS corr&sfx.
				         FROM __corr1
				         WHERE _type_='CORR' AND _name_ = 'mkt_exc_3l';""",
            "query51":"""CREATE TABLE __mktvol as 
                         SELECT id, calc_date AS eom, STD(mktrf) AS __mktvol{sfx}
                         FROM calc_data_screen
                         GROUP BY id, calc_date
                         HAVING COUNT(ret_exc) >= {__min};""",
            "query52":"""SELECT name AS memname
                         FROM sqlite_master
                         WHERE type = 'table' AND lower(tbl_name) = 'work' AND tbl_name REGEXP '^op_';"""
        },





        "seasonality":{
            "query1":""" """,
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




        "var_growth":{
            "query1":"""CREATE TABLE OutputTable AS
                        SELECT *, (var_gr/LAG(var_gr,{horizon}) OVER (PARTITION BY id ORDER BY eom)-1) AS name_gr
                        FROM InputTable;""",
            "query2":"""UPDATE OutputTable
                        SET name_gr = NULL
                        WHERE count<={horizon} OR LAG(var_gr,{horizon}) OVER (PARTITION BY id ORDER BY eom)<=0;""",
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