class query_storage:
    query_bank={
        "prepare_crsp_sf":{
            #query1: format
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
            #query3: sort table by permno,date
            "query3":"""CREATE TABLE __crsp_sf1_sorted AS 
                        SELECT * FROM __crsp_sf1 ORDER BY permno,date;""",
            #query4:
            "query4":"""CREATE TABLE __crsp_sf2 AS
                        SELECT *, ABS(prc) * vol AS dolvol,
                        CASE
                            WHEN ROW_NUMBER() OVER (PARTITION BY permno ORDER BY date) = 1 THEN NULL
                            ELSE (ret - retx) * LAG(prc) OVER (PARTITION BY permno ORDER BY date) * (cfacshr / LAG(cfacshr) OVER (PARTITION BY permno ORDER BY date))
                        END AS div_tot
                        FROM __crsp_sf1_sorted;""",
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
            "query6_1":"""ALTER TABLE __crsp_sf4 DROP COLUMN ret;""",
            "query6_2":"""ALTER TABLE __crsp_sf4 DROP COLUMN dlret;""",
            "query6_3":"""ALTER TABLE __crsp_sf4 DROP COLUMN dlstcd;""",
            "query6_4":"""ALTER TABLE __crsp_sf4 RENAME COLUMN dlret_new TO dlret;""",
            "query6_5":"""ALTER TABLE __crsp_sf4 RENAME COLUMN ret_new TO ret;""",
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
                       git  GROUP BY permco,date;""",
            "query9":"""UPDATE __crsp_sf6
			            SET vol=vol*100,dolvol=dolvol*100;
		                """,
            #query10: format
            "query10":"""CREATE TABLE _crsp_{freq}sf AS
                         SELECT DISTINCT *
                         FROM __crsp_sf6
                         ORDER BY permno, date;""",
            #query11
            "query11_1":"DROP TABLE IF EXISTS __crsp_sf1",
            "query11_2":"DROP TABLE IF EXISTS __crsp_sf1_sorted",
            "query11_3":"DROP TABLE IF EXISTS __crsp_sf2",
            "query11_4":"DROP TABLE IF EXISTS __crsp_sf3",
            "query11_5":"DROP TABLE IF EXISTS __crsp_sf4",
            "query11_6":"DROP TABLE IF EXISTS __crsp_sf4_temp",
            "query11_7":"DROP TABLE IF EXISTS __crsp_sf5",
            "query11_8":"DROP TABLE IF EXISTS __crsp_sf6",
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
            "query2":"""CREATE TABLE __temp AS 
                        SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY gvkey,datadate
                            ORDER BY gvkey,datadate DESC) 
                            AS row_number    
                        FROM __firm_shares1) 
                        AS rows WHERE row_number = 1;""",
            "query2_1":"""ALTER TABLE __temp DROP COLUMN row_number;""",
            "query3_1":"""CREATE TABLE __temp1 AS 
                            SELECT *,LAG(datadate) OVER (PARTITION BY gvkey ORDER BY datadate DESC) AS following,
                                ROW_NUMBER() OVER (PARTITION BY gvkey ORDER BY datadate DESC) AS row_number,
                                DATE(datadate,'+12 months','start of month','+1 months','-1 days') AS forward_max
                            FROM __temp;""",
            "query3_2":"""CREATE TABLE __temp2 AS 
                          SELECT *, CASE WHEN row_number=1 THEN NULL ELSE following END AS following_new,
                              CAST(JULIANDAY(MIN(following,forward_max))-JULIANDAY(datadate) AS int) AS n
                          FROM __temp1;""",
            "query3_3":"""CREATE TABLE __temp3 AS 
                          SELECT *,DATE(datadate,'+' || n || ' days','start of month','+1 month','-1 day') AS ddate
                          FROM __temp2;""",
            "query3_4":"""CREATE TABLE __firm_shares2 AS 
                        SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY gvkey,datadate,ddate
                            ORDER BY gvkey,datadate,ddate) 
                            AS row_number    
                        FROM __firm_shares1) 
                        AS rows WHERE row_number = 1;""",
            "query4_1":"""ALTER TABLE __firm_shares2 DROP COLUMN following;""",
            "query4_2":"""ALTER TABLE __firm_shares2 RENAME COLUMN following_new TO following;""",
            "query4_3":"ALTER TABLE __firm_shares2 DROP COLUMN forward_max;",
            "query4_4":"ALTER TABLE __firm_shares2 DROP COLUMN n;",
            "query4_5":"ALTER TABLE __firm_shares2 DROP COLUMN row_number;",
            "query4_6":"""DROP TABLE IF EXISTS __temp""",
            "query4_7": """DROP TABLE IF EXISTS __temp1""",
            "query4_8":"""DROP TABLE IF EXISTS __temp2""",
            "query4_9": """DROP TABLE IF EXISTS __temp2""",
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
        "combine_crsp_comp_sf":{
            "query1":"""CREATE TABLE __msf_world1 AS
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
                        FROM {comp_msf};"""
        },
        "mispricing_factors":{
            "query1":"""CREATE TABLE chars1 AS 
                        SELECT id, eom, excntry, chcsho_12m, eqnpo_12m, oaccruals_at, noa_at, at_gr1, 
                            ppeinv_gr1a, o_score, ret_12_1, gp_at, niq_at
                        FROM data
                        WHERE common=1 AND primary_sec=1 AND obs_main=1 AND exch_main = 1 AND ret_exc IS NOT NULL AND me IS NOT NULL
                        ORDER BY excntry, eom;""",
            "query2":"""CREATE TEMP TABLE __subset AS
                        SELECT *
                        FROM chars1
                        GROUP BY excntry, eom
                        HAVING COUNT({v}) >= {min_stks};""",
            "query3":"""CREATE TABLE chars{nums1} AS 
                        SELECT a.*, b.rank_{v}
                        FROM chars{nums2} AS a
                        LEFT JOIN __ranks AS b 
                        ON a.id=b.id AND a.eom=b.eom;""",
        },
        "add_helper_vars":{
            "query1":"""CREATE TABLE __comp_dates1 AS
                        SELECT gvkey, curcd, MIN(datadate) AS start_date, MAX(datadate) AS end_date
                        FROM {data}
                        GROUP BY gvkey, curcd;""",
            "query2":"""CREATE TABLE __helpers1 AS 
                        SELECT a.gvkey, a.curcd, a.datadate, b.*, 
                            CASE WHEN b.gvkey IS NOT NULL THEN 1 ELSE 0 END AS data_available
                        FROM __comp_dates2 AS a
                        LEFT JOIN {data} AS b ON a.gvkey=b.gvkey AND a.curcd=b.curcd AND a.datadate=b.datadate;""",
            "query3":"""CREATE TABLE __helpers1_sorted AS 
                        SELECT * FROM (
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY gvkey,curcd,datadate 
                            ORDER BY gvkey,curcd,datadate) 
                            AS row_number    
                        FROM __helpers1) 
                        AS rows WHERE row_number = 1;""",
            "query4":"""CREATE TABLE __helpers2 AS 
                        SELECT *, ROW_NUMBER() OVER (
                            PARTITION BY gvkey,curcd
                            ORDER BY gvkey,curcd
                        ) AS count
                        FROM __helpers1_sorted;""",
            "query5":"""UPDATE __helpers2
		                SET {variable} = 
			            CASE 
				            WHEN {varibale}<0 THEN NULL
				            ELSE {variable}
			            END;""",
            "query6":"""CREATE TABLE {out} AS
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
            "query7":"""ALTER TABLE {out} DROP COLUMN count;""",
            "query8_1":"""DROP TABLE IF EXISTS __comp_dates1;""",
            "query8_2":"""DROP TABLE IF EXISTS __comp_dates2;""",
            "query8_3":"""DROP TABLE IF EXISTS __helpers1;""",
            "query8_4":"""DROP TABLE IF EXISTS __helpers2;""",
        },
        "crsp_industry":{
            "query1":"""CREATE TABLE permno0 AS
                        SELECT DISTINCT permno,permco,namedt,nameendt,siccd AS sic,cast(naics AS INTEGER) AS naics
                        FROM crsp.dsenames
                        ORDER BY permno, namedt, nameendt;""",
            "query2":"""UPDATE permno0
                        SET sic=
                        CASE 
                            WHEN sic IS NULL THEN -999
                            WHEN sic=0 THEN -999
                            WHEN naics IS NULL THEN -999
                            ELSE sic
                        END;""",
            "query3":"""UPDATE permno0
                        SET permno_diff=julianday(nameendt)-julianday(namedt)
                        END;""",
            "query4":"""CREATE TABLE permno2 AS 
                        SELECT * FROM permno0
                        ORDER BY permno,namedt,nameendt""",
            "query5_1":"""CREATE TABLE permno3 AS
                        SELECT * FROM permno2;""",
            "query5_2":"""UPDATE permno3
                          SET namedt=DATE(namedt,'{} days')""",
            "query5_3":"""ALTER TABLE permno3 DROP COLUMN nameendt;""",
            "query5_4":"""ALTER TABLE permno3 DROP COLUMN nameendt;""",
            "query6":"""CREATE TABLE permno4 AS
                        SELECT *, CASE WHEN sic = -999 THEN NULL ELSE sic END AS sic, 
                        CASE WHEN naics = -999 THEN NULL ELSE naics END AS naics,
                        date=namedt
                        FROM permno0;""",
            "query6_1":"""ALTER TABLE permno4 DROP COLUMN namedt;""",
            "query7":"""CREATE TABLE {out} AS 
                        SELECT DISTINCT * FROM permno4
                        ORDER BY permno,date;""",
            "query8_1":"""DROP TABLE IF EXISTS permno0;""",
            "query8_2": """DROP TABLE IF EXISTS permno2;""",
            "query8_3": """DROP TABLE IF EXISTS permno3;""",
            "query8_4": """DROP TABLE IF EXISTS permno4;""",
        },
        "create_acc_chars":{
            ""
        }
    }