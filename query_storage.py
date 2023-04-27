class query_storage:
    query_bank={
        "prepare_crsp_sf":{
            #query1: format
            "query1":"""create table __crsp_sf1 as 
                        select a.permno, a.permco, a.date, (a.prc < 0) as bidask, abs(a.prc) as prc, a.shrout/1000 as shrout, abs(a.prc)*a.shrout/1000 as me,
	                    a.ret, a.retx, a.cfacshr, a.vol, 
		                case when a.prc > 0 and a.askhi > 0 then a.askhi else NULL end as prc_high,   
		                case when a.prc > 0 and a.bidlo > 0 then a.bidlo else NULL end as prc_low,    
		                b.shrcd, b.exchcd, c.gvkey, c.liid as iid, 
		                b.exchcd in (1, 2, 3) as exch_main			
		                from crsp_{freq}sf as a 
		                left join crsp_{freq}senames as b
		                on a.permno=b.permno and a.date>=namedt and a.date<=b.nameendt
		                left join crsp_ccmxpf_lnkhist as c
		                on a.permno=c.lpermno and (a.date>=c.linkdt or (c.linkdt IS NOT NULL)) and 
		                (a.date<=c.linkenddt or (c.linkenddt IS NOT NULL)) and c.linktype in ('LC', 'LU', 'LS');""",
            #query2:
            "query2":"""update __crsp_sf1
		                set vol = 
			            case 
				            when date < DATE('2001-02-01') then vol / 2
				            when date <= DATE('2001-12-31') then vol / 1.8
				            when date < DATE('2003-12-31') then vol / 1.6
				            else vol
			            end
		                where exchcd = 3;""",
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
                        GROUP BY permco,date;""",
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
                        FROM comp.fundq 
                        WHERE indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C' AND cshoq IS NOT NULL AND ajexq IS NOT NULL 
                        UNION 
                        SELECT gvkey, datadate, csho AS csho_fund, ajex AS ajex_fund 
                        FROM comp.funda 
                        WHERE indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C' AND csho IS NOT NULL AND ajex IS NOT NULL;
                        """,
            "query2":""" """,
        },
        "add_primary_sec":{
            "query1":"""CREATE TABLE __prihistrow AS 
                        SELECT gvkey,itemvalue AS prihistrow,effdate,thrudate 
                        FROM comp.g_sec_history 
                        WHERE item = 'PRIHISTROW';""",
            "query2":"""CREATE TABLE __prihistusa AS 
                        SELECT gvkey,itemvalue AS prihistusa,effdate,thrudate
                        FROM comp.sec_history 
                        WHERE item='PRIHISTUSA';""",
            "query3":"""CREATE TABLE __prihistcan AS 
                        SELECT gvkey, itemvalue AS prihistcan, effdate, thrudate
                        FROM comp.sec_history 
                        WHERE item = 'PRIHISTCAN';""",
            "query4":""" CREATE TABLE __header AS
                         SELECT gvkey, MAX(prirow) AS prirow, MAX(priusa) AS priusa, MAX(prican) AS prican 
                         FROM (
                         SELECT gvkey, prirow, priusa, prican FROM comp.company
                         UNION ALL
                         SELECT gvkey, prirow, priusa, prican FROM comp.g_company
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
					            when sic in (3600, 3620, 3621, 3648, 3649, 3660, 3699) or (sic BETWEEN 3610 AND 3613) or (sic BETWEEN 3623 AND 3629) or (sic BETWEEN 3640 AND 3646 or (sic BETWEEN 3690 AND 3692) then 22
					            when sic in (2296, 2396, 3010, 3011, 3537, 3647, 3694, 3700, 3710, 3711, 3799) or (sic BETWEEN 3713 AND 3716) or (sic BETWEEN 3790 AND 3792) then 23
					            when sic in (3720, 3721, 3728, 3729) or (sic BETWEEN 3723 AND 3725) then 24
					            when sic in (3730, 3731) or (sic BETWEEN 3740 AND 3743) then 25
					            when (sic = 3795) or (sic BETWEEN 3760 AND 3769) or (sic BETWEEN 3480 AND 3489) then 26
					            when (sic = 3795) or (sic BETWEEN 3760 AND 3769) or (sic BETWEEN 3480 AND 3489) then 26
					            when (sic BETWEEN 1040 AND 1049) then 27
					            when (sic BETWEEN 1000 AND 1039) or (sic BETWEEN 1050 AND 1119) or (sic BETWEEN 1400 AND 1499) then 28
					            when (sic BETWEEN 1200 AND 1299) then 29
					            when (sic in (1300, 1389)) or (sic BETWEEN 1310 AND 1339) or (sic BETWEEN 1370 AND 1382) or (sic BETWEEN 2900 AND 2912) or (sic BETWEEN 2990 AND 2999) then 30
					            when (sic in (4900, 4910, 4911, 4939)) or (sic BETWEEN 4920 AND 4925) or (sic BETWEEN 4930 AND 4932 or (sic BETWEEN 4940 AND 4942) then 31
					            when (sic in (4800, 4899)) or (sic BETWEEN 4810 AND 4813) or (sic BETWEEN 4820 AND 4822) or (sic BETWEEN 4830 AND 4841) or (sic BETWEEN 4880 AND 4892) then 32
					            when sic in (7020, 7021, 7200, 7230, 7231, 7240, 7241, 7250, 7251, 7395, 7500, 7600, 7620, 7622, 7623, 7640, 7641) or (sic BETWEEN 7030 AND 7033) or (sic BETWEEN 7210 AND 7212) or (sic BETWEEN 7214 AND 7217) or (sic BETWEEN 7219 AND 7221) or (sic BETWEEN 7260 AND 7299) or (sic BETWEEN 7520 AND 7549) or (sic BETWEEN 7629 AND 7631) or (sic BETWEEN 7690 AND 7699) or (sic BETWEEN 8100 AND 8499) or (sic BETWEEN 8600 AND 8699) or (sic BETWEEN 8800 AND 8899) or (sic BETWEEN 7510 AND 7515) then 33
					            when sic in (3993, 7218, 7300, 7374, 7396, 7397, 7399, 7519, 8700, 8720, 8721) or (sic BETWEEN 2750 AND 2759) or (sic BETWEEN 7310 AND 7342) or (sic BETWEEN 7349 AND 7353) or (sic BETWEEN 7359 AND 7369) or (sic BETWEEN 7376 AND 7385) or (sic BETWEEN 7389 AND 7394) or (sic BETWEEN 8710 AND 8713) or (sic BETWEEN 8730 AND 8734) or (sic BETWEEN 8740 AND 8748) or (sic BETWEEN 8900 AND 8911) or (sic BETWEEN 8920 AND 8999) or (sic BETWEEN 4220 AND 4229) then 34
					            when (sic = 3695) or (sic BETWEEN 3570 AND 3579) or (sic BETWEEN 3680 AND 3689) then 35
					            when (sic = 7375) or (sic BETWEEN 7370 AND 7373) then 36
					            when sic in (3622, 3810, 3812) or (sic BETWEEN 3661 AND 3666) or (sic BETWEEN 3669 AND 3679) then 37
					            when sic = 3811 or (sic BETWEEN 3820 AND 3827) or (sic BETWEEN 3829 AND 3839) then 38
					            when sic in (2760, 2761) or (sic BETWEEN 2520 AND 2549) or (sic BETWEEN 2600 AND 2639) or (sic BETWEEN 2670 AND 2699) or (sic BETWEEN 3950 AND 3955) then 39
					            when sic in (3220, 3221) or (sic BETWEEN 2440 AND 2449) or (sic BETWEEN 2640 AND 2659) or (sic BETWEEN 3410 AND 3412) then 40
					            when sic in (4100. 4130, 4131, 4150, 4151, 4230, 4231, 4780, 4789) or (sic BETWEEN 4000 AND 4013) or (sic BETWEEN 4040 AND 4049) or (sic BETWEEN 4110 AND 4121) or (sic BETWEEN 4140 AND 4142) or (sic BETWEEN 4170 AND 4173) or (sic BETWEEN 4190 AND 4200) or (sic BETWEEN 4210 AND 4219) or (sic BETWEEN 4240 AND 4249) or (sic BETWEEN 4400 AND 4700) or (sic BETWEEN 4710 AND 4712) or (sic BETWEEN 4720 AND 4749) or (sic BETWEEN 4782 AND 4785) then 41
					            when sic in (5000, 5099, 5100) or (sic BETWEEN 5010 AND 5015) or (sic BETWEEN 5020 AND 5023) or (sic BETWEEN 5030 AND 5060) or (sic BETWEEN 5063 AND 5065) or (sic BETWEEN 5070 AND 5078) or (sic BETWEEN 5080 AND 5088) or (sic BETWEEN 5090 AND 5094) or (sic BETWEEN 5110 AND 5113) or (sic BETWEEN 5120 AND 5122) or (sic BETWEEN 5130 AND 5172) or (sic BETWEEN 5180 AND 5182) or (sic BETWEEN 5190 AND 5199) then 42
					            when sic in (5200, 5250, 5251, 5260, 5261, 5270, 5271, 5300, 5310, 5311, 5320, 5330, 5331, 5334, 5900, 5999) or (sic BETWEEN 5210 AND 5231) or (sic BETWEEN 5340 AND 5349) or (sic BETWEEN 5390 AND 5400) or (sic BETWEEN 5410 AND 5412) or (sic BETWEEN 5420 AND 5469) or (sic BETWEEN 5490 AND 5500) or (sic BETWEEN 5510 AND 5579) or (sic BETWEEN 5590 AND 5700) or (sic BETWEEN 5710 AND 5722) or (sic BETWEEN 5730 AND 5736) or (sic BETWEEN 5750 AND 5799) or (sic BETWEEN 5910 AND 5912) or (sic BETWEEN 5920 AND 5932) or (sic BETWEEN 5940 AND 5990) or (sic BETWEEN 5992 AND 5995) then 43 
					            when sic in (7000, 7213) or (sic BETWEEN 5800 AND 5829) or (sic BETWEEN 5890 AND 5899) or (sic BETWEEN 7010 AND 7019) or (sic BETWEEN 7040 AND 7049) then 44
					            when sic = 6000 or (sic BETWEEN 6010 AND 6036) or (sic BETWEEN 6040 AND 6062) or (sic BETWEEN 6080 AND 6082) or (sic BETWEEN 6090 AND 6100) or (sic BETWEEN 6110 AND 6113) or (sic BETWEEN 6120 AND 6179) or (sic BETWEEN 6190 AND 6199) then 45
					            when sic in (6300, 6350, 6351, 6360, 6361) or (sic BETWEEN 6310 AND 6331) or (sic BETWEEN 6370 AND 6379) or (sic BETWEEN 6390 AND 6411) then 46
                                when sic in (6500, 6510, 6540, 6541, 6610, 6611) or (sic BETWEEN 6512 AND 6515) or (sic BETWEEN 6517 AND 6532) or (sic BETWEEN 6550 AND 6553) or (sic BETWEEN 6590 AND 6599) then 47
					            when (sic in (6700, 6798, 6799)) or (sic BETWEEN 6200 AND 6299) or (sic BETWEEN 6710 AND 6726) or (sic BETWEEN 6730 AND 6733) or (sic BETWEEN 6740 AND 6779) or (sic BETWEEN 6790 AND 6795) then 48
					            when (sic in (4970, 4971, 4990, 4991)) or (sic BETWEEN 4950 AND 4961) then 49
					            else NULL
					        END AS ff49
					    FROM {data}""",
        }
    }