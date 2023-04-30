acc_chars=[
    #accounting based size measures
    "assets",
    "sales",
    "book_equity",
    "net_income",
    "enterprise_value"
    
    #1 yr Growth 
    "at_gr1",
    "ca_gr1",
    "nca_gr1",
    "lt_gr1",
    "cl_gr1",
    "ncl_gr1",
    "be_gr1",
    "pstk_gr1",
    "debt_gr1",
    "sale_gr1",
    "cogs_gr1",
    "sga_gr1",
    "opex_gr1",

    #3 yr Growth
    "at_gr3","ca_gr3","nca_gr3","lt_gr3","cl_gr3","ncl_gr3","be_gr3","pstk_gr3",
    "debt_gr3","sale_gr3","cogs_gr3","sga_gr3","opex_gr3",

    #1 yr Growth Scaled by Assets
    "cash_gr1a",
    "inv_gr1a",
    "rec_gr1a",
    "ppeg_gr1a",
    "lti_gr1a",
    "intan_gr1a",
    "debtst_gr1a",
    "ap_gr1a",
    "txp_gr1a",
    "debtlt_gr1a",
    "txditc_gr1a",
    "coa_gr1a",
    "col_gr1a",
    "cowc_gr1a",
    "ncoa_gr1a",
    "ncol_gr1a",
    "nncoa_gr1a",
    "oa_gr1a",
    "ol_gr1a",
    "noa_gr1a",
    "fna_gr1a",
    "fnl_gr1a",
    "nfna_gr1a",
    "gp_gr1a",
    "ebitda_gr1a",
    "ebit_gr1a",
    "ope_gr1a",
    "ni_gr1a",
    "nix_gr1a",
    "dp_gr1a",
    "ocf_gr1a",
    "fcf_gr1a",
    "nwc_gr1a",
    "eqnetis_gr1a",
    "dltnetis_gr1a",
    "dstnetis_gr1a",
    "dbnetis_gr1a",
    "netis_gr1a",
    "fincf_gr1a",
    "eqnpo_gr1a",
    "tax_gr1a",
    "div_gr1a",
    "eqbb_gr1a",
    "eqis_gr1a",
    "eqpo_gr1a",
    "capx_gr1a",

    #3 yr Growth Scaled by Assets
    "cash_gr3a","inv_gr3a","rec_gr3a","ppeg_gr3a","lti_gr3a","intan_gr3a","debtst_gr3a",
    "ap_gr3a","txp_gr3a","debtlt_gr3a","txditc_gr3a","coa_gr3a","col_gr3a","cowc_gr3a",
    "ncoa_gr3a","ncol_gr3a","nncoa_gr3a","oa_gr3a","ol_gr3a","fna_gr3a","fnl_gr3a",
    "nfna_gr3a","gp_gr3a","ebitda_gr3a","ebit_gr3a","ope_gr3a","ni_gr3a","nix_gr3a",
    "dp_gr3a","ocf_gr3a","fcf_gr3a","nwc_gr3a","eqnetis_gr3a","dltnetis_gr3a","dstnetis_gr3a",
    "dbnetis_gr3a","netis_gr3a","fincf_gr3a","eqnpo_gr3a","tax_gr3a","div_gr3a","eqbb_gr3a",
    "eqis_gr3a","eqpo_gr3a","capx_gr3a",

    # Investment
    "capx_at","rd_at",

    # Profitability
    "gp_sale",
    "ebitda_sale",
    "ebit_sale",
    "pi_sale",
    "ni_sale",
    "nix_sale",
    "ocf_sale",
    "fcf_sale", #Profit Margins
    "gp_at",
    "ebitda_at",
    "ebit_at",
    "fi_at",
    "cop_at",
    "ope_be",
    "ni_be",
    "nix_be",
    "ocf_be",
    "fcf_be", #Return on Book Equity
    "gp_bev",
    "ebitda_bev",
    "ebit_bev",
    "fi_bev",
    "cop_bev", # Return on Invested Capital
    "gp_ppen",
    "ebitda_ppen",
    "fcf_ppen", # Return on Physical Capital

    # Issuance
    "fincf_at",
    "netis_at",
    "eqnetis_at",
    "eqis_at",
    "dbnetis_at",
    "dltnetis_at",
    "dstnetis_at",

    # Equity Payout
    "eqnpo_at",
    "eqbb_at",
    "div_at",

    # Accruals
    "oaccruals_at",
    "oaccruals_ni",
    "taccruals_at",
    "taccruals_ni",
    "noa_at",

    # Capitalization / Leverage Ratios
    "be_bev",
    "debt_bev",
    "cash_bev",
    "pstk_bev",
    "debtlt_bev",
    "debtst_bev",
    "debt_mev",
    "pstk_mev",
    "debtlt_mev",
    "debtst_mev",

    # Financial Soundness Ratios
    "int_debtlt",
    "int_debt",
    "cash_lt",
    "inv_act",
    "rec_act",
    "ebitda_debt",
    "debtst_debt",
    "cl_lt",
    "debtlt_debt",
    "profit_cl",
    "ocf_cl",
    "ocf_debt",
    "lt_ppen",
    "debtlt_be",
    "fcf_ocf",
    "opex_at",
    "nwc_at",

    # Solvency Ratios
    "debt_at",
    "debt_be",
    "ebit_int",

    # Liquidity Ratios
    "cash_cl",
    "caliq_cl",
    "ca_cl",
    "inv_days",
    "rec_days",
    "ap_days",
    "cash_conversion",

    # Activity / Efficiency Ratio
    "inv_turnover",
    "at_turnover",
    "rec_turnover",
    "ap_turnover",

    # Non Recurring Items
    "spi_at",
    "xido_at",
    "nri_at",

    # Miscalleneous
    "adv_sale",
    "staff_sale",
    "rd_sale",
    "div_ni",
    "sale_bev",
    "sale_be",
    "sale_nwc",
    "tax_pi",

    # Balance Sheet Fundamentals to Market Equity
    "be_me",
    "at_me",
    "cash_me",

    # Income Fundamentals to Market Equity
    "gp_me",
    "ebitda_me",
    "ebit_me",
    "ope_me",
    "ni_me",
    "nix_me",
    "sale_me",
    "ocf_me",
    "fcf_me",
    "cop_me",
    "rd_me",

    # Equity Payout / issuance to Market Equity
    "div_me",
    "eqbb_me",
    "eqis_me",
    "eqpo_me",
    "eqnpo_me",
    "eqnetis_me",

    # Debt Issuance to Market Enterprice Value
    "dltnetis_mev",
    "dstnetis_mev",
    "dbnetis_mev",

    # Firm Payout / issuance to Market Enterpice Value
    "netis_mev",

    # Balance Sheet Fundamentals to Market Enterprise Value
    "at_mev",
    "be_mev",
    "bev_mev",
    "ppen_mev",
    "cash_mev",

    # Income / CF Fundamentals to Market Enterprise Value
    "gp_mev",
    "ebitda_mev",
    "ebit_mev",
    "cop_mev",
    "sale_mev",
    "ocf_mev",
    "fcf_mev",
    "fincf_mev",

    # New Variables from HXZ
    "ni_inc8q",
    "ppeinv_gr1a",
    "lnoa_gr1a",
    "capx_gr1",
    "capx_gr2",
    "capx_gr3",
    "sti_gr1a",
    "niq_at",
    "niq_at_chg1",
    "niq_be",
    "niq_be_chg1",
    "saleq_gr1",
    "rd5_at",
    "dsale_dinv",
    "dsale_drec",
    "dgp_dsale",
    "dsale_dsga",
    "saleq_su",
    "niq_su",
    "debt_me",
    "netdebt_me",
    "capex_abn",
    "inv_gr1",
    "be_gr1a",
    "op_at",
    "pi_nix",
    "op_atl1",
    "gp_atl1",
    "ope_bel1",
    "cop_atl1",
    "at_be",
    "ocfq_saleq_std",
    "aliq_at",
    "aliq_mat",
    "tangibility",
    "eq_dur",
    "f_score",
    "o_score",
    "z_score",
    "kz_index",
    "intrinsic_value",
    "ival_me",
    "sale_emp_gr1",
    "emp_gr1",
    "cash_at",
    "earnings_variability",
    "ni_ar1",
    "ni_ivol",

    # New Variables not in HXZ
    "niq_saleq_std",
    "ni_emp",
    "sale_emp",
    "ni_at",
    "ocf_at",
    "ocf_at_chg1",
    "roeq_be_std",
    "roe_be_std",
    "gpoa_ch5",
    "roe_ch5",
    "roa_ch5",
    "cfoa_ch5",
    "gmar_ch5"
]

macro_chars=[

	# Market Based Size Variables
	"market_equity",
	# Total Dividend Paid to Market Equity
	"div1m_me","div3m_me","div6m_me","div12m_me",
	# Special Dividend Paid to Market Equity
	"divspc1m_me","divspc12m_me",
	# Change in Shares Outstanding
	"chcsho_1m","chcsho_3m","chcsho_6m","chcsho_12m",
	# Net Equity Payout
	"eqnpo_1m","eqnpo_3m","eqnpo_6m","eqnpo_12m",
	# Momentum/Reversal
	"ret_1_0","ret_2_0","ret_3_0","ret_3_1","ret_6_0","ret_6_1 ret_9_0","ret_9_1","ret_12_0","ret_12_1","ret_12_7","ret_18_1","ret_24_1","ret_24_12",
	"ret_36_1","ret_36_12","ret_48_1","ret_48_12","ret_60_1","ret_60_12","ret_60_36",
	# Seasonality
	"seas_1_1an","seas_2_5an","seas_6_10an","seas_11_15an","seas_16_20an",
	"seas_1_1na","seas_2_5na","seas_6_10na","seas_11_15na","seas_16_20na",
]
common_vars=["gvkey","iid","datadate","eom","tpci","exchg","curcdd","prc_local","prc_high",
             "prc_low","ajexdi","cshoc","ri_local","fx","prc","me","cshtrm","dolvol","ri",
             "div_tot","div_cash","div_spc"]

special_exchanges=(0,1,2,3,4,15, 16,17,18,21,13,19,20,127,150,157,229,263,269,281,283,290,320,326,341,342,347,348,349,352)