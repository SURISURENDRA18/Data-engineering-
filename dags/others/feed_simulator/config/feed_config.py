FEED_METADATA = {

    # =====================================================
    # USAGE CDR
    # =====================================================

    "usage_cdr_hol_01": {

        "feed_family": "usage_cdr",

        "feed_prefix": "HOL_01",

        "feed_pattern": "CDRTrafficDetail",

        "sample_file":
            "HOL_01_CDRTrafficDetail_000045249_20260519031502.csv",

        "files_per_day": 96,

        "interval_minutes": 15,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    "usage_cdr_hol_02": {

        "feed_family": "usage_cdr",

        "feed_prefix": "HOL_02",

        "feed_pattern": "CDRTrafficDetail",

        "sample_file":
            "HOL_02_CDRTrafficDetail_000045249_20260519031502.csv",

        "files_per_day": 96,

        "interval_minutes": 15,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    "usage_cdr_sng_01": {

        "feed_family": "usage_cdr",

        "feed_prefix": "SNG_01",

        "feed_pattern": "CDRTrafficDetail",

        "sample_file":
            "SNG_01_CDRTrafficDetail_000148056_20260519080002.csv",

        "files_per_day": 48,

        "interval_minutes": 30,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    # =====================================================
    # KYC
    # =====================================================

    "kyc_hourly": {

        "feed_family": "kyc",

        "feed_prefix":
            "KYCMEDIATIONFEED_HOURLY",

        "sample_file":
            "KYCMEDIATIONFEED_HOURLY_20260517000060.csv",

        "files_per_day": 24,

        "interval_minutes": 60,

        "records_per_file": 5000,

        "delimiter": ","
    },

    "kyc_weekly": {

        "feed_family": "kyc",

        "feed_prefix":
            "KYCMEDIATIONFEED_WEEKLY",

        "sample_file":
            "KYCMEDIATIONFEED_WEEKLY_20260426000030.csv",

        "files_per_day": 1,

        "interval_minutes": 10080,

        "records_per_file": 5000,

        "delimiter": ","
    },

    # =====================================================
    # SIM INVENTORY
    # =====================================================

    "sim_inventory_hol_01": {

        "feed_family": "sim_inventory",

        "feed_prefix": "HOL_01",

        "feed_pattern": "SimInventory",

        "sample_file":
            "HOL_01_SimInventory_000147743_20260520000004.csv",

        "files_per_day": 48,

        "interval_minutes": 30,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    "sim_inventory_sng_01": {

        "feed_family": "sim_inventory",

        "feed_prefix": "SNG_01",

        "feed_pattern": "SimInventory",

        "sample_file":
            "SNG_01_SimInventory_000149083_20260520000002.csv",

        "files_per_day": 48,

        "interval_minutes": 30,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    # =====================================================
    # ACTIVE
    # =====================================================

    "active_hol_01": {

        "feed_family": "active",

        "feed_prefix": "HOL_01",

        "feed_pattern": "Active",

        "sample_file":
            "HOL_01_Active_000050315_20260519230017.csv",

        "files_per_day": 3,

        "interval_minutes": 480,

        "records_per_file": 5000,

       "delimiter": ";"
    },

    "active_sng_01": {

        "feed_family": "active",

        "feed_prefix": "SNG_01",

        "feed_pattern": "Active",

        "sample_file":
            "SNG_01_Active_000148149_20260520130001.csv",

        "files_per_day": 48,

        "interval_minutes": 30,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    # =====================================================
    # ACTIVATIONS
    # =====================================================

    "activations_hol_01": {

        "feed_family": "activations",

        "feed_prefix": "HOL_01",

        "feed_pattern": "Activations",

        "sample_file":
            "HOL_01_Activations_000095549_20260509200002.csv",

        "files_per_day": 6,

        "interval_minutes": 240,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    "activations_sng_01": {

        "feed_family": "activations",

        "feed_prefix": "SNG_01",

        "feed_pattern": "Activations",

        "sample_file":
            "SNG_01_Activations_000146617_20260519053001.csv",

        "files_per_day": 48,

        "interval_minutes": 30,

        "records_per_file": 1,

        "delimiter": ";"
    },

    # =====================================================
    # TOLLING
    # =====================================================

    "tolling_hol_01": {

        "feed_family": "tolling",

        "feed_prefix": "HOL_01",

        "feed_pattern": "Tolling",

        "sample_file":
            "HOL_01_Tolling_000070167_20260311133002.csv",

        "files_per_day": 48,

        "interval_minutes": 30,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    "tolling_sng_01": {

        "feed_family": "tolling",

        "feed_prefix": "SNG_01",

        "feed_pattern": "Tolling",

        "sample_file":
            "SNG_01_Tolling_000138082_20260214110001.csv",

        "files_per_day": 48,

        "interval_minutes": 30,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    # =====================================================
    # IMEI
    # =====================================================

    "imei_hol_01": {

        "feed_family": "imei",

        "feed_prefix": "HOL_01",

        "feed_pattern": "IMEIChange",

        "sample_file":
            "HOL_01_IMEIChange_000002630_20260514230008.csv",

        "files_per_day": 1,

        "interval_minutes": 1440,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    # =====================================================
    # ESIMHUB MED
    # =====================================================

    "esimhub_med": {

        "feed_family": "esimhub_med",

        "feed_prefix": "eSIMHUB_MED",

        "feed_pattern": "",

        "sample_file":
            "eSIMHUB_MED_202604280000.csv",

        "files_per_day": 1,

        "interval_minutes": 1440,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    # =====================================================
    # NL PREACTIVE
    # =====================================================

    "nl_preactive_30420": {

        "feed_family": "nl_preactive",

        "feed_prefix": "NL_PreActive",

        "feed_pattern": "",

        "feed_sequence": "000030420",

        "sample_file":
            "NL_PreActive_000030421_20260521080000.csv",

        "files_per_day": 48,

        "interval_minutes": 30,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    "nl_preactive_30421": {

        "feed_family": "nl_preactive",

        "feed_prefix": "NL_PreActive",

        "feed_pattern": "",

        "feed_sequence": "000030421",

        "sample_file":
            "NL_PreActive_000030421_20260521080000.csv",

        "files_per_day": 48,

        "interval_minutes": 30,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    # =====================================================
    # PRODUCT SUBSCRIPTION
    # =====================================================

    "product_subscription_hol_01_1090": {

        "feed_family": "product_subscription",

        "feed_prefix": "HOL_01",

        "feed_pattern": "Product_Subscription",

        "feed_sequence": "000001090",

        "sample_file":
            "HOL_01_Product_Subscription_000001090_20260514080007.csv",

        "files_per_day": 1,

        "interval_minutes": 1440,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    # =====================================================
    # DR NL BOSS MED PREACTIVE
    # =====================================================

    "dr_nl_boss_med_preactive": {

        "feed_family": "dr_nl_boss_med_preactive",

        "feed_prefix": "DR_NL_BOSS_MED_PreActive",

        "feed_pattern": "",

        "feed_sequence": "000000715",

        "sample_file":
            "DR_NL_BOSS_MED_PreActive_000000715_20260520235959.csv",

        "files_per_day": 1,

        "interval_minutes": 1440,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    # =====================================================
    # DR NL BOSSMED PORTINOUT
    # =====================================================

    "dr_nl_bossmed_portinout": {

        "feed_family": "dr_nl_bossmed_portinout",

        "feed_prefix": "DR_NL_BOSSMED_PORTINOUT",

        "feed_pattern": "",

        "feed_sequence": "000000353",

        "sample_file":
            "DR_NL_BOSSMED_PORTINOUT_000000352_20260519235959.csv",

        "files_per_day": 1,

        "interval_minutes": 1440,

        "records_per_file": 5000,

        "delimiter": ";"
    },

    # =====================================================
    # MR NL BOSSMED PORTINOUT
    # =====================================================

    "mr_nl_bossmed_portinout": {

        "feed_family": "mr_nl_bossmed_portinout",

        "feed_prefix": "MR_NL_BOSSMED_PORTINOUT",

        "feed_pattern": "",

        "feed_sequence": "000000021",

        "sample_file":
            "MR_NL_BOSSMED_PORTINOUT_000000021_20260430235959.csv",

        "files_per_day": 1,

        "interval_minutes": 43200,

        "records_per_file": 5000,

        "delimiter": ";"
    },

 
    # =====================================================
    # MM REPORT - SPONSOR
    # =====================================================


    "mm_report_hol_01": {

    "feed_family": "mm_report",

    "feed_prefix": "HOL_01",

    "feed_pattern": "Sponsor_Master",

    "feed_sequence": "000000161",

    "sample_file":
        "HOL_01_Sponsor_Master_000000161_20260216080008.csv",

    "files_per_day": 1,

    "interval_minutes": 10080,

    "records_per_file": 5000,

    "delimiter": ";"
 },

    # =====================================================
    # MOBILIUM
    # =====================================================


 "mobilium_ny_med": {

    "feed_family": "mobilium",

    "feed_prefix": "NY_Mobileum_MED",

    "feed_pattern": "",

    "sample_file":
        "NY_Mobileum_MED_202603120500_001.csv",

    "files_per_day": 1,

    "interval_minutes": 1440,

    "records_per_file": 5000,

    "delimiter": ",",

    "checksum_extension": ".cksum"
},

"mnp_portinout_bossmed": {

    "feed_family": "mnp",

    "feed_prefix": "NL_PORTINOUT_BOSSMED",

    "feed_pattern": "",

    "sample_file":
        "NL_PORTINOUT_BOSSMED_000000888_20260219070000.csv",

    "files_per_day": 24,

    "interval_minutes": 60,

    "records_per_file": 5000,

    "delimiter": ";"
},
"ic_usage": {

    "feed_family": "ic_usage",

    "feed_prefix": "IC_USAGE",

    "feed_pattern": "",

    "sample_file":
        "IC_USAGE_000053544_20260525123005.CSV",

    "files_per_day": 96,

    "interval_minutes": 15,

    "records_per_file": 5000,

    "delimiter": ";"
}

}