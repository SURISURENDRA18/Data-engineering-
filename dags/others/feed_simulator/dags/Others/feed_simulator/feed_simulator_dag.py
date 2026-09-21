from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

import os
import sys

# =========================================================
# PROJECT PATHS
# =========================================================

BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

PROJECT_ROOT = BASE_DIR

CONFIG_DIR = os.path.join(
    PROJECT_ROOT,
    "config"
)

GENERATOR_DIR = os.path.join(
    PROJECT_ROOT,
    "generators"
)

# =========================================================
# PYTHON PATHS
# =========================================================

sys.path.append(PROJECT_ROOT)

sys.path.append(CONFIG_DIR)

sys.path.append(GENERATOR_DIR)

# =========================================================
# IMPORTS
# =========================================================

from config.feed_config import FEED_METADATA

from usage_cdr_generator import (
    generate_usage_cdr_feed
)

from kyc_generator import (
    generate_kyc_feed
)

from sim_inventory_generator import (
    generate_sim_inventory_feed
)

from active_generator import (
    generate_active_feed
)

from activations_generator import (
    generate_activations_feed
)

from tolling_generator import (
    generate_tolling_feed
)

from imei_generator import (
    generate_imei_feed
)

from esimhub_med_generator import (
    generate_esimhub_med_feed
)


from nl_preactive_generator import (
    generate_nl_preactive_feed
)



from product_subscription_generator import (
    generate_product_subscription_feed
)

from dr_nl_boss_med_preactive_generator import (
    generate_dr_nl_boss_med_preactive_feed
)

from dr_nl_bossmed_portinout_generator import (
    generate_dr_nl_bossmed_portinout_feed
)

from mr_nl_bossmed_portinout_generator import (
    generate_mr_nl_bossmed_portinout_feed
)

from mm_report_generator import (
    generate_mm_report_feed
)

from mobilium_generator import (
    generate_mobilium_feed
)

from mnp_generator import (
    generate_mnp_feed
)

from ic_usage_generator import (
    generate_ic_usage_feed
)


# =========================================================
# SAMPLE FILE DIRECTORY
# =========================================================

SAMPLE_DIR = os.path.join(
    PROJECT_ROOT,
    "sample_feed_files"
)

# =========================================================
# TEMP OUTPUT DIRECTORY
# =========================================================

OUTPUT_DIR = "/tmp/feed_simulator_output"

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True
)

# =========================================================
# USAGE CDR TASK
# =========================================================

def run_usage_cdr():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "usage_cdr"
    ]

    generate_usage_cdr_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# KYC TASK
# =========================================================

def run_kyc():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "kyc"
    ]

    generate_kyc_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# SIM INVENTORY TASK
# =========================================================

def run_sim_inventory():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "sim_inventory"
    ]

    generate_sim_inventory_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# ACTIVE TASK
# =========================================================

def run_active():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "active"
    ]

    generate_active_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# ACTIVATIONS TASK
# =========================================================

def run_activations():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "activations"
    ]

    generate_activations_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# TOLLING TASK
# =========================================================

def run_tolling():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "tolling"
    ]

    generate_tolling_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# IMEI TASK
# =========================================================

def run_imei():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "imei"
    ]

    generate_imei_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# eSIMHUB_MED TASK
# =========================================================

def run_esimhub_med():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "esimhub_med"
    ]

    generate_esimhub_med_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# nl_PreActive TASK
# =========================================================


def run_nl_preactive():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "nl_preactive"
    ]

    generate_nl_preactive_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

def run_product_subscription():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "product_subscription"
    ]

    generate_product_subscription_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )


def run_dr_nl_boss_med_preactive():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"]
        == "dr_nl_boss_med_preactive"
    ]

    generate_dr_nl_boss_med_preactive_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )


def run_dr_nl_bossmed_portinout():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"]
        == "dr_nl_bossmed_portinout"
    ]

    generate_dr_nl_bossmed_portinout_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )


def run_mr_nl_bossmed_portinout():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"]
        == "mr_nl_bossmed_portinout"
    ]

    generate_mr_nl_bossmed_portinout_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# MM REPORT TASK
# =========================================================

def run_mm_report():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "mm_report"
    ]

    generate_mm_report_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

    # =========================================================
# MOBILIUM TASK
# =========================================================

def run_mobilium():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "mobilium"
    ]

    generate_mobilium_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )
# =========================================================
# DAG CONFIGURATION
# =========================================================

default_args = {

    "owner": "airflow",

    "depends_on_past": False,

    "start_date": datetime(
        2025,
        1,
        1
    )
}

# =========================================================
# MNP TASK
# =========================================================

def run_mnp():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "mnp"
    ]

    generate_mnp_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# IC USAGE TASK
# =========================================================

def run_ic_usage():

    configs = [

        config

        for config in FEED_METADATA.values()

        if config["feed_family"] == "ic_usage"
    ]

    generate_ic_usage_feed(

        configs,

        SAMPLE_DIR,

        OUTPUT_DIR
    )

# =========================================================
# DAG
# =========================================================

with DAG(

    dag_id="feed_simulator_dag",

    default_args=default_args,

    schedule=None,

    catchup=False,

    description="Telecom Feed Simulator"

) as dag:

    # =====================================================
    # USAGE CDR
    # =====================================================

    usage_cdr_task = PythonOperator(

        task_id="generate_usage_cdr",

        python_callable=run_usage_cdr
    )

    # =====================================================
    # KYC
    # =====================================================

    kyc_task = PythonOperator(

        task_id="generate_kyc",

        python_callable=run_kyc
    )

    # =====================================================
    # SIM INVENTORY
    # =====================================================

    sim_inventory_task = PythonOperator(

        task_id="generate_sim_inventory",

        python_callable=run_sim_inventory
    )

    # =====================================================
    # ACTIVE
    # =====================================================

    active_task = PythonOperator(

        task_id="generate_active",

        python_callable=run_active
    )

    # =====================================================
    # ACTIVATIONS
    # =====================================================

    activations_task = PythonOperator(

        task_id="generate_activations",

        python_callable=run_activations
    )

    # =====================================================
    # TOLLING
    # =====================================================

    tolling_task = PythonOperator(

        task_id="generate_tolling",

        python_callable=run_tolling
    )

    # =====================================================
    # IMEI
    # =====================================================

    imei_task = PythonOperator(

        task_id="generate_imei",

        python_callable=run_imei
    )

    # =====================================================
    # eSIMHUB_MED
    # =====================================================


    esimhub_med_task = PythonOperator(

        task_id="generate_esimhub_med",

        python_callable=run_esimhub_med
    )

    # =====================================================
    # nl_PreActive
    # =====================================================

    nl_preactive_task = PythonOperator(

        task_id="generate_nl_preactive",

        python_callable=run_nl_preactive
    )

    product_subscription_task = PythonOperator(

        task_id="generate_product_subscription",

        python_callable=run_product_subscription
    )

    dr_nl_boss_med_preactive_task = PythonOperator(

        task_id="generate_dr_nl_boss_med_preactive",

        python_callable=run_dr_nl_boss_med_preactive
    )

    dr_nl_bossmed_portinout_task = PythonOperator(

        task_id="generate_dr_nl_bossmed_portinout",

        python_callable=run_dr_nl_bossmed_portinout
    )

    mr_nl_bossmed_portinout_task = PythonOperator(

        task_id="generate_mr_nl_bossmed_portinout",

        python_callable=run_mr_nl_bossmed_portinout
    )

 
    # =====================================================
    # MM REPORT
    # =====================================================

    mm_report_task = PythonOperator(

        task_id="generate_mm_report",

        python_callable=run_mm_report
    )

   # =====================================================
    # MOBILIUM
    # =====================================================

    mobilium_task = PythonOperator(

        task_id="generate_mobilium",

        python_callable=run_mobilium
    )

    # =====================================================
    # MNP
    # =====================================================

    mnp_task = PythonOperator(

        task_id="generate_mnp",

        python_callable=run_mnp
    )


    # =====================================================
    # IC USAGE
    # =====================================================

    ic_usage_task = PythonOperator(

        task_id="generate_ic_usage",

        python_callable=run_ic_usage
    )

    # =====================================================
    # PARALLEL EXECUTION
    # =====================================================

    [
        usage_cdr_task,
        kyc_task,
        sim_inventory_task,
        active_task,
        activations_task,
        tolling_task,
        imei_task,
        esimhub_med_task,
        nl_preactive_task,
        product_subscription_task,
        dr_nl_boss_med_preactive_task,
        dr_nl_bossmed_portinout_task,
        mr_nl_bossmed_portinout_task,
        mm_report_task,
        mobilium_task,
        mnp_task,
        ic_usage_task
    ]