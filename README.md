# dbt_dbx_vault_demo

* Note 1: This repo follows the Medium article below to implement an example of a data vault architecture using dbt and databricks:
[data vault with dbt and databricks](https://medium.com/@valentin.loghin/unlocking-faster-insights-with-dbtcore-and-data-vault-on-databricks-f0eeee7d5543)

  * I did the following changes:
    - Renamed `raw` to `bronze`
    - Renamed `raw_vault` to `silver_vault`
    - Renamed `bv_customer_metrics` to `biz_vault_customer_metrics`

* Note 2: I used the dbt package [automate_dv](https://automate-dv.readthedocs.io/en/latest/) for this exercise.
