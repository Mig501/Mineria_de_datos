from great_expectations.core.batch import RuntimeBatchRequest

def ensure_datasource(context):
    datasource_name = "runtime_spark_ds"

    if datasource_name not in context.list_datasources():
        context.add_datasource(
            name=datasource_name,
            class_name="Datasource",
            execution_engine={
                "class_name": "PandasExecutionEngine"
            },
            data_connectors={
                "runtime_connector": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["run_id"],
                }
            },
        )

def validate_with_great_expectations(context, df, suite_name):

    ensure_datasource(context)

    context.add_or_update_expectation_suite(
    expectation_suite_name=suite_name
    )


    batch_request = RuntimeBatchRequest(
        datasource_name="runtime_spark_ds",
        data_connector_name="runtime_connector",
        data_asset_name="stocks_data",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_id": "run_01"},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    validator.expect_table_row_count_to_be_between(min_value=1)

    validator.expect_column_values_to_not_be_null("Date")
    price_column = "BBVA.MC_Close"

    validator.expect_column_values_to_not_be_null(price_column)

    validator.expect_column_values_to_be_between(
        column=price_column,
        min_value=0
    )

    validator.expect_column_values_to_be_unique("Date")

    validator.save_expectation_suite()
    return validator.validate()

def validate_business_rules(context, df, suite_name):

    context.add_or_update_expectation_suite(
        expectation_suite_name=suite_name
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="runtime_spark_ds",
        data_connector_name="runtime_connector",
        data_asset_name="stocks_business_rules",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_id": "run_business"},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    open_col = "BBVA.MC_Open"
    high_col = "BBVA.MC_High"
    low_col = "BBVA.MC_Low"
    close_col = "BBVA.MC_Close"
    volume_col = "BBVA.MC_Volume"

    # Ningún precio negativo
    for col in [open_col, high_col, low_col, close_col]:
        validator.expect_column_values_to_be_between(
            column=col,
            min_value=0
        )

    # High ≥ Low
    validator.expect_column_pair_values_A_to_be_greater_than_B(
        column_A=high_col,
        column_B=low_col,
        or_equal=True
    )

    # Open ∈ [Low, High]
    validator.expect_column_pair_values_A_to_be_greater_than_B(
        column_A=open_col,
        column_B=low_col,
        or_equal=True
    )
    validator.expect_column_pair_values_A_to_be_greater_than_B(
        column_A=high_col,
        column_B=open_col,
        or_equal=True
    )

    # Close ∈ [Low, High]
    validator.expect_column_pair_values_A_to_be_greater_than_B(
        column_A=close_col,
        column_B=low_col,
        or_equal=True
    )
    validator.expect_column_pair_values_A_to_be_greater_than_B(
        column_A=high_col,
        column_B=close_col,
        or_equal=True
    )

    # Volumen ≥ 0
    validator.expect_column_values_to_be_between(
        column=volume_col,
        min_value=0
    )

    validator.save_expectation_suite()
    return validator.validate()
