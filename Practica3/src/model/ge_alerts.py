def get_failed_expectations(result, suite_name):
    failed = []

    for res in result["results"]:
        if not res["success"]:
            failed.append({
                "expectation_type": res["expectation_config"]["expectation_type"],
                "kwargs": res["expectation_config"]["kwargs"]
            })

    return {
        "suite_name": suite_name,
        "success": result["success"],
        "failed_expectations": failed
    }
