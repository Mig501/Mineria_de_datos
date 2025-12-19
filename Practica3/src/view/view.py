def show_text(text):
    print(text)

def show_validation_result(result):
    if result["success"]:
        print("Validación correcta. Todas las expectations se han cumplido.")
    else:
        print("Validación fallida. Alguna expectation no se cumple.")

def show_validation_details(status):
    suite = status["suite_name"]

    if status["success"]:
        print(f"La validación '{suite}' se ha completado correctamente.")
        return

    print(f"La validación '{suite}' ha FALLADO.")
    print("   Expectations que no se han cumplido:")

    for exp in status["failed_expectations"]:
        exp_type = exp["expectation_type"]
        kwargs = exp["kwargs"]

        print(f"   - {exp_type} | parámetros: {kwargs}")
