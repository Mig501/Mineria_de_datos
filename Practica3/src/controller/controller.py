from pathlib import Path
from great_expectations.data_context import DataContext
from model.data_loader import download_data
from model.data_cleaner import clean_data
from model.ge_validator import validate_with_great_expectations
from view.view import show_validation_result, show_text, show_validation_details
from model.ge_validator import validate_with_great_expectations, validate_business_rules
from model.ge_alerts import get_failed_expectations

class ValidationController:

    def __init__(self):
        project_root = Path(__file__).resolve().parents[2]
        ge_root = project_root / "gx"
        self.context = DataContext(context_root_dir=str(ge_root))

    def run(self):
        tickers = [
            "BBVA.MC", "SAB.MC", "IBE.MC",
            "NTGY.MC", "TEF.MC", "CLNX.MC"
        ]

        df = download_data(
            tickers=tickers,
            start="2020-01-01",
            end="2025-01-01"
        )

        df_clean = clean_data(df)

        # Ejercicio 1 – formato
        result_format = validate_with_great_expectations(
            context=self.context,
            df=df_clean,
            suite_name="stocks_format_suite"
        )

        # Ejercicio 2 – negocio
        result_business = validate_business_rules(
            context=self.context,
            df=df_clean,
            suite_name="stocks_business_suite"
        )

        self.context.build_data_docs()
        show_text("Ej1:")
        show_validation_result(result_format)

        show_text("Ej2:")
        show_validation_result(result_business)

        show_text("Ej3:")
        status_format = get_failed_expectations(
            result_format, "stocks_format_suite"
        )
        show_validation_details(status_format)
        status_business = get_failed_expectations(
            result_business, "stocks_business_suite"
        )
        show_validation_details(status_business)
