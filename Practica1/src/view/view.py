# ---------------- view.py ----------------
class CLIView:
    @staticmethod
    def show_head(df, n=10):
        print("\n=== Vista CLI ===")
        df.show(n, truncate=False)
