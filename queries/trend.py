import pandas as pd

def get_trend_arrow(today, avg_last_7_days):
    if avg_last_7_days == 0 or pd.isna(avg_last_7_days):
        return "→ (0%)"
    change_percent = ((today - avg_last_7_days) / avg_last_7_days) * 100
    arrow = "🡅" if change_percent > 0 else "🡇" if change_percent < 0 else "→"
    return f"{arrow} ({change_percent:.1f}%)"

def get_monthly_trend_arrow(current_month_sales, avg_last_two_months_sales):
    if avg_last_two_months_sales == 0 or pd.isna(avg_last_two_months_sales):
        return "→ (0%)"
    change_percent = ((current_month_sales - avg_last_two_months_sales) / avg_last_two_months_sales) * 100
    arrow = "🡅" if change_percent > 0 else "🡇" if change_percent < 0 else "→"
    return f"{arrow} ({change_percent:.1f}%)"