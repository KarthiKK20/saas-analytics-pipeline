import sys
from src.build_silver import (
    build_silver_customers,
    build_silver_users,
    build_silver_subscriptions,
    build_silver_payments,
    build_silver_usage_events,
)
from src.build_gold import (
    build_gold_mrr_monthly,
    build_gold_customer_churn,
    build_gold_dau_mau,
    build_gold_active_customers,
    build_gold_dashboard_monthly,
)

def run_silver():
    build_silver_customers()
    build_silver_users()
    build_silver_subscriptions()
    build_silver_payments()
    build_silver_usage_events()

def run_gold():
    build_gold_mrr_monthly()
    build_gold_customer_churn()
    build_gold_dau_mau()
    build_gold_active_customers()
    build_gold_dashboard_monthly()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python etl.py [silver|gold|all]")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "silver":
        run_silver()
    elif mode == "gold":
        run_gold()
    elif mode == "all":
        run_silver()
        run_gold()
    else:
        print("Invalid option: silver | gold | all")
