## MRR Reconciliation Notes

- Gold MRR is derived from Silver active subscriptions.
- For active subscriptions without an end date, MRR is treated as a point-in-time snapshot.
- Therefore, the total SUM(mrr) in Gold matches the SUM(monthly_amount) of active subscriptions in Silver.
- This reconciliation confirms no revenue inflation or loss across layers.

## Active Customers Reconciliation

- Silver active customer count represents the total number of customers who were ever active at any point in time.
- Gold active_customers_monthly represents point-in-time monthly active customers.
- Therefore, the maximum monthly active customers in Gold is significantly lower than the cumulative Silver count.
- This difference is expected and confirms correct temporal aggregation.

## Engagement Reconciliation (DAU / MAU)

- Silver usage_events contains all users who have ever generated events.
- Gold MAU represents distinct users active in a single month.
- Therefore, the maximum MAU is significantly lower than the cumulative Silver user count.
- This difference is expected and confirms correct monthly aggregation.

