#!/bin/bash
set -euo pipefail

MYSQL_CONFIG="$HOME/.config/mysql/local.cnf"
BINARY="cargo run --release --"

DUMP_FILE="${1:?Usage: $0 <dump.sql> [tables to exclude...]}"
shift
EXCLUDE_TABLES="${*:-}"

# Build exclude flags
EXCLUDE_FLAGS=""
for table in $EXCLUDE_TABLES; do
    EXCLUDE_FLAGS="$EXCLUDE_FLAGS -x $table"
done

# Base command (without output flag)
BASE_CMD="$BINARY \"$DUMP_FILE\" --merge-inserts 67108864 -i aborted_transactions,admins,bank_accounts,billing_details,cashout_requests,companies,company_changes,deferred_invoice_payments,dim_bank_accounts,dim_billing_processes,dim_financial_entities,dim_invoicing_settings,dim_payers,dim_payoneer_confirmations,dim_payoneer_payouts,dim_projects,dim_revolut_counter_parties,dim_revolut_events,dim_revolut_payments,dim_stripe_events,dim_stripe_subscriptions,dim_transaction_intents,dim_transferwise_recipients,dim_transferwise_requirements,dim_transferwise_statements,dim_transferwise_transfer_changes,dim_transferwise_transfers,dim_worldpay_events,dim_worldpay_payouts,dim_worldpay_quotes,email_addresses,email_aliases,fact_audit_logs,fact_audit_runs,fact_exchange_rates,fact_integrity_checks,fact_service_metrics,fact_service_orders,generic_stores,invoices_transaction_intents_mappings,invoices_with_fx_costs,invoicing_entities,organization_billings,organization_user_roles,organizations,payer_mappings,payment_settings,pending_transactions,pricing_scales,profiles,projects,projects_members,rate_adjustments,rates,revolut_default_bank_accounts,sequelize_meta,settings,shares,succeeded_transactions,system_bank_account_overrides,system_bank_accounts,transaction_partials,transactions,transactions_stripe,user_card_roles,user_cards,user_groups,user_roles,user_session_ids,users,vendor_users,vendors,wp_currencies,wp_fx_rates,wp_requirements,wp_transfer_fees,invoices,invoice_items,service_orders,service_order_items $EXCLUDE_FLAGS"

# Extract database name from config
DATABASE=$(grep -E '^database\s*=' "$MYSQL_CONFIG" | sed 's/^database\s*=\s*//' | tr -d '[:space:]')
if [[ -z "$DATABASE" ]]; then
    echo "Error: No database specified in $MYSQL_CONFIG" >&2
    exit 1
fi

# Verify MySQL is accessible
if ! mysql --defaults-extra-file="$MYSQL_CONFIG" --database="" -e "SELECT 1" >/dev/null; then
    exit 1
fi

# Drop and create database
mysql --defaults-extra-file="$MYSQL_CONFIG" --database="" -e "DROP DATABASE IF EXISTS \`$DATABASE\`; CREATE DATABASE \`$DATABASE\`"

# Calculate total size of output (sum of Size column, skipping header)
OUTPUT_SIZE=$(eval "$BASE_CMD" | awk 'NR>2 {sum += $3} END {print sum}')
echo "Output size: $OUTPUT_SIZE bytes ($(numfmt --to=iec-i --suffix=B "$OUTPUT_SIZE"))"

# Load into MySQL
# insert_ranges "$DUMP_FILE" $EXCLUDE_FLAGS -o - | mysql --defaults-extra-file="$MYSQL_CONFIG"
# Rewrite DEFINER clauses to root@localhost (only on lines starting with /*!50013 DEFINER=)
time eval "$BASE_CMD -o -" \
    | pv -s "$OUTPUT_SIZE" \
    | sed '/^\/\*!50013 DEFINER=/s/DEFINER=`[^`]*`@`[^`]*`/DEFINER=`root`@`localhost`/' \
    | mysql --defaults-extra-file="$MYSQL_CONFIG"
