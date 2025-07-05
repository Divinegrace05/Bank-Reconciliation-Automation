import os 
import re
import streamlit as st
import pandas as pd
import numpy as np
from io import BytesIO
import datetime
from pathlib import Path
from io import StringIO
import time
import openpyxl

# --- Configuration for Reports Storage ---
# Define the base directory in your home folder
BASE_DIR = os.path.expanduser("~/NGN_Reconciliation_Assistant")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
REPORTS_FILE = os.path.join(REPORTS_DIR, "reconciliation_reports.csv")

# Create directories if they don't exist
Path(REPORTS_DIR).mkdir(parents=True, exist_ok=True)
print(f"Reports will be stored in: {REPORTS_DIR}")  # Debug output

# --- Configuration Data ---

# Define the global structure for countries and banks
COUNTRIES_BANKS = {
    "Kenya": ["Pesaswap", "Zamupay PYCS", "Equity KE", "Cellulant KE", "Mpesa KE", "I&M KES", "I&M USD (KE)", "NCBA KES", "NCBA USD"],
    "Tanzania": ["NMB", "M-pesa TZ", "Selcom TZ", "CRDB TZS", "CRDB USD", "Equity TZ", "Cellulant TZ", "I&M TZS", "I&M USD (TZ)", "UBA"],
    "Uganda": ["Pegasus", "Flutterwave Ug", "Equity UGX", "Equity Ug USD"],
    "Ghana": ["Flutterwave GHS", "Fincra GHS", "Zeepay"],
    "Senegal & Côte d'Ivoire XOF": ["Aza Finance", "Hub2 IC", "Hub2 SEN"],
    "Rwanda": ["I&M RWF", "I&M USD (RWF)", "Kremit", "Flutterwave RWF"],
    "Cameroon XAF": ["Peex", "Pawapay", "Aza Finance", "Hub2"],
    "Nigeria": ["Moniepoint", "Verto", "Cellulant NGN", "Flutterwave NGN", "Fincra NGN", "Zenith"]
}

MONTH_FILTER_BANKS = ["Verto", "Moniepoint"] # Needing month filter

def debug_file_operations():
    """Debug helper to check file system permissions"""
    debug_dir = os.path.expanduser("~/NGN_Reconciliation_Assistant/reports")
    test_file = os.path.join(debug_dir, "test_file.txt")
    
    try:
        # Create directory if needed
        Path(debug_dir).mkdir(parents=True, exist_ok=True)
        
        # Test writing
        with open(test_file, 'w') as f:
            f.write("test content")
        
        # Test reading
        with open(test_file, 'r') as f:
            content = f.read()
        
        # Test deleting
        os.remove(test_file)
        
        return True, f"Success! Can write to {debug_dir}"
    except Exception as e:
        return False, f"Filesystem error: {str(e)}"

def debug_dataframe_operations():
    """Debug helper to check DataFrame saving"""
    try:
        test_df = pd.DataFrame({"test": [1, 2, 3]})
        buffer = StringIO()
        test_df.to_csv(buffer)
        return True, "DataFrame can be converted to CSV"
    except Exception as e:
        return False, f"DataFrame error: {str(e)}"

# --- Helper Functions for File Reading ---
def create_empty_matched_df():
    """Creates an empty matched DataFrame with standard columns"""
    return pd.DataFrame(columns=[
        'Date_Internal', 'Amount_Internal', 'ID_Internal',
        'Date_Bank', 'Amount_Bank', 'ID_Bank',
        'Amount_Rounded', 'Date_Difference_Days'
    ])

def create_empty_unmatched_df():
    """Creates an empty unmatched DataFrame with standard columns"""
    return pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    
def read_uploaded_file(uploaded_file, header=None):
    """
    Reads an uploaded file (BytesIO object) into a pandas DataFrame.
    Handles both CSV and Excel file types based on the file extension.
    If 'header' is None, pandas will infer the header.
    """
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file, header=header)
    elif uploaded_file.name.endswith(('.xlsx', 'xls')):
        # For Excel files, assume the first sheet if a specific sheet name is not given.
        # The user's original code implies CSV exports from Excel sheets,
        # so this logic handles both actual XLSX and CSV-like files.
        try:
            # If the filename suggests a CSV export (e.g., "Sheet0.csv" but from XLSX source)
            # attempt to read it as CSV first to match original code's behavior.
            if "sheet" in uploaded_file.name.lower() and uploaded_file.name.lower().endswith(".csv"):
                 return pd.read_csv(uploaded_file, header=header)
            else:
                 # Otherwise, read as a standard Excel file
                 return pd.read_excel(uploaded_file, header=header)
        except Exception as e:
            st.error(f"Error reading {uploaded_file.name} as Excel or CSV: {e}")
            return None
    else:
        st.error(f"Unsupported file type: {uploaded_file.name}. Please upload CSV or Excel files.")
        return None

def find_column(df, keywords):
    """
    Function to find a column in a DataFrame based on a list of keywords (case-insensitive).
    Returns the found column name or None if not found.
    """
    for col in df.columns:
        if any(keyword.lower() in col.lower() for keyword in keywords):
            return col
    return None

def perform_date_tolerance_matching(unmatched_internal_df, unmatched_bank_df, tolerance_days=3):
    """
    Performs reconciliation on remaining unmatched records with a date tolerance.
    Prioritizes matches with the smallest date difference.
    Assumes 'Date' is a datetime column, 'Amount' is numeric, and 'ID' is unique in both DFs.
    Returns matched_df, remaining_internal_df, remaining_bank_df.
    """
    
    # Ensure date columns are datetime objects and normalized (midnight)
    unmatched_internal_df.loc[:, 'Date'] = pd.to_datetime(unmatched_internal_df['Date'], errors='coerce').dt.normalize()
    unmatched_bank_df.loc[:, 'Date'] = pd.to_datetime(unmatched_bank_df['Date'], errors='coerce').dt.normalize()

    # Drop rows where date conversion failed
    unmatched_internal_df = unmatched_internal_df.dropna(subset=['Date']).copy()
    unmatched_bank_df = unmatched_bank_df.dropna(subset=['Date']).copy()

    if unmatched_internal_df.empty or unmatched_bank_df.empty:
        # st.write("DEBUG: One of the DataFrames for tolerance matching is empty. Skipping tolerance matching.")
        return pd.DataFrame(), unmatched_internal_df.copy(), unmatched_bank_df.copy()

    # Create temporary IDs for tracking if not already unique
    if 'ID' not in unmatched_internal_df.columns or unmatched_internal_df['ID'].duplicated().any():
        unmatched_internal_df.loc[:, '_internal_temp_id'] = range(len(unmatched_internal_df))
    else:
        unmatched_internal_df.loc[:, '_internal_temp_id'] = unmatched_internal_df['ID'] # Use existing ID

    if 'ID' not in unmatched_bank_df.columns or unmatched_bank_df['ID'].duplicated().any():
        unmatched_bank_df.loc[:, '_bank_temp_id'] = range(len(unmatched_bank_df))
    else:
        unmatched_bank_df.loc[:, '_bank_temp_id'] = unmatched_bank_df['ID'] # Use existing ID


    # Step 1: Find all potential matches within the tolerance window
    all_potential_matches = pd.merge(
        unmatched_internal_df,
        unmatched_bank_df,
        on='Amount_Rounded',
        how='inner',
        suffixes=('_internal', '_bank')
    )

    if all_potential_matches.empty:
        # st.write("DEBUG: No potential matches found in tolerance matching before date diff.")
        return pd.DataFrame(), unmatched_internal_df.drop(columns=['_internal_temp_id'], errors='ignore'), unmatched_bank_df.drop(columns=['_bank_temp_id'], errors='ignore')

    # Calculate date difference
    all_potential_matches.loc[:, 'date_diff'] = (
        all_potential_matches['Date_internal'] - all_potential_matches['Date_bank']
    ).abs()

    # Filter by tolerance
    all_potential_matches = all_potential_matches[
        all_potential_matches['date_diff'] <= pd.Timedelta(days=tolerance_days)
    ].copy()

    if all_potential_matches.empty:
        # st.write("DEBUG: No tolerant matches found after filtering by tolerance.")
        return pd.DataFrame(), unmatched_internal_df.drop(columns=['_internal_temp_id'], errors='ignore'), unmatched_bank_df.drop(columns=['_bank_temp_id'], errors='ignore')

    # Step 2: Prioritize and select unique matches (THIS IS THE CRITICAL PART)
    # Sort by date difference (ascending) to prefer closer dates, then by amount (desc) for stability
    all_potential_matches_sorted = all_potential_matches.sort_values(
        by=['date_diff', 'Amount_Rounded'], ascending=[True, False]
    ).copy()

    # Drop duplicates to get unique matches:
    # First, keep the first match for each internal ID
    # Then, from the remaining, keep the first match for each bank ID
    # This greedy approach prioritizes closer dates and ensures 1:1 matching
    matched_df_unique = all_potential_matches_sorted.drop_duplicates(
        subset=['_internal_temp_id'], keep='first'
    ).drop_duplicates(
        subset=['_bank_temp_id'], keep='first'
    ).copy()

    # Determine matched IDs
    matched_internal_ids = matched_df_unique['_internal_temp_id'].unique()
    matched_bank_ids = matched_df_unique['_bank_temp_id'].unique()

    # Determine remaining unmatched records
    remaining_internal_df = unmatched_internal_df[
        ~unmatched_internal_df['_internal_temp_id'].isin(matched_internal_ids)
    ].drop(columns=['_internal_temp_id'], errors='ignore').copy()

    remaining_bank_df = unmatched_bank_df[
        ~unmatched_bank_df['_bank_temp_id'].isin(matched_bank_ids)
    ].drop(columns=['_bank_temp_id'], errors='ignore').copy()
    
    # Clean up and select relevant columns for matched_df_unique
    output_cols_mapping = {
        'Date_internal': 'Date_Internal', 'Amount_internal': 'Amount_Internal', 'ID_internal': 'ID_Internal',
        'Date_bank': 'Date_Bank', 'Amount_bank': 'Amount_Bank', 'ID_bank': 'ID_Bank',
        'Amount_Rounded': 'Amount_Rounded', 'date_diff': 'Date_Difference_Days'
    }
    
    # Select and rename columns
    final_matched_df_cols = {}
    for original_col, new_col in output_cols_mapping.items():
        if original_col in matched_df_unique.columns:
            final_matched_df_cols[original_col] = new_col
    
    matched_df_unique = matched_df_unique.rename(columns=final_matched_df_cols)
    
    # Convert date_diff to days for readability
    if 'Date_Difference_Days' in matched_df_unique.columns:
        matched_df_unique['Date_Difference_Days'] = matched_df_unique['Date_Difference_Days'].dt.days

    return matched_df_unique, remaining_internal_df, remaining_bank_df

def validate_summary_data(summary_dict):
    """Ensures the summary contains all required fields with proper types."""
    required_fields = {
        "Provider name": str,
        "Currency": str,
        "Month & Year": str,
        "# of Transactions": int,
        "Partner Statement": float,
        "Treasury Records": float,
        "Variance": float,
        "% accuracy": str,
        "Status": str
    }
    
    validated = {}
    for field, dtype in required_fields.items():
        if field not in summary_dict:
            st.error(f"Missing required field: {field}")
            return None
        try:
            validated[field] = dtype(summary_dict[field])
        except (ValueError, TypeError):
            st.error(f"Invalid type for {field}. Expected {dtype.__name__}")
            return None
    return validated

# --- Reconciliation Logic Functions ---
def reconcile_equity_ke(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Equity KE.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=8).
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = pd.DataFrame(columns=[
        'Date_Internal', 'Amount_Internal', 'ID_Internal',
        'Date_Bank', 'Amount_Bank', 'ID_Bank',
        'Amount_Rounded'
    ])
    unmatched_internal = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    unmatched_bank = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    summary = {}

    try:
        # --- 1. Load the datasets using read_uploaded_file ---
        equity_hex_df = read_uploaded_file(internal_file_obj, header=0)
        equity_ke_df = read_uploaded_file(bank_file_obj, header=8)
    except Exception as e:
        st.error(f"Error reading files for Equity KE: {e}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    if equity_hex_df is None or equity_ke_df is None:
        st.error("One or both files could not be loaded for Equity KE.")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    # --- 2. Preprocessing for equity_hex_df (Internal Records) ---
    try:
        equity_hex_df.columns = equity_hex_df.columns.astype(str).str.strip()

        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if 'TRANSFER_ID' in equity_hex_df.columns:
            internal_required_cols.append('TRANSFER_ID')

        if not all(col in equity_hex_df.columns for col in internal_required_cols if col != 'TRANSFER_ID'):
            missing_cols = [col for col in ['TRANSFER_DATE', 'AMOUNT'] if col not in equity_hex_df.columns]
            st.error(f"Internal records (Equity Hex) are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        equity_hex_df_processed = equity_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date', 'AMOUNT': 'Amount'
        }).copy()
        
        if 'TRANSFER_ID' in equity_hex_df.columns:
            equity_hex_df_processed = equity_hex_df_processed.rename(columns={'TRANSFER_ID': 'ID'})
        else:
            equity_hex_df_processed['ID'] = 'Internal_' + equity_hex_df_processed.index.astype(str)

        equity_hex_df_processed['Date'] = pd.to_datetime(equity_hex_df_processed['Date'], errors='coerce')
        equity_hex_df_processed = equity_hex_df_processed.dropna(subset=['Date']).copy()

        equity_hex_df_recon = equity_hex_df_processed[equity_hex_df_processed['Amount'] > 0].copy()
        equity_hex_df_recon.loc[:, 'Amount_Rounded'] = equity_hex_df_recon['Amount'].round(2)
        # Add Date_Match column for consistent merging
        equity_hex_df_recon.loc[:, 'Date_Match'] = equity_hex_df_recon['Date'].dt.date

        if equity_hex_df_recon.empty:
            st.warning("No valid internal records found after preprocessing for Equity KE.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- Extract currency from equity_hex_df ---
        extracted_currency = "N/A"
        if 'CURRENCY' in equity_hex_df.columns and not equity_hex_df['CURRENCY'].empty:
            unique_currencies = equity_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])
            else:
                extracted_currency = "N/A (No currency in column)"
        else:
            extracted_currency = "N/A (CURRENCY column missing or empty)"

        # --- 3. Preprocessing for equity_ke_df (Bank Statements) ---
        equity_ke_df.columns = equity_ke_df.columns.str.strip()

        bank_required_cols = ['Transaction Date', 'Credit']
        if 'Transaction Ref' in equity_ke_df.columns:
            bank_required_cols.append('Transaction Ref')

        if not all(col in equity_ke_df.columns for col in bank_required_cols if col != 'Transaction Ref'):
            missing_cols = [col for col in ['Transaction Date', 'Credit'] if col not in equity_ke_df.columns]
            st.error(f"Bank statement (EquityKE) is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        equity_ke_df_processed = equity_ke_df.rename(columns={
            'Transaction Date': 'Date', 'Credit': 'Amount'
        }).copy()

        if 'Transaction Ref' in equity_ke_df.columns:
            equity_ke_df_processed = equity_ke_df_processed.rename(columns={'Transaction Ref': 'ID'})
        else:
            equity_ke_df_processed['ID'] = 'Bank_' + equity_ke_df_processed.index.astype(str)

        equity_ke_df_processed['Date'] = pd.to_datetime(equity_ke_df_processed['Date'], dayfirst=True, errors='coerce')
        equity_ke_df_processed = equity_ke_df_processed.dropna(subset=['Date']).copy()

        equity_ke_df_processed['Amount'] = pd.to_numeric(equity_ke_df_processed['Amount'], errors='coerce').fillna(0)
        
        equity_ke_df_recon = equity_ke_df_processed[equity_ke_df_processed['Amount'] > 0].copy()
        equity_ke_df_recon.loc[:, 'Amount_Rounded'] = equity_ke_df_recon['Amount'].round(2)
        # Add Date_Match column for consistent merging
        equity_ke_df_recon.loc[:, 'Date_Match'] = equity_ke_df_recon['Date'].dt.date

        if equity_ke_df_recon.empty:
            st.warning("No valid bank records found after basic preprocessing for Equity KE.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- Filter bank records by 'RTGS NALA' in 'Narrative' if 'Narrative' exists ---
        if 'Narrative' in equity_ke_df.columns: # Check original df for Narrative
            if 'Narrative' in equity_ke_df_recon.columns: # Ensure 'Narrative' column exists in recon DF
                equity_ke_df_recon['Narrative'] = equity_ke_df_recon['Narrative'].astype(str)
                equity_ke_df_recon = equity_ke_df_recon[
                    equity_ke_df_recon['Narrative'].str.contains('RTGS NALA', case=False, na=False)
                ].copy()
                equity_ke_df_recon = equity_ke_df_recon.drop(columns=['Narrative'], errors='ignore')
            else:
                pass # Narrative not in recon DF, so no filter applied
        else:
            st.warning("Bank statement (EquityKE) does not have a 'Narrative' column. Skipping 'RTGS NALA' filter.")

        if equity_ke_df_recon.empty:
            st.warning("No valid bank records found after 'RTGS NALA' filter for Equity KE.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = equity_hex_df_recon['Amount'].sum()
        total_bank_credits = equity_ke_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (transaction-level) ---
        reconciled_df = pd.merge(
            equity_hex_df_recon.assign(Source_Internal='Internal'),
            equity_ke_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'], # MERGING ON Date_Match and Amount_Rounded
            how='outer',
            suffixes=('_Internal', '_Bank'))
        
        # Identify matched transactions
        temp_matched = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not temp_matched.empty:
            # Select only columns that exist in temp_matched from the desired list
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in temp_matched.columns]
            matched_transactions = temp_matched[cols_to_select].copy()
        else:
            matched_transactions = pd.DataFrame(columns=[
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ])

        # Identify unmatched internal transactions
        temp_unmatched_internal = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not temp_unmatched_internal.empty:
            # Define desired columns BEFORE renaming for internal unmatched
            desired_internal_cols = ['Date_Match_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']
            # Select only columns that exist and then rename
            cols_existing_internal = [col for col in desired_internal_cols if col in temp_unmatched_internal.columns]
            
            unmatched_internal = temp_unmatched_internal[cols_existing_internal].rename(columns={
                'Date_Match_Internal': 'Date',
                'Amount_Internal': 'Amount',
                'ID_Internal': 'ID'
            }).copy()
            
            # Apply the string formatting and fillna after renaming
            if 'Date' in unmatched_internal.columns: # Extra safeguard
                unmatched_internal['Date'] = pd.to_datetime(unmatched_internal['Date'], errors='coerce')
                unmatched_internal['Date'] = unmatched_internal['Date'].dt.strftime('%Y-%m-%d').fillna('')
        else:
            unmatched_internal = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])

        # Identify unmatched bank transactions
        temp_unmatched_bank = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not temp_unmatched_bank.empty:
            # Define desired columns BEFORE renaming for bank unmatched
            desired_bank_cols = ['Date_Match_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']
            # Select only columns that exist and then rename
            cols_existing_bank = [col for col in desired_bank_cols if col in temp_unmatched_bank.columns]

            unmatched_bank = temp_unmatched_bank[cols_existing_bank].rename(columns={
                'Date_Match_Bank': 'Date',
                'Amount_Bank': 'Amount',
                'ID_Bank': 'ID'
            }).copy()
            
            # Apply the string formatting and fillna after renaming
            if 'Date' in unmatched_bank.columns: # Extra safeguard
                unmatched_bank['Date'] = pd.to_datetime(unmatched_bank['Date'], errors='coerce')
                unmatched_bank['Date'] = unmatched_bank['Date'].dt.strftime('%Y-%m-%d').fillna('')
        else:
            unmatched_bank = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
        
        unmatched_internal_amount_final = unmatched_internal['Amount'].sum() if not unmatched_internal.empty else 0.0
        unmatched_bank_amount_final = unmatched_bank['Amount'].sum() if not unmatched_bank.empty else 0.0

        # --- 6. Summary of Reconciliation ---
        summary = {
            "Total Internal Records (for recon)": len(equity_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(equity_ke_df_recon),
            "Total Internal Credits (Original)": total_internal_credits,
            "Total Bank Credits (Original)": total_bank_credits,
            "Overall Discrepancy (Original)": discrepancy_amount,
            "Total Matched Transactions (All Stages)": len(matched_transactions),
            "Unmatched Internal Records (Final)": len(unmatched_internal),
            "Unmatched Bank Records (Final)": len(unmatched_bank),
            "Unmatched Internal Amount (Final)": unmatched_internal_amount_final,
            "Unmatched Bank Amount (Final)": unmatched_bank_amount_final,
            "Currency": extracted_currency
        }

    except Exception as e:
        st.error(f"Error during Equity KE reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    # --- 7. Return the results ---
    return matched_transactions, unmatched_internal, unmatched_bank, summary

def reconcile_cellulant_ke(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Cellulant KE.
    Expects internal_file_obj (CSV) and bank_file_obj (CSV with header=5).
    Includes date tolerance matching (up to 3 days).
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    try:
        cellulant_hex_df = read_uploaded_file(internal_file_obj, header=0)
        cellulant_ke_df = read_uploaded_file(bank_file_obj, header=5)
    except Exception as e:
        st.error(f"Error reading files for Cellulant KE: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    if cellulant_hex_df is None or cellulant_ke_df is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # --- 2. Preprocessing for cellulant_hex_df (Internal Records) ---
    cellulant_hex_df.columns = cellulant_hex_df.columns.astype(str).str.strip()

    # Essential columns for internal records
    internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
    if not all(col in cellulant_hex_df.columns for col in internal_required_cols):
        missing_cols = [col for col in internal_required_cols if col not in cellulant_hex_df.columns]
        st.error(f"Internal records (Cellulant Hex) are missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    cellulant_hex_df = cellulant_hex_df.rename(columns={
        'TRANSFER_DATE': 'Date', 'AMOUNT': 'Amount', 'COMMENT': 'Description'
    })
    cellulant_hex_df['Date'] = pd.to_datetime(cellulant_hex_df['Date'], errors='coerce')
    cellulant_hex_df = cellulant_hex_df.dropna(subset=['Date']).copy() # Drop rows where Date couldn't be parsed

    cellulant_hex_df_recon = cellulant_hex_df[cellulant_hex_df['Amount'] > 0].copy()
    # Ensure 'TRANSFER_ID' is handled gracefully if it doesn't exist
    if 'TRANSFER_ID' in cellulant_hex_df_recon.columns:
        cellulant_hex_df_recon = cellulant_hex_df_recon[['Date', 'Amount', 'Description', 'TRANSFER_ID']].copy()
    else:
        cellulant_hex_df_recon = cellulant_hex_df_recon[['Date', 'Amount', 'Description']].copy()

    cellulant_hex_df_recon.loc[:, 'Date_Match'] = cellulant_hex_df_recon['Date'].dt.date

    extracted_currency = "N/A"
    if 'CURRENCY' in cellulant_hex_df.columns and not cellulant_hex_df['CURRENCY'].empty:
        unique_currencies = cellulant_hex_df['CURRENCY'].dropna().unique()
        if unique_currencies.size > 0:
            extracted_currency = str(unique_currencies[0])
        else:
            extracted_currency = "N/A (No currency in column)"
    else:
        extracted_currency = "N/A (CURRENCY column missing or empty)"


    # --- 3. Preprocessing for cellulant_ke_df (Bank Statements) ---
    cellulant_ke_df.columns = cellulant_ke_df.columns.astype(str).str.strip()

    # Essential columns for bank statements
    bank_required_cols = ['DateTime', 'Credit Amount']
    if not all(col in cellulant_ke_df.columns for col in bank_required_cols):
        missing_cols = [col for col in bank_required_cols if col not in cellulant_ke_df.columns]
        st.error(f"Bank statement (Cellulant KE) is missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    cellulant_ke_df = cellulant_ke_df.rename(columns={
        'DateTime': 'Date', 'Credit Amount': 'Credit', 'Transaction Type': 'Description',
        'Customer Float Transaction ID': 'ID'
    })
    cellulant_ke_df['Date'] = pd.to_datetime(cellulant_ke_df['Date'], infer_datetime_format=True, errors='coerce')
    cellulant_ke_df = cellulant_ke_df.dropna(subset=['Date']).copy() # Drop rows where Date couldn't be parsed

    # Handle timezone if present, localize to None
    if cellulant_ke_df['Date'].dt.tz is not None:
        cellulant_ke_df['Date'] = cellulant_ke_df['Date'].dt.tz_localize(None)

    cellulant_ke_df.loc[:, 'Date_Match'] = cellulant_ke_df['Date'].dt.date

    cellulant_ke_df['Credit'] = pd.to_numeric(
        cellulant_ke_df['Credit'].astype(str).str.replace('+', '', regex=False).str.replace(',', '', regex=False),
        errors='coerce'
    ).fillna(0)

    cellulant_ke_df['Amount'] = cellulant_ke_df['Credit']
    cellulant_ke_df_recon = cellulant_ke_df[cellulant_ke_df['Amount'] > 0].copy()

    # Ensure 'ID' is handled gracefully if it doesn't exist
    if 'ID' in cellulant_ke_df_recon.columns:
        cellulant_ke_df_recon = cellulant_ke_df_recon[['Date', 'Amount', 'Description', 'ID', 'Date_Match']].copy()
    else:
        cellulant_ke_df_recon = cellulant_ke_df_recon[['Date', 'Amount', 'Description', 'Date_Match']].copy()


    # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
    total_internal_credits = cellulant_hex_df_recon['Amount'].sum()
    total_bank_credits = cellulant_ke_df_recon['Amount'].sum()
    discrepancy_amount = total_internal_credits - total_bank_credits


    # Add Amount_Rounded to the recon DFs for matching
    cellulant_hex_df_recon.loc[:, 'Amount_Rounded'] = cellulant_hex_df_recon['Amount'].round(2)
    cellulant_ke_df_recon.loc[:, 'Amount_Rounded'] = cellulant_ke_df_recon['Amount'].round(2)


    # --- 5. Initial Reconciliation (transaction-level: exact date & amount) ---
    reconciled_cellulant_df = pd.merge(
        cellulant_hex_df_recon.assign(Source_Internal='Internal'),
        cellulant_ke_df_recon.assign(Source_Bank='Bank'),
        on=['Date_Match', 'Amount_Rounded'],
        how='outer',
        suffixes=('_Internal', '_Bank')
    )

    # Identify initially matched transactions
    matched_initial = reconciled_cellulant_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()

    # Identify initially unmatched internal transactions (prepare for next stage)
    unmatched_internal_initial = reconciled_cellulant_df[reconciled_cellulant_df['Source_Bank'].isna()].copy()
    if not unmatched_internal_initial.empty and \
       all(col in unmatched_internal_initial.columns for col in ['Date_Match', 'Amount_Internal', 'Amount_Rounded', 'Source_Internal']):
        unmatched_internal_initial = unmatched_internal_initial[[
            'Date_Match', 'Amount_Internal', 'Amount_Rounded', 'Source_Internal'
        ]].rename(columns={
            'Date_Match': 'Date', 'Amount_Internal': 'Amount', 'Source_Internal': 'Source'
        }).copy()
        unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
    else:
        unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'Amount_Rounded', 'Source'])
        unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])


    # Identify initially unmatched bank transactions (prepare for next stage)
    unmatched_bank_initial = reconciled_cellulant_df[reconciled_cellulant_df['Source_Internal'].isna()].copy()
    if not unmatched_bank_initial.empty and \
       all(col in unmatched_bank_initial.columns for col in ['Date_Match', 'Amount_Bank', 'Amount_Rounded', 'Source_Bank']):
        unmatched_bank_initial = unmatched_bank_initial[[
            'Date_Match', 'Amount_Bank', 'Amount_Rounded', 'Source_Bank'
        ]].rename(columns={
            'Date_Match': 'Date', 'Amount_Bank': 'Amount', 'Source_Bank': 'Source'
        }).copy()
        unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
    else:
        unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'Amount_Rounded', 'Source'])
        unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])


    # --- Stage 1: Date Tolerance Matching ---
    matched_date_tolerance_df = pd.DataFrame()
    final_unmatched_internal = unmatched_internal_initial.copy()
    final_unmatched_bank = unmatched_bank_initial.copy()

    if not unmatched_internal_initial.empty and not unmatched_bank_initial.empty:
        st.info("Attempting date tolerance matching for remaining unmatched records (Cellulant KE)...")
        matched_date_tolerance_df, final_unmatched_internal, final_unmatched_bank = \
            perform_date_tolerance_matching(
                unmatched_internal_initial,
                unmatched_bank_initial,
                tolerance_days=3 # Allowing up to 3 days difference, similar to Equity TZ
            )
        # Combine matched records from initial and date tolerance stages
        matched_total = pd.concat([matched_initial, matched_date_tolerance_df], ignore_index=True)
    else:
        matched_total = matched_initial


    # --- Stage 2: Daily Grouping and Amount Matching (To be implemented later) ---
    # As discussed, we'll implement this stage after date tolerance is verified.


    # --- 6. Summary of Reconciliation ---
    total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
    total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
    remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
    remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

    summary = {
        "Total Internal Records (for recon)": len(cellulant_hex_df_recon),
        "Total Bank Statement Records (for recon)": len(cellulant_ke_df_recon),
        "Total Internal Credits (Original)": total_internal_credits,
        "Total Bank Credits (Original)": total_bank_credits,
        "Overall Discrepancy (Original)": discrepancy_amount,
        "Total Matched Transactions (All Stages)": len(matched_total),
        "Total Matched Amount (Internal)": total_matched_amount_internal,
        "Total Matched Amount (Bank)": total_matched_amount_bank,
        "Unmatched Internal Records (Final)": len(final_unmatched_internal),
        "Unmatched Bank Records (Final)": len(final_unmatched_bank),
        "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
        "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
        "Currency": extracted_currency
    }

    # --- 7. Return the results ---
    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_zamupay(internal_file_obj, bank_file_obj):
    """
    Performs comprehensive reconciliation for Zamupay (PYCS).
    Incorporates exact match, 3-day date tolerance, and split transaction aggregation.
    Expects internal_file_obj (CSV) and bank_file_obj (CSV).
    Returns matched_total, final_unmatched_internal, final_unmatched_bank dataframes,
    and a summary dictionary.
    """
    try:
        zamupay_internal_df = read_uploaded_file(internal_file_obj, header=0)
        zamupay_bank_df = read_uploaded_file(bank_file_obj, header=0)

        # --- Extract currency from internal_df ---
        extracted_currency = "N/A" # Default in case column is missing or empty
        if 'CURRENCY' in zamupay_internal_df.columns and not zamupay_internal_df['CURRENCY'].empty:
            # Get the first unique currency. Assuming consistency.
            unique_currencies = zamupay_internal_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0]) # Convert to string
            else:
                extracted_currency = "N/A (No currency in column)"
        else:
            extracted_currency = "N/A (CURRENCY column missing or empty)"

    except Exception as e:
        st.error(f"Error reading files for Zamupay: {e}")
        # Return empty dataframes and an empty summary if file reading fails
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    if zamupay_internal_df is None or zamupay_bank_df is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # --- 1. Preprocessing for Zamupay Internal Records ---
    zamupay_internal_df.columns = zamupay_internal_df.columns.astype(str).str.strip()

    # Essential columns check for internal records
    internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
    if not all(col in zamupay_internal_df.columns for col in internal_required_cols):
        missing_cols = [col for col in internal_required_cols if col not in zamupay_internal_df.columns]
        st.error(f"Internal records (Zamupay) are missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    zamupay_internal_df = zamupay_internal_df.rename(columns={'TRANSFER_DATE': 'Date', 'AMOUNT': 'Amount'})
    zamupay_internal_df['Date'] = pd.to_datetime(zamupay_internal_df['Date'], errors='coerce')
    zamupay_internal_df = zamupay_internal_df.dropna(subset=['Date']).copy()

    zamupay_internal_df['Amount'] = zamupay_internal_df['Amount'].astype(str).str.replace(',', '', regex=False).astype(float)
    zamupay_internal_df_recon = zamupay_internal_df[zamupay_internal_df['Amount'] > 0].copy()
    zamupay_internal_df_recon.loc[:, 'Date_Match'] = zamupay_internal_df_recon['Date'].dt.date


    # --- 2. Preprocessing for Zamupay Bank Statements ---
    zamupay_bank_df.columns = zamupay_bank_df.columns.astype(str).str.strip()

    # Essential columns check for bank statements
    bank_required_cols = ['Tran. Date', 'Credit Amt.', 'Particulars']
    if not all(col in zamupay_bank_df.columns for col in bank_required_cols):
        missing_cols = [col for col in bank_required_cols if col not in zamupay_bank_df.columns]
        st.error(f"Bank statement (Zamupay) is missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    zamupay_bank_df = zamupay_bank_df.rename(columns={
        'Tran. Date': 'Date',
        'Credit Amt.': 'Amount',
        'Particulars': 'Description'
    })
    zamupay_bank_df['Date'] = pd.to_datetime(zamupay_bank_df['Date'], errors='coerce')
    zamupay_bank_df = zamupay_bank_df.dropna(subset=['Date']).copy()

    zamupay_bank_df['Amount'] = zamupay_bank_df['Amount'].astype(str).str.replace(',', '', regex=False).astype(float)

    # --- Filter out records with "REVERSAL" in 'Description' ---
    if 'Description' in zamupay_bank_df.columns:
        zamupay_bank_df = zamupay_bank_df[
            ~zamupay_bank_df['Description'].astype(str).str.contains('REVERSAL', case=False, na=False)
        ].copy()
    else:
        st.warning("Warning: 'Description' (Particulars) column not found in bank statement. Skipping 'REVERSAL' filter.")

    zamupay_bank_df_recon = zamupay_bank_df[zamupay_bank_df['Amount'] > 0].copy()
    zamupay_bank_df_recon.loc[:, 'Date_Match'] = zamupay_bank_df_recon['Date'].dt.date


    # --- 3. Calculate Total Amounts and Discrepancy (before reconciliation) ---
    total_internal_credits = zamupay_internal_df_recon['Amount'].sum()
    total_bank_credits = zamupay_bank_df_recon['Amount'].sum()
    discrepancy_amount = total_internal_credits - total_bank_credits


    # --- 4. Reconciliation (transaction-level, exact date match) ---
    zamupay_internal_df_recon.loc[:, 'Amount_Rounded'] = zamupay_internal_df_recon['Amount'].round(2)
    zamupay_bank_df_recon.loc[:, 'Amount_Rounded'] = zamupay_bank_df_recon['Amount'].round(2)

    reconciled_zamupay_df_exact = pd.merge(
        zamupay_internal_df_recon.assign(Source_Internal='Internal'),
        zamupay_bank_df_recon.assign(Source_Bank='Bank'),
        on=['Date_Match', 'Amount_Rounded'],
        how='outer',
        suffixes=('_Internal', '_Bank')
    )

    matched_zamupay_transactions_exact = reconciled_zamupay_df_exact.dropna(subset=['Source_Internal', 'Source_Bank']).copy()

    # Prepare initially unmatched internal transactions for the next stage (Date Tolerance)
    unmatched_internal_for_tolerance = reconciled_zamupay_df_exact[reconciled_zamupay_df_exact['Source_Bank'].isna()].copy()
    if not unmatched_internal_for_tolerance.empty and \
       all(col in unmatched_internal_for_tolerance.columns for col in ['Date_Match', 'Amount_Internal', 'Amount_Rounded', 'Source_Internal']):
        unmatched_internal_for_tolerance = unmatched_internal_for_tolerance[[
            'Date_Match', 'Amount_Internal', 'Amount_Rounded', 'Source_Internal'
        ]].rename(columns={
            'Date_Match': 'Date', 'Amount_Internal': 'Amount', 'Source_Internal': 'Source'
        }).copy()
        unmatched_internal_for_tolerance['Date'] = pd.to_datetime(unmatched_internal_for_tolerance['Date'])
    else:
        unmatched_internal_for_tolerance = pd.DataFrame(columns=['Date', 'Amount', 'Amount_Rounded', 'Source'])
        unmatched_internal_for_tolerance['Date'] = pd.to_datetime(unmatched_internal_for_tolerance['Date'])

    # Prepare initially unmatched bank transactions for the next stage (Date Tolerance)
    unmatched_bank_for_tolerance = reconciled_zamupay_df_exact[reconciled_zamupay_df_exact['Source_Internal'].isna()].copy()
    if not unmatched_bank_for_tolerance.empty and \
       all(col in unmatched_bank_for_tolerance.columns for col in ['Date_Match', 'Amount_Bank', 'Amount_Rounded', 'Source_Bank']):
        unmatched_bank_for_tolerance = unmatched_bank_for_tolerance[[
            'Date_Match', 'Amount_Bank', 'Amount_Rounded', 'Source_Bank'
        ]].rename(columns={
            'Date_Match': 'Date', 'Amount_Bank': 'Amount', 'Source_Bank': 'Source'
        }).copy()
        unmatched_bank_for_tolerance['Date'] = pd.to_datetime(unmatched_bank_for_tolerance['Date'])
    else:
        unmatched_bank_for_tolerance = pd.DataFrame(columns=['Date', 'Amount', 'Amount_Rounded', 'Source'])
        unmatched_bank_for_tolerance['Date'] = pd.to_datetime(unmatched_bank_for_tolerance['Date'])


    # --- 5. Reconciliation with Date Tolerance (3 days) using perform_date_tolerance_matching ---
    matched_zamupay_with_tolerance = pd.DataFrame()
    unmatched_internal_after_tolerance = unmatched_internal_for_tolerance.copy()
    unmatched_bank_after_tolerance = unmatched_bank_for_tolerance.copy()

    if not unmatched_internal_for_tolerance.empty and not unmatched_bank_for_tolerance.empty:
        st.info("Attempting date tolerance matching for remaining unmatched records (Zamupay)...")
        matched_zamupay_with_tolerance, unmatched_internal_after_tolerance, unmatched_bank_after_tolerance = \
            perform_date_tolerance_matching(
                unmatched_internal_for_tolerance,
                unmatched_bank_for_tolerance,
                tolerance_days=3 # Allowing up to 3 days difference
            )


    # --- 6. Reconciliation by Grouping Bank Records (Split Transactions) ---
    matched_by_aggregation_list = []
    # Copy for aggregation, ensuring 'Date' column is datetime for manipulation
    temp_unmatched_bank_for_agg = unmatched_bank_after_tolerance.copy()
    temp_unmatched_bank_for_agg['Date_DT'] = pd.to_datetime(temp_unmatched_bank_for_agg['Date'])

    bank_indices_matched_by_agg = []
    internal_indices_matched_by_agg = []

    current_unmatched_internal_agg = unmatched_internal_after_tolerance.copy()

    if not current_unmatched_internal_agg.empty and not temp_unmatched_bank_for_agg.empty:
        st.info("Attempting aggregation matching for remaining unmatched records (Zamupay)...")
        for i, internal_row in current_unmatched_internal_agg.iterrows():
            internal_date = pd.to_datetime(internal_row['Date'])
            internal_amount = internal_row['Amount_Rounded']

            # Define date range for tolerance (already handled by previous stage, but re-applying for safety)
            start_date = internal_date - pd.Timedelta(days=3)
            end_date = internal_date + pd.Timedelta(days=3)

            # Get potential bank matches within the date tolerance from the *remaining* unmatched bank records
            potential_bank_records_in_range = temp_unmatched_bank_for_agg[
                (temp_unmatched_bank_for_agg['Date_DT'] >= start_date) &
                (temp_unmatched_bank_for_agg['Date_DT'] <= end_date)
            ]

            # Group these potential bank records by date and sum their amounts
            grouped_bank_sums = potential_bank_records_in_range.groupby('Date_DT')['Amount_Rounded'].sum().reset_index()

            # Find if any aggregated sum matches the internal amount
            matched_agg_bank_entry = grouped_bank_sums[
                grouped_bank_sums['Amount_Rounded'].round(2) == internal_amount
            ]

            if not matched_agg_bank_entry.empty:
                # Take the first aggregated match
                agg_date_dt = matched_agg_bank_entry.iloc[0]['Date_DT']
                agg_amount = matched_agg_bank_entry.iloc[0]['Amount_Rounded']

                # Get the original individual bank records that sum up to this aggregation
                contributing_bank_records = temp_unmatched_bank_for_agg[
                    (temp_unmatched_bank_for_agg['Date_DT'] == agg_date_dt)
                ]

                # Double check if the sum of these contributing records still equals the internal amount
                if contributing_bank_records['Amount_Rounded'].sum().round(2) == internal_amount:
                    new_matched_row = {
                        'Date_Internal': internal_row['Date'],
                        'Amount_Internal': internal_row['Amount'],
                        'Date_Match_Internal': internal_row['Date'].date(),
                        'Source_Internal': internal_row['Source'], # Use Source from tolerance stage
                        'Date_Bank': None, # This will be set to the aggregation date
                        'Amount_Bank': agg_amount,
                        'Date_Match_Bank': agg_date_dt.date(),
                        'Source_Bank': 'Bank (Aggregated)',
                        'Amount_Rounded': internal_amount
                    }
                    matched_by_aggregation_list.append(new_matched_row)

                    # Mark internal index for removal
                    internal_indices_matched_by_agg.append(i)

                    # Mark all contributing bank records for removal
                    bank_indices_matched_by_agg.extend(contributing_bank_records.index.tolist())
                    # Remove them from temp_unmatched_bank_for_agg to avoid re-matching
                    temp_unmatched_bank_for_agg = temp_unmatched_bank_for_agg.drop(contributing_bank_records.index)


    matched_zamupay_by_aggregation = pd.DataFrame(matched_by_aggregation_list)

    # Remove matched records from the current unmatched dataframes
    final_unmatched_zamupay_internal = current_unmatched_internal_agg.drop(internal_indices_matched_by_agg)
    # Remove only those bank records that were part of an aggregation
    final_unmatched_zamupay_bank = temp_unmatched_bank_for_agg.drop(columns=['Date_DT'], errors='ignore') # Remove temp column


    # --- 7. Final Summary of Reconciliation ---
    # Combine all matched dataframes for total counts and amounts
    all_matched_dfs = [matched_zamupay_transactions_exact, matched_zamupay_with_tolerance, matched_zamupay_by_aggregation]
    # Filter out empty dataframes before concatenation
    all_matched_dfs = [df for df in all_matched_dfs if not df.empty]

    if all_matched_dfs:
        matched_total = pd.concat(all_matched_dfs, ignore_index=True)
    else:
        matched_total = pd.DataFrame(columns=[
            'Date_Internal', 'Amount_Internal', 'Date_Match_Internal', 'Source_Internal',
            'Date_Bank', 'Amount_Bank', 'Date_Match_Bank', 'Source_Bank', 'Amount_Rounded'
        ])

    total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
    total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
    remaining_unmatched_internal_amount = final_unmatched_zamupay_internal['Amount'].sum() if 'Amount' in final_unmatched_zamupay_internal.columns else 0
    remaining_unmatched_bank_amount = final_unmatched_zamupay_bank['Amount'].sum() if 'Amount' in final_unmatched_zamupay_bank.columns else 0

    summary = {
        "Total Internal Records (for recon)": len(zamupay_internal_df_recon),
        "Total Bank Statement Records (for recon)": len(zamupay_bank_df_recon),
        "Total Internal Credits (Original)": total_internal_credits,
        "Total Bank Credits (Original)": total_bank_credits,
        "Overall Discrepancy (Original)": discrepancy_amount,
        "Total Matched Transactions (All Stages)": len(matched_total),
        "Total Matched Amount (Internal)": total_matched_amount_internal,
        "Total Matched Amount (Bank)": total_matched_amount_bank,
        "Unmatched Internal Records (Final)": len(final_unmatched_zamupay_internal),
        "Unmatched Bank Records (Final)": len(final_unmatched_zamupay_bank),
        "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
        "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
        "Currency": extracted_currency
    }
    # Return the aggregated matched dataframe, final unmatched dataframes, and the summary
    return matched_total, final_unmatched_zamupay_internal, final_unmatched_zamupay_bank, summary
    
def reconcile_selcom_tz(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Selcom TZ.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel).
    Includes date tolerance matching (up to 3 days).
    Returns matched_total, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    try:
        # --- 1. Load the datasets using read_uploaded_file ---
        selcom_hex_df = read_uploaded_file(internal_file_obj, header=0)
        selcom_bank_df = read_uploaded_file(bank_file_obj, header=0)
    except Exception as e:
        st.error(f"Error reading files for Selcom TZ: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {} # Return empty DFs and dict on error

    if selcom_hex_df is None or selcom_bank_df is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {} # Return empty DFs and dict if files not loaded

    # --- 2. Preprocessing for selcom_hex_df (Internal Records) ---
    selcom_hex_df.columns = selcom_hex_df.columns.astype(str).str.strip()

    # Essential columns for internal records
    internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
    if not all(col in selcom_hex_df.columns for col in internal_required_cols):
        missing_cols = [col for col in internal_required_cols if col not in selcom_hex_df.columns]
        st.error(f"Internal records (SelcomHex) are missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}
    selcom_hex_df = selcom_hex_df.rename(columns={'TRANSFER_DATE': 'Date', 'AMOUNT': 'Amount'})

    # Convert 'Date' to datetime objects
    selcom_hex_df['Date'] = pd.to_datetime(selcom_hex_df['Date'], errors='coerce')
    selcom_hex_df = selcom_hex_df.dropna(subset=['Date']).copy() # Drop rows where date conversion failed

    # Ensure amount is numeric and positive
    selcom_hex_df['Amount'] = pd.to_numeric(selcom_hex_df['Amount'], errors='coerce').fillna(0)
    selcom_hex_df_recon = selcom_hex_df[selcom_hex_df['Amount'] > 0].copy()
    selcom_hex_df_recon.loc[:, 'Date_Match'] = selcom_hex_df_recon['Date'].dt.date

    # --- Extract currency from selcom_hex_df ---
    extracted_currency = "N/A"
    if 'CURRENCY' in selcom_hex_df.columns and not selcom_hex_df['CURRENCY'].empty:
        unique_currencies = selcom_hex_df['CURRENCY'].dropna().unique()
        if unique_currencies.size > 0:
            extracted_currency = str(unique_currencies[0])
        else:
            extracted_currency = "N/A (No currency in column)"
    else:
        extracted_currency = "N/A (CURRENCY column missing or empty)"


    # --- 3. Preprocessing for selcom_bank_df (Bank Statements) ---
    selcom_bank_df.columns = selcom_bank_df.columns.astype(str).str.strip()

    # Essential columns for bank statements
    bank_required_cols = ['Date', 'Amount']
    # Check if 'Date' or 'DATE' and 'Amount' or 'AMOUNT' exist after initial strip, then rename
    if 'DATE' in selcom_bank_df.columns:
        selcom_bank_df = selcom_bank_df.rename(columns={'DATE': 'Date'})
    if 'AMOUNT' in selcom_bank_df.columns:
        selcom_bank_df = selcom_bank_df.rename(columns={'AMOUNT': 'Amount'})

    if not all(col in selcom_bank_df.columns for col in bank_required_cols):
        missing_cols = [col for col in bank_required_cols if col not in selcom_bank_df.columns]
        st.error(f"Bank statement (Selcom) is missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # Convert 'Date' to datetime objects (auto-infer format)
    selcom_bank_df['Date'] = pd.to_datetime(selcom_bank_df['Date'], errors='coerce')
    selcom_bank_df = selcom_bank_df.dropna(subset=['Date']).copy()

    # --- More Robust Amount Conversion for Bank Data ---
    # Apply string cleaning explicitly before conversion
    if 'Amount' in selcom_bank_df.columns:
        selcom_bank_df['Amount'] = selcom_bank_df['Amount'].astype(str).str.replace(',', '', regex=False).str.strip()
        selcom_bank_df['Amount'] = pd.to_numeric(selcom_bank_df['Amount'], errors='coerce').fillna(0)
    else:
        st.error("Bank statement (Selcom) is missing 'Amount' column after renaming/stripping.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # Filter bank records to include only positive amounts (credits)
    selcom_bank_df_recon = selcom_bank_df[selcom_bank_df['Amount'] > 0].copy()
    selcom_bank_df_recon.loc[:, 'Date_Match'] = selcom_bank_df_recon['Date'].dt.date

    # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
    total_internal_credits = selcom_hex_df_recon['Amount'].sum()
    total_bank_credits = selcom_bank_df_recon['Amount'].sum()
    discrepancy_amount = total_internal_credits - total_bank_credits

    # Add a warning if bank credits are zero, as this seems to be the core issue
    if total_bank_credits == 0 and total_internal_credits > 0:
        st.warning("Warning: Total Bank Statement Credit Amount is 0.00. Please check the 'Amount' column in the bank statement file for correct data and format.")

    # --- 5. Reconciliation (Exact Match) ---
    selcom_hex_df_recon.loc[:, 'Amount_Rounded'] = selcom_hex_df_recon['Amount'].round(2)
    selcom_bank_df_recon.loc[:, 'Amount_Rounded'] = selcom_bank_df_recon['Amount'].round(2)
    reconciled_selcom_df_exact = pd.merge(
        selcom_hex_df_recon.assign(Source_Internal='Internal'),
        selcom_bank_df_recon.assign(Source_Bank='Bank'),
        on=['Date_Match', 'Amount_Rounded'],
        how='outer',
        suffixes=('_Internal', '_Bank')
    )
    matched_exact = reconciled_selcom_df_exact.dropna(subset=['Source_Internal', 'Source_Bank']).copy()

    # Prepare initially unmatched internal transactions for the next stage (Date Tolerance)
    unmatched_internal_for_tolerance = reconciled_selcom_df_exact[reconciled_selcom_df_exact['Source_Bank'].isna()].copy()
    if not unmatched_internal_for_tolerance.empty:
        unmatched_internal_for_tolerance = unmatched_internal_for_tolerance[[
            'Date_Match', 'Amount_Internal', 'Amount_Rounded', 'Source_Internal'
        ]].rename(columns={
            'Date_Match': 'Date', 'Amount_Internal': 'Amount', 'Source_Internal': 'Source'
        }).copy()
        unmatched_internal_for_tolerance['Date'] = pd.to_datetime(unmatched_internal_for_tolerance['Date'])
    else:
        unmatched_internal_for_tolerance = pd.DataFrame(columns=['Date', 'Amount', 'Amount_Rounded', 'Source'])
        unmatched_internal_for_tolerance['Date'] = pd.to_datetime(unmatched_internal_for_tolerance['Date'])

    # Prepare initially unmatched bank transactions for the next stage (Date Tolerance)
    unmatched_bank_for_tolerance = reconciled_selcom_df_exact[reconciled_selcom_df_exact['Source_Internal'].isna()].copy()
    if not unmatched_bank_for_tolerance.empty:
        unmatched_bank_for_tolerance = unmatched_bank_for_tolerance[[
            'Date_Match', 'Amount_Bank', 'Amount_Rounded', 'Source_Bank'
        ]].rename(columns={
            'Date_Match': 'Date', 'Amount_Bank': 'Amount', 'Source_Bank': 'Source'
        }).copy()
        unmatched_bank_for_tolerance['Date'] = pd.to_datetime(unmatched_bank_for_tolerance['Date'])
    else:
        unmatched_bank_for_tolerance = pd.DataFrame(columns=['Date', 'Amount', 'Amount_Rounded', 'Source'])
        unmatched_bank_for_tolerance['Date'] = pd.to_datetime(unmatched_bank_for_tolerance['Date'])

    # --- 6. Reconciliation with Date Tolerance (3 days) using perform_date_tolerance_matching ---
    matched_with_tolerance = pd.DataFrame()
    final_unmatched_internal = unmatched_internal_for_tolerance.copy()
    final_unmatched_bank = unmatched_bank_for_tolerance.copy()

    if not unmatched_internal_for_tolerance.empty and not unmatched_bank_for_tolerance.empty:
        st.info("Attempting date tolerance matching for remaining unmatched records (Selcom TZ)...")
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = \
            perform_date_tolerance_matching(
                unmatched_internal_for_tolerance,
                unmatched_bank_for_tolerance,
                tolerance_days=3 # Allowing up to 3 days difference
            )
    # --- 7. Final Summary of Reconciliation ---
    # Combine all matched dataframes for total counts and amounts
    all_matched_dfs = [matched_exact, matched_with_tolerance]
    all_matched_dfs = [df for df in all_matched_dfs if not df.empty] # Filter out empty DFs
    if all_matched_dfs:
        matched_total = pd.concat(all_matched_dfs, ignore_index=True)
    else:
        matched_total = pd.DataFrame(columns=[
            'Date_Internal', 'Amount_Internal', 'Date_Match_Internal', 'Source_Internal',
            'Date_Bank', 'Amount_Bank', 'Date_Match_Bank', 'Source_Bank', 'Amount_Rounded'
        ])

    total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
    total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
    remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
    remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0
    summary = {
        "Total Internal Records (for recon)": len(selcom_hex_df_recon),
        "Total Bank Statement Records (for recon)": len(selcom_bank_df_recon),
        "Total Internal Credits (Original)": total_internal_credits,
        "Total Bank Credits (Original)": total_bank_credits,
        "Overall Discrepancy (Original)": discrepancy_amount,
        "Total Matched Transactions (All Stages)": len(matched_total),
        "Total Matched Amount (Internal)": total_matched_amount_internal,
        "Total Matched Amount (Bank)": total_matched_amount_bank,
        "Unmatched Internal Records (Final)": len(final_unmatched_internal),
        "Unmatched Bank Records (Final)": len(final_unmatched_bank),
        "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
        "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
        "Currency": extracted_currency
    }
    # --- 8. Return the results ---
    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_equity_tz(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Equity TZ.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=8).
    Includes narrative filtering and date tolerance matching (up to 3 days).
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    try:
        equity_tz_hex_df = read_uploaded_file(internal_file_obj, header=0)
        equity_tz_bank_df = read_uploaded_file(bank_file_obj, header=8)
    except Exception as e:
        st.error(f"Error reading files for Equity TZ: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    if equity_tz_hex_df is None or equity_tz_bank_df is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # --- 2. Preprocessing for equity_tz_hex_df (Internal Records) ---
    equity_tz_hex_df.columns = equity_tz_hex_df.columns.astype(str).str.strip()
    if 'TRANSFER_DATE' in equity_tz_hex_df.columns:
        equity_tz_hex_df = equity_tz_hex_df.rename(columns={'TRANSFER_DATE': 'Date'})
    if 'AMOUNT' in equity_tz_hex_df.columns:
        equity_tz_hex_df = equity_tz_hex_df.rename(columns={'AMOUNT': 'Amount'})

    if 'Date' in equity_tz_hex_df.columns:
        equity_tz_hex_df['Date'] = pd.to_datetime(equity_tz_hex_df['Date'], errors='coerce')
        equity_tz_hex_df = equity_tz_hex_df.dropna(subset=['Date']).copy()
    else:
        st.error("Internal records (EquityTZHex) are missing 'TRANSFER_DATE' column.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    if 'Amount' in equity_tz_hex_df.columns:
        equity_tz_hex_df_recon = equity_tz_hex_df[equity_tz_hex_df['Amount'] > 0].copy()
    else:
        st.error("Internal records (EquityTZHex) are missing 'AMOUNT' column.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    equity_tz_hex_df_recon.loc[:, 'Date_Match'] = equity_tz_hex_df_recon['Date'].dt.date

    extracted_currency = "N/A"
    if 'CURRENCY' in equity_tz_hex_df.columns and not equity_tz_hex_df['CURRENCY'].empty:
        unique_currencies = equity_tz_hex_df['CURRENCY'].dropna().unique()
        if unique_currencies.size > 0:
            extracted_currency = str(unique_currencies[0])
        else:
            extracted_currency = "N/A (No currency in column)"
    else:
        extracted_currency = "N/A (CURRENCY column missing or empty)"

    # --- 3. Preprocessing for equity_tz_bank_df (Bank Statements) ---
    equity_tz_bank_df.columns = equity_tz_bank_df.columns.astype(str).str.strip()
    if 'Transaction Date' in equity_tz_bank_df.columns:
        equity_tz_bank_df = equity_tz_bank_df.rename(columns={'Transaction Date': 'Date'})
    else:
        st.error("Bank statement (EquityTZ) is missing 'Transaction Date' column.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    if 'Date' in equity_tz_bank_df.columns:
        equity_tz_bank_df['Date'] = pd.to_datetime(equity_tz_bank_df['Date'], dayfirst=True, errors='coerce')
        equity_tz_bank_df = equity_tz_bank_df.dropna(subset=['Date']).copy()
    else:
        st.error("Bank statement (EquityTZ) is missing 'Date' column after renaming or conversion failed.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    if 'Credit' in equity_tz_bank_df.columns:
        equity_tz_bank_df['Credit'] = pd.to_numeric(equity_tz_bank_df['Credit'], errors='coerce').fillna(0)
    else:
        st.error("Bank statement (EquityTZ) is missing 'Credit' column.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    equity_tz_bank_df['Amount'] = equity_tz_bank_df['Credit']
    equity_tz_bank_df_recon = equity_tz_bank_df[equity_tz_bank_df['Amount'] > 0].copy()
    equity_tz_bank_df_recon.loc[:, 'Date_Match'] = equity_tz_bank_df_recon['Date'].dt.date

    # --- Filter bank records by 'RTGS NALA' in 'Narrative' ---
    narrative_filter = 'RTGS NALA'
    if 'Narrative' in equity_tz_bank_df_recon.columns:
        equity_tz_bank_df_recon['Narrative'] = equity_tz_bank_df_recon['Narrative'].astype(str)
        equity_tz_bank_df_recon = equity_tz_bank_df_recon[
            equity_tz_bank_df_recon['Narrative'].str.contains(narrative_filter, case=False, na=False)
        ].copy()
        equity_tz_bank_df_recon = equity_tz_bank_df_recon.drop(columns=['Narrative'], errors='ignore')
    else:
        st.warning(f"Bank statement (EquityTZ) does not have a 'Narrative' column. Skipping '{narrative_filter}' filter.")

    # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
    total_internal_credits = equity_tz_hex_df_recon['Amount'].sum()
    total_bank_credits = equity_tz_bank_df_recon['Amount'].sum()
    discrepancy_amount = total_internal_credits - total_bank_credits


    # Add Amount_Rounded to the recon DFs for matching
    equity_tz_hex_df_recon['Amount_Rounded'] = equity_tz_hex_df_recon['Amount'].round(2)
    equity_tz_bank_df_recon['Amount_Rounded'] = equity_tz_bank_df_recon['Amount'].round(2)


    # --- 5. Initial Reconciliation (transaction-level: exact date & amount) ---
    reconciled_equity_tz_df = pd.merge(
        equity_tz_hex_df_recon.assign(Source_Internal='Internal'),
        equity_tz_bank_df_recon.assign(Source_Bank='Bank'),
        on=['Date_Match', 'Amount_Rounded'], # Merge on exact Date_Match and rounded Amount
        how='outer',
        suffixes=('_Internal', '_Bank')
    )

    # Identify initially matched transactions
    matched_initial = reconciled_equity_tz_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()

    # Identify initially unmatched internal transactions (prepare for next stage)
    unmatched_internal_initial = reconciled_equity_tz_df[reconciled_equity_tz_df['Source_Bank'].isna()].copy()
    if not unmatched_internal_initial.empty and \
       all(col in unmatched_internal_initial.columns for col in ['Date_Match', 'Amount_Internal', 'Amount_Rounded', 'Source_Internal']):
        unmatched_internal_initial = unmatched_internal_initial[[
            'Date_Match', 'Amount_Internal', 'Amount_Rounded', 'Source_Internal' # Use single Amount_Rounded
        ]].rename(columns={
            'Date_Match': 'Date', 'Amount_Internal': 'Amount', 'Source_Internal': 'Source'
        }).copy()
        # Convert 'Date' to datetime objects for date tolerance matching
        unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
    else:
        unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'Amount_Rounded', 'Source'])
        unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date']) # Ensure correct dtype

    # Identify initially unmatched bank transactions (prepare for next stage)
    unmatched_bank_initial = reconciled_equity_tz_df[reconciled_equity_tz_df['Source_Internal'].isna()].copy()
    if not unmatched_bank_initial.empty and \
       all(col in unmatched_bank_initial.columns for col in ['Date_Match', 'Amount_Bank', 'Amount_Rounded', 'Source_Bank']):
        unmatched_bank_initial = unmatched_bank_initial[[
            'Date_Match', 'Amount_Bank', 'Amount_Rounded', 'Source_Bank' # Use single Amount_Rounded
        ]].rename(columns={
            'Date_Match': 'Date', 'Amount_Bank': 'Amount', 'Source_Bank': 'Source'
        }).copy()
        # Convert 'Date' to datetime objects for date tolerance matching
        unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
    else:
        unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'Amount_Rounded', 'Source'])
        unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date']) # Ensure correct dtype

    # --- Stage 1: Date Tolerance Matching ---
    matched_date_tolerance_df = pd.DataFrame()
    final_unmatched_internal = unmatched_internal_initial.copy()
    final_unmatched_bank = unmatched_bank_initial.copy()

    if not unmatched_internal_initial.empty and not unmatched_bank_initial.empty:
        st.info("Attempting date tolerance matching for remaining unmatched records (Equity TZ)...")
        matched_date_tolerance_df, final_unmatched_internal, final_unmatched_bank = \
            perform_date_tolerance_matching(
                unmatched_internal_initial,
                unmatched_bank_initial,
                tolerance_days=3 # Allowing up to 3 days difference
            )
        # Combine matched records from initial and date tolerance stages
        matched_total = pd.concat([matched_initial, matched_date_tolerance_df], ignore_index=True)
    else:
        matched_total = matched_initial

    # --- Stage 2: Daily Grouping and Amount Matching (To be implemented later) ---
    # As discussed, we'll implement this stage after date tolerance is verified.

    # --- 6. Summary of Reconciliation ---
    total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
    total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
    remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
    remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

    summary = {
        "Total Internal Records (for recon)": len(equity_tz_hex_df_recon),
        "Total Bank Statement Records (for recon)": len(equity_tz_bank_df_recon),
        "Total Internal Credits (Original)": total_internal_credits,
        "Total Bank Credits (Original)": total_bank_credits,
        "Overall Discrepancy (Original)": discrepancy_amount,
        "Total Matched Transactions (All Stages)": len(matched_total),
        "Total Matched Amount (Internal)": total_matched_amount_internal,
        "Total Matched Amount (Bank)": total_matched_amount_bank,
        "Unmatched Internal Records (Final)": len(final_unmatched_internal),
        "Unmatched Bank Records (Final)": len(final_unmatched_bank),
        "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
        "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
        "Currency": extracted_currency
    }
   
    # --- 7. Return the results ---
    return matched_total, final_unmatched_internal, final_unmatched_bank, summary


def reconcile_cellulant_tz(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Cellulant TZ with a two-pass approach:
    1. Initial match on Amount_Rounded + Date Proximity to resolve many-to-many.
    2. Second pass for remaining unmatches: simple one-to-one match on Amount_Rounded.
    Expects internal_file_obj (CSV) and bank_file_obj (CSV with header=5).
    Returns matched_total, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    try:
        cellulant_tz_hex_df = read_uploaded_file(internal_file_obj, header=0)
        cellulant_tz_bank_df = read_uploaded_file(bank_file_obj, header=5)
    except Exception as e:
        print(f"ERROR: Error reading files for Cellulant TZ: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    if cellulant_tz_hex_df is None or cellulant_tz_bank_df is None:
        print("ERROR: One or both files could not be loaded for Cellulant TZ.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # --- 1. Preprocessing for cellulant_tz_hex_df (Internal Records) ---
    cellulant_tz_hex_df.columns = cellulant_tz_hex_df.columns.astype(str).str.strip()

    internal_required_cols = ['TRANSFER_DATE', 'AMOUNT', 'COMMENT', 'TRANSFER_ID']
    if not all(col in cellulant_tz_hex_df.columns for col in internal_required_cols):
        missing_cols = [col for col in internal_required_cols if col not in cellulant_tz_hex_df.columns]
        print(f"ERROR: Internal records (Cellulant Hex) are missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    cellulant_tz_hex_df_processed = cellulant_tz_hex_df.rename(columns={
        'TRANSFER_DATE': 'Date', 'AMOUNT': 'Amount', 'COMMENT': 'Description', 'TRANSFER_ID': 'ID'
    }).copy()

    cellulant_tz_hex_df_processed['Date'] = pd.to_datetime(cellulant_tz_hex_df_processed['Date'], errors='coerce')
    cellulant_tz_hex_df_processed = cellulant_tz_hex_df_processed.dropna(subset=['Date']).copy()

    cellulant_tz_hex_df_processed['Amount'] = pd.to_numeric(cellulant_tz_hex_df_processed['Amount'], errors='coerce').fillna(0)
    cellulant_tz_hex_df_recon = cellulant_tz_hex_df_processed[cellulant_tz_hex_df_processed['Amount'] > 0].copy()

    cellulant_tz_hex_df_recon.loc[:, 'Amount_Rounded'] = cellulant_tz_hex_df_recon['Amount'].round(2)


    extracted_currency = "N/A"
    if 'CURRENCY' in cellulant_tz_hex_df.columns and not cellulant_tz_hex_df['CURRENCY'].empty:
        unique_currencies = cellulant_tz_hex_df['CURRENCY'].dropna().unique()
        if unique_currencies.size > 0:
            extracted_currency = str(unique_currencies[0])
        else:
            extracted_currency = "N/A (No currency in column)"
    else:
        extracted_currency = "N/A (CURRENCY column missing or empty)"


    # --- 2. Preprocessing for cellulant_tz_bank_df (Bank Statements) ---
    cellulant_tz_bank_df.columns = cellulant_tz_bank_df.columns.astype(str).str.strip()

    bank_required_cols = ['DateTime', 'Credit Amount', 'Transaction Type', 'Customer Float Transaction ID']
    if not all(col in cellulant_tz_bank_df.columns for col in bank_required_cols):
        missing_cols = [col for col in bank_required_cols if col not in cellulant_tz_bank_df.columns]
        print(f"ERROR: Bank statement (Cellulant) are missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    cellulant_tz_bank_df_processed = cellulant_tz_bank_df.rename(columns={
        'DateTime': 'Date', 'Credit Amount': 'Amount', 'Transaction Type': 'Description',
        'Customer Float Transaction ID': 'ID'
    }).copy()

    cellulant_tz_bank_df_processed['Date'] = pd.to_datetime(cellulant_tz_bank_df_processed['Date'], errors='coerce', infer_datetime_format=True)
    cellulant_tz_bank_df_processed = cellulant_tz_bank_df_processed.dropna(subset=['Date']).copy()
    cellulant_tz_bank_df_processed['Date'] = cellulant_tz_bank_df_processed['Date'].dt.tz_localize(None) # Remove timezone info

    cellulant_tz_bank_df_processed['Amount'] = (
        cellulant_tz_bank_df_processed['Amount'].astype(str)
        .str.replace('+', '', regex=False)
        .str.replace(',', '', regex=False)
        .str.strip()
    )
    cellulant_tz_bank_df_processed['Amount'] = pd.to_numeric(cellulant_tz_bank_df_processed['Amount'], errors='coerce').fillna(0)

    cellulant_tz_bank_df_recon = cellulant_tz_bank_df_processed[cellulant_tz_bank_df_processed['Amount'] > 0].copy()
    cellulant_tz_bank_df_recon.loc[:, 'Amount_Rounded'] = cellulant_tz_bank_df_recon['Amount'].round(2)


    # --- 3. Calculate Total Amounts and Discrepancy (before reconciliation) ---
    total_internal_credits_original = cellulant_tz_hex_df_recon['Amount'].sum()
    total_bank_credits_original = cellulant_tz_bank_df_recon['Amount'].sum()
    overall_discrepancy_original = total_internal_credits_original - total_bank_credits_original

    # --- 4. Reconciliation Pass 1: Amount Match + Date Proximity De-duplication ---

    # Initial merge on Amount_Rounded to find all potential matches
    potential_matches_df = pd.merge(
        cellulant_tz_hex_df_recon.assign(Source_Internal='Internal'),
        cellulant_tz_bank_df_recon.assign(Source_Bank='Bank'),
        on=['Amount_Rounded'],
        how='inner',
        suffixes=('_Internal', '_Bank')
    )

    matched_total_pass1 = pd.DataFrame()
    if not potential_matches_df.empty:
        potential_matches_df.loc[:, 'date_diff'] = abs(potential_matches_df['Date_Internal'] - potential_matches_df['Date_Bank'])
        potential_matches_df.loc[:, 'date_diff_days'] = potential_matches_df['date_diff'].dt.days

        potential_matches_df_sorted = potential_matches_df.sort_values(by=['date_diff_days', 'Amount_Rounded'], ascending=[True, False]).copy()

        # Deduplicate to ensure each internal and bank ID is matched only once in pass 1
        matched_total_pass1 = potential_matches_df_sorted.drop_duplicates(
            subset=['ID_Internal'], keep='first'
        ).drop_duplicates(
            subset=['ID_Bank'], keep='first'
        ).copy()

    # Determine unmatched records after Pass 1
    matched_internal_ids_pass1 = matched_total_pass1['ID_Internal'].unique() if not matched_total_pass1.empty else []
    matched_bank_ids_pass1 = matched_total_pass1['ID_Bank'].unique() if not matched_total_pass1.empty else []

    unmatched_internal_pass1 = cellulant_tz_hex_df_recon[
        ~cellulant_tz_hex_df_recon['ID'].isin(matched_internal_ids_pass1)
    ].copy()

    unmatched_bank_pass1 = cellulant_tz_bank_df_recon[
        ~cellulant_tz_bank_df_recon['ID'].isin(matched_bank_ids_pass1)
    ].copy()

    # --- 5. Reconciliation Pass 2: Simple One-to-One Match for Remaining Unmatches (by Amount_Rounded) ---
    matched_total_pass2 = pd.DataFrame()
    if not unmatched_internal_pass1.empty and not unmatched_bank_pass1.empty:
        # Create temporary IDs for one-to-one matching of duplicated amounts
        unmatched_internal_pass1.loc[:, 'temp_id_group'] = unmatched_internal_pass1.groupby('Amount_Rounded').cumcount()
        unmatched_bank_pass1.loc[:, 'temp_id_group'] = unmatched_bank_pass1.groupby('Amount_Rounded').cumcount()

        matched_total_pass2 = pd.merge(
            unmatched_internal_pass1,
            unmatched_bank_pass1,
            on=['Amount_Rounded', 'temp_id_group'],
            how='inner',
            suffixes=('_Internal', '_Bank')
        )
        # Drop the temporary ID column before concatenating
        matched_total_pass2 = matched_total_pass2.drop(columns=['temp_id_group']).copy()

    # Combine matches from both passes
    final_matched_total = pd.concat([matched_total_pass1, matched_total_pass2], ignore_index=True)

    # Determine final unmatched records
    final_matched_internal_ids = final_matched_total['ID_Internal'].unique() if not final_matched_total.empty else []
    final_matched_bank_ids = final_matched_total['ID_Bank'].unique() if not final_matched_total.empty else []

    final_unmatched_internal = cellulant_tz_hex_df_recon[
        ~cellulant_tz_hex_df_recon['ID'].isin(final_matched_internal_ids)
    ].rename(columns={
        'Date': 'Date_Internal', 'Amount': 'Amount_Internal', 'ID': 'ID_Internal'
    }).copy()

    final_unmatched_bank = cellulant_tz_bank_df_recon[
        ~cellulant_tz_bank_df_recon['ID'].isin(final_matched_bank_ids)
    ].rename(columns={
        'Date': 'Date_Bank', 'Amount': 'Amount_Bank', 'ID': 'ID_Bank'
    }).copy()


    # --- 6. Summary of Reconciliation ---
    total_matched_amount_internal = final_matched_total['Amount_Internal'].sum() if 'Amount_Internal' in final_matched_total.columns else 0
    total_matched_amount_bank = final_matched_total['Amount_Bank'].sum() if 'Amount_Bank' in final_matched_total.columns else 0

    remaining_unmatched_internal_amount = final_unmatched_internal['Amount_Internal'].sum() if 'Amount_Internal' in final_unmatched_internal.columns else 0
    remaining_unmatched_bank_amount = final_unmatched_bank['Amount_Bank'].sum() if 'Amount_Bank' in final_unmatched_bank.columns else 0

    summary = {
        "Total Internal Records (for recon)": len(cellulant_tz_hex_df_recon),
        "Total Bank Statement Records (for recon)": len(cellulant_tz_bank_df_recon),
        "Total Internal Credits (Original)": total_internal_credits_original,
        "Total Bank Credits (Original)": total_bank_credits_original,
        "Overall Discrepancy (Original)": overall_discrepancy_original,
        "Total Matched Transactions (All Stages)": len(final_matched_total),
        "Total Matched Amount (Internal)": total_matched_amount_internal,
        "Total Matched Amount (Bank)": total_matched_amount_bank,
        "Unmatched Internal Records (Final)": len(final_unmatched_internal),
        "Unmatched Bank Records (Final)": len(final_unmatched_bank),
        "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
        "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
        "Currency": extracted_currency
    }

    # --- 7. Return the results ---
    return final_matched_total, final_unmatched_internal, final_unmatched_bank, summary

 
def reconcile_flutterwave_ug(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Flutterwave Uganda.
    Includes exact match, date tolerance, and a second pass for one-to-one amount matches.
    Expects internal_file_obj (CSV) and bank_file_obj (CSV, header=0).
    Returns matched_total, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    try:
        # Bank file header is 0 as per ipynb code
        flutterwave_hex_df = read_uploaded_file(internal_file_obj, header=0)
        flutterwave_bank_df = read_uploaded_file(bank_file_obj, header=0)
    except Exception as e:
        st.error(f"Error reading files for Flutterwave UG: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    if flutterwave_hex_df is None or flutterwave_bank_df is None:
        st.error("One or both files could not be loaded for Flutterwave UG.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # --- 1. Preprocessing for flutterwave_hex_df (Internal Records) ---
    flutterwave_hex_df.columns = flutterwave_hex_df.columns.astype(str).str.strip()

    internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
    if not all(col in flutterwave_hex_df.columns for col in internal_required_cols):
        missing_cols = [col for col in internal_required_cols if col not in flutterwave_hex_df.columns]
        st.error(f"Internal records (Flutterwave Hex) are missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    flutterwave_hex_df_processed = flutterwave_hex_df.rename(columns={
        'TRANSFER_DATE': 'Date', 'AMOUNT': 'Amount'
    }).copy()
    
    flutterwave_hex_df_processed['Date'] = pd.to_datetime(flutterwave_hex_df_processed['Date'], errors='coerce')
    # Convert to tz-naive if it's tz-aware, to prevent TypeError in date comparisons
    if pd.api.types.is_datetime64tz_dtype(flutterwave_hex_df_processed['Date']):
        flutterwave_hex_df_processed['Date'] = flutterwave_hex_df_processed['Date'].dt.tz_localize(None)
    flutterwave_hex_df_processed = flutterwave_hex_df_processed.dropna(subset=['Date']).copy()
    
    flutterwave_hex_df_processed['Amount'] = pd.to_numeric(flutterwave_hex_df_processed['Amount'], errors='coerce').fillna(0)
    flutterwave_hex_df_recon = flutterwave_hex_df_processed[flutterwave_hex_df_processed['Amount'] > 0].copy()

    # Add a unique ID for reconciliation if 'TRANSFER_ID' is not consistently available
    if 'TRANSFER_ID' in flutterwave_hex_df_recon.columns:
        flutterwave_hex_df_recon.loc[:, 'ID'] = flutterwave_hex_df_recon['TRANSFER_ID']
    else:
        flutterwave_hex_df_recon.loc[:, 'ID'] = 'Internal_' + flutterwave_hex_df_recon.index.astype(str)

    flutterwave_hex_df_recon.loc[:, 'Date_Match'] = flutterwave_hex_df_recon['Date'].dt.date
    flutterwave_hex_df_recon.loc[:, 'Amount_Rounded'] = flutterwave_hex_df_recon['Amount'].round(2)

    extracted_currency = "N/A"
    if 'CURRENCY' in flutterwave_hex_df.columns and not flutterwave_hex_df['CURRENCY'].empty:
        unique_currencies = flutterwave_hex_df['CURRENCY'].dropna().unique()
        if unique_currencies.size > 0:
            extracted_currency = str(unique_currencies[0])
        else:
            extracted_currency = "N/A (No currency in column)"
    else:
        extracted_currency = "N/A (CURRENCY column missing or empty)"


    # --- 2. Preprocessing for flutterwave_bank_df (Bank Statements) ---
    flutterwave_bank_df.columns = flutterwave_bank_df.columns.astype(str).str.strip()

    # Dynamically find and rename 'Date' column
    date_col_bank = find_column(flutterwave_bank_df, ['date', 'value date', 'transaction date'])
    if date_col_bank:
        flutterwave_bank_df = flutterwave_bank_df.rename(columns={date_col_bank: 'Date'})
        flutterwave_bank_df['Date'] = pd.to_datetime(flutterwave_bank_df['Date'], dayfirst=True, errors='coerce')
        # Convert to tz-naive if it's tz-aware, to prevent TypeError in date comparisons
        if pd.api.types.is_datetime64tz_dtype(flutterwave_bank_df['Date']):
            flutterwave_bank_df['Date'] = flutterwave_bank_df['Date'].dt.tz_localize(None)
        flutterwave_bank_df = flutterwave_bank_df.dropna(subset=['Date']).copy()
    else:
        st.error("Bank statement (Flutterwave) missing 'Date' column.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # Dynamically find and rename 'Amount' column
    amount_col_bank = find_column(flutterwave_bank_df, ['amount', 'credit'])
    if amount_col_bank:
        flutterwave_bank_df = flutterwave_bank_df.rename(columns={amount_col_bank: 'Amount'})
        flutterwave_bank_df['Amount'] = flutterwave_bank_df['Amount'].astype(str).str.replace(',', '', regex=False).astype(float)
        flutterwave_bank_df['Amount'] = flutterwave_bank_df['Amount'].fillna(0)
    else:
        st.error("Bank statement (Flutterwave) missing 'Amount' (or 'Credit') column.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # Dynamically find and rename 'Type' column
    type_col_bank = find_column(flutterwave_bank_df, ['type'])
    if type_col_bank:
        flutterwave_bank_df = flutterwave_bank_df.rename(columns={type_col_bank: 'Type'})
        # Filter for 'Type' = 'C' (Credits)
        flutterwave_bank_df = flutterwave_bank_df[flutterwave_bank_df['Type'].astype(str).str.upper() == 'C'].copy()
    else:
        st.warning("Warning: 'Type' column not found in bank statement for Flutterwave. Skipping 'type' filtering.")

    # Dynamically find and rename 'Remarks' column
    remarks_col_bank = find_column(flutterwave_bank_df, ['remarks', 'narration'])
    if remarks_col_bank:
        flutterwave_bank_df = flutterwave_bank_df.rename(columns={remarks_col_bank: 'Remarks'})
        # Filter out records with 'rvsl' in 'Remarks'
        flutterwave_bank_df = flutterwave_bank_df[~flutterwave_bank_df['Remarks'].astype(str).str.contains('rvsl', case=False, na=False)].copy()
    else:
        st.warning("Warning: 'Remarks' column not found in bank statement for Flutterwave. Skipping 'rvsl' filtering.")

    # Filter bank records to include only positive amounts (credits) after all other filters
    flutterwave_bank_df_recon = flutterwave_bank_df[flutterwave_bank_df['Amount'] > 0].copy()

    # Add a unique ID for reconciliation for bank records if no specific ID column is found
    if 'Customer Float Transaction ID' in flutterwave_bank_df_recon.columns: # Common from previous Cellulant files
        flutterwave_bank_df_recon.loc[:, 'ID'] = flutterwave_bank_df_recon['Customer Float Transaction ID']
    elif 'Reference' in flutterwave_bank_df_recon.columns: # Another common one
        flutterwave_bank_df_recon.loc[:, 'ID'] = flutterwave_bank_df_recon['Reference']
    else:
        flutterwave_bank_df_recon.loc[:, 'ID'] = 'Bank_' + flutterwave_bank_df_recon.index.astype(str)

    flutterwave_bank_df_recon.loc[:, 'Date_Match'] = flutterwave_bank_df_recon['Date'].dt.date
    flutterwave_bank_df_recon.loc[:, 'Amount_Rounded'] = flutterwave_bank_df_recon['Amount'].round(2)


    # --- 3. Calculate Total Amounts and Discrepancy (before reconciliation) ---
    total_internal_credits_original = flutterwave_hex_df_recon['Amount'].sum()
    total_bank_credits_original = flutterwave_bank_df_recon['Amount'].sum()
    overall_discrepancy_original = total_internal_credits_original - total_bank_credits_original


    # --- 4. Reconciliation Pass 1: Exact Match (Date_Match + Amount_Rounded) ---
    reconciled_df_exact = pd.merge(
        flutterwave_hex_df_recon.assign(Source_Internal='Internal'),
        flutterwave_bank_df_recon.assign(Source_Bank='Bank'),
        on=['Date_Match', 'Amount_Rounded'],
        how='outer',
        suffixes=('_Internal', '_Bank')
    )

    matched_total_pass1 = reconciled_df_exact.dropna(subset=['Source_Internal', 'Source_Bank']).copy()

    # Prepare unmatched records for Pass 2 (Date Tolerance)
    unmatched_internal_pass1 = reconciled_df_exact[reconciled_df_exact['Source_Bank'].isna()].copy()
    if not unmatched_internal_pass1.empty:
        unmatched_internal_for_tolerance = unmatched_internal_pass1[[
            'Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded'
        ]].rename(columns={
            'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
        }).copy()
    else:
        unmatched_internal_for_tolerance = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])


    unmatched_bank_pass1 = reconciled_df_exact[reconciled_df_exact['Source_Internal'].isna()].copy()
    if not unmatched_bank_pass1.empty:
        unmatched_bank_for_tolerance = unmatched_bank_pass1[[
            'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
        ]].rename(columns={
            'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
        }).copy()
    else:
        unmatched_bank_for_tolerance = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])


    # --- 5. Reconciliation Pass 2: Date Tolerance Matching (using perform_date_tolerance_matching) ---
    matched_tolerance_pass2 = pd.DataFrame()
    remaining_internal_after_tolerance = unmatched_internal_for_tolerance.copy()
    remaining_bank_after_tolerance = unmatched_bank_for_tolerance.copy()

    if not unmatched_internal_for_tolerance.empty and not unmatched_bank_for_tolerance.empty:
        st.info("Attempting date tolerance matching for remaining unmatched records (Flutterwave UG)...")
        # Dates passed to perform_date_tolerance_matching are now guaranteed tz-naive by preprocessing
        matched_tolerance_pass2, remaining_internal_after_tolerance, remaining_bank_after_tolerance = \
            perform_date_tolerance_matching(
                unmatched_internal_for_tolerance,
                unmatched_bank_for_tolerance,
                tolerance_days=3 # Default 3 days tolerance
            )


    # --- 6. Reconciliation Pass 3: Simple One-to-One Match for Residuals (by Amount_Rounded) ---
    matched_total_pass3 = pd.DataFrame()
    if not remaining_internal_after_tolerance.empty and not remaining_bank_after_tolerance.empty:
        # Create temporary IDs for one-to-one matching of duplicated amounts
        remaining_internal_after_tolerance.loc[:, 'temp_id_group'] = remaining_internal_after_tolerance.groupby('Amount_Rounded').cumcount()
        remaining_bank_after_tolerance.loc[:, 'temp_id_group'] = remaining_bank_after_tolerance.groupby('Amount_Rounded').cumcount()

        matched_total_pass3 = pd.merge(
            remaining_internal_after_tolerance,
            remaining_bank_after_tolerance,
            on=['Amount_Rounded', 'temp_id_group'],
            how='inner',
            suffixes=('_Internal', '_Bank')
        )
        matched_total_pass3 = matched_total_pass3.drop(columns=['temp_id_group']).copy()


    # --- 7. Combine all matched transactions from all passes ---
    all_matched_dfs = [matched_total_pass1, matched_tolerance_pass2, matched_total_pass3]
    final_matched_total = pd.concat([df for df in all_matched_dfs if not df.empty], ignore_index=True)  

    # --- 8. Determine final unmatched records ---
    final_matched_internal_ids = final_matched_total['ID_Internal'].unique() if not final_matched_total.empty else []
    final_matched_bank_ids = final_matched_total['ID_Bank'].unique() if not final_matched_total.empty else []

    final_unmatched_internal = flutterwave_hex_df_recon[
        ~flutterwave_hex_df_recon['ID'].isin(final_matched_internal_ids)
    ].rename(columns={
        'Date': 'Date_Internal', 'Amount': 'Amount_Internal', 'ID': 'ID_Internal'
    }).copy()

    final_unmatched_bank = flutterwave_bank_df_recon[
        ~flutterwave_bank_df_recon['ID'].isin(final_matched_bank_ids)
    ].rename(columns={
        'Date': 'Date_Bank', 'Amount': 'Amount_Bank', 'ID': 'ID_Bank'
    }).copy()

    # --- 9. Summary of Reconciliation ---
    total_matched_amount_internal = final_matched_total['Amount_Internal'].sum() if 'Amount_Internal' in final_matched_total.columns else 0
    total_matched_amount_bank = final_matched_total['Amount_Bank'].sum() if 'Amount_Bank' in final_matched_total.columns else 0

    remaining_unmatched_internal_amount = final_unmatched_internal['Amount_Internal'].sum() if 'Amount_Internal' in final_unmatched_internal.columns else 0
    remaining_unmatched_bank_amount = final_unmatched_bank['Amount_Bank'].sum() if 'Amount_Bank' in final_unmatched_bank.columns else 0

    summary = {
        "Total Internal Records (for recon)": len(flutterwave_hex_df_recon),
        "Total Bank Statement Records (for recon)": len(flutterwave_bank_df_recon),
        "Total Internal Credits (Original)": total_internal_credits_original,
        "Total Bank Credits (Original)": total_bank_credits_original,
        "Overall Discrepancy (Original)": overall_discrepancy_original,
        "Total Matched Transactions (All Stages)": len(final_matched_total),
        "Total Matched Amount (Internal)": total_matched_amount_internal,
        "Total Matched Amount (Bank)": total_matched_amount_bank,
        "Unmatched Internal Records (Final)": len(final_unmatched_internal),
        "Unmatched Bank Records (Final)": len(final_unmatched_bank),
        "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
        "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
        "Currency": extracted_currency
    }

    # --- 10. Return the results ---
    return final_matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_cellulant_ngn(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Cellulant Nigeria (NGN).
    Includes exact match, date tolerance matching (up to 3 days), and Nigeria-specific filters.
    Expects internal_file_obj (CSV) and bank_file_obj (Excel with header=5).
    Returns matched_total, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    try:
        # --- 1. Load the datasets ---
        cellulant_hex_df = read_uploaded_file(internal_file_obj, header=0)
        # Try both Excel engines for bank file
        try:
            cellulant_bank_df = read_uploaded_file(bank_file_obj, header=5)
        except Exception as e:
            st.warning(f"First Excel engine failed, trying alternative: {str(e)}")
            cellulant_bank_df = read_uploaded_file(bank_file_obj, header=5)
            
    except Exception as e:
        st.error(f"Error reading files for Cellulant NGN: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    if cellulant_hex_df is None or cellulant_bank_df is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # --- 2. Preprocessing for cellulant_hex_df (Internal Records) ---
    cellulant_hex_df.columns = cellulant_hex_df.columns.astype(str).str.strip()

    # Essential columns for internal records
    internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
    if not all(col in cellulant_hex_df.columns for col in internal_required_cols):
        missing_cols = [col for col in internal_required_cols if col not in cellulant_hex_df.columns]
        st.error(f"Internal records (Cellulant Hex) are missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    cellulant_hex_df = cellulant_hex_df.rename(columns={
        'TRANSFER_DATE': 'Date', 
        'AMOUNT': 'Amount',
        'COMMENT': 'Description',
        'TRANSFER_ID': 'ID'
    })

    # Convert 'Date' to datetime objects
    cellulant_hex_df['Date'] = pd.to_datetime(cellulant_hex_df['Date'], errors='coerce')
    cellulant_hex_df = cellulant_hex_df.dropna(subset=['Date']).copy()

    # Filter internal records to include only positive amounts (credits/deposits)
    cellulant_hex_df_recon = cellulant_hex_df[cellulant_hex_df['Amount'] > 0].copy()
    cellulant_hex_df_recon.loc[:, 'Date_Match'] = cellulant_hex_df_recon['Date'].dt.date

    # --- Extract currency from internal_df ---
    extracted_currency = "NGN"  # Default for Nigeria
    if 'CURRENCY' in cellulant_hex_df.columns and not cellulant_hex_df['CURRENCY'].empty:
        unique_currencies = cellulant_hex_df['CURRENCY'].dropna().unique()
        if unique_currencies.size > 0:
            extracted_currency = str(unique_currencies[0])

    # --- 3. Preprocessing for cellulant_bank_df (Bank Statements) ---
    cellulant_bank_df.columns = cellulant_bank_df.columns.astype(str).str.strip()

    # Essential columns for bank statements
    bank_required_cols = ['DateTime', 'Credit Amount', 'Transaction Type']
    if not all(col in cellulant_bank_df.columns for col in bank_required_cols):
        missing_cols = [col for col in bank_required_cols if col not in cellulant_bank_df.columns]
        st.error(f"Bank statement (Cellulant) are missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    cellulant_bank_df = cellulant_bank_df.rename(columns={
        'DateTime': 'Date',
        'Credit Amount': 'Credit',
        'Transaction Type': 'Transaction_Type',
        'Customer Float Transaction ID': 'ID'
    })

    # Convert 'Date' to datetime objects (handle format like '5/30/25, 3:21 PM GMT+3')
    cellulant_bank_df['Date'] = pd.to_datetime(cellulant_bank_df['Date'], infer_datetime_format=True, errors='coerce')
    
    # Remove timezone information if present
    if pd.api.types.is_datetime64tz_dtype(cellulant_bank_df['Date']):
        cellulant_bank_df['Date'] = cellulant_bank_df['Date'].dt.tz_localize(None)
    
    cellulant_bank_df = cellulant_bank_df.dropna(subset=['Date']).copy()
    cellulant_bank_df.loc[:, 'Date_Match'] = cellulant_bank_df['Date'].dt.date

    # --- Nigeria Specific Filters ---
    # Filter for Transaction Type = 'allocate' or 'revoke'
    if 'Transaction_Type' in cellulant_bank_df.columns:
        cellulant_bank_df = cellulant_bank_df[
            cellulant_bank_df['Transaction_Type'].isin(['allocate', 'revoke'])
        ].copy()

    # Filter for Transaction ID = 1 (if this column exists)
    if 'Transaction ID' in cellulant_bank_df.columns:
        cellulant_bank_df = cellulant_bank_df[
            cellulant_bank_df['Transaction ID'] == 1
        ].copy()

    # Process Credit Amount - remove '+' and ',' then convert to numeric
    cellulant_bank_df['Credit'] = (
        cellulant_bank_df['Credit'].astype(str)
        .str.replace('+', '', regex=False)
        .str.replace(',', '', regex=False)
        .astype(float)
    )
    cellulant_bank_df['Credit'] = cellulant_bank_df['Credit'].fillna(0)

    # For reconciliation, consider only credit values from the bank statements
    cellulant_bank_df['Amount'] = cellulant_bank_df['Credit']
    cellulant_bank_df_recon = cellulant_bank_df[cellulant_bank_df['Amount'] > 0].copy()

    # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
    total_internal_credits = cellulant_hex_df_recon['Amount'].sum()
    total_bank_credits = cellulant_bank_df_recon['Amount'].sum()
    discrepancy_amount = total_internal_credits - total_bank_credits

    # --- 5. Reconciliation (Exact Match) ---
    cellulant_hex_df_recon.loc[:, 'Amount_Rounded'] = cellulant_hex_df_recon['Amount'].round(2)
    cellulant_bank_df_recon.loc[:, 'Amount_Rounded'] = cellulant_bank_df_recon['Amount'].round(2)

    reconciled_df_exact = pd.merge(
        cellulant_hex_df_recon.assign(Source_Internal='Internal'),
        cellulant_bank_df_recon.assign(Source_Bank='Bank'),
        on=['Date_Match', 'Amount_Rounded'],
        how='outer',
        suffixes=('_Internal', '_Bank')
    )

    matched_exact = reconciled_df_exact.dropna(subset=['Source_Internal', 'Source_Bank']).copy()

    # Prepare initially unmatched internal transactions for the next stage (Date Tolerance)
    unmatched_internal_for_tolerance = reconciled_df_exact[reconciled_df_exact['Source_Bank'].isna()].copy()
    if not unmatched_internal_for_tolerance.empty:
        unmatched_internal_for_tolerance = unmatched_internal_for_tolerance[[
            'Date_Match', 'Amount_Internal', 'Amount_Rounded', 'Source_Internal', 'ID_Internal'
        ]].rename(columns={
            'Date_Match': 'Date', 'Amount_Internal': 'Amount', 'Source_Internal': 'Source', 'ID_Internal': 'ID'
        }).copy()
        unmatched_internal_for_tolerance['Date'] = pd.to_datetime(unmatched_internal_for_tolerance['Date'])
    else:
        unmatched_internal_for_tolerance = pd.DataFrame(columns=['Date', 'Amount', 'Amount_Rounded', 'Source', 'ID'])
        unmatched_internal_for_tolerance['Date'] = pd.to_datetime(unmatched_internal_for_tolerance['Date'])

    # Prepare initially unmatched bank transactions for the next stage (Date Tolerance)
    unmatched_bank_for_tolerance = reconciled_df_exact[reconciled_df_exact['Source_Internal'].isna()].copy()
    if not unmatched_bank_for_tolerance.empty:
        unmatched_bank_for_tolerance = unmatched_bank_for_tolerance[[
            'Date_Match', 'Amount_Bank', 'Amount_Rounded', 'Source_Bank', 'ID_Bank'
        ]].rename(columns={
            'Date_Match': 'Date', 'Amount_Bank': 'Amount', 'Source_Bank': 'Source', 'ID_Bank': 'ID'
        }).copy()
        unmatched_bank_for_tolerance['Date'] = pd.to_datetime(unmatched_bank_for_tolerance['Date'])
    else:
        unmatched_bank_for_tolerance = pd.DataFrame(columns=['Date', 'Amount', 'Amount_Rounded', 'Source', 'ID'])
        unmatched_bank_for_tolerance['Date'] = pd.to_datetime(unmatched_bank_for_tolerance['Date'])

    # --- 6. Reconciliation with Date Tolerance (3 days) using perform_date_tolerance_matching ---
    matched_with_tolerance = pd.DataFrame()
    remaining_internal_after_tolerance = unmatched_internal_for_tolerance.copy()
    remaining_bank_after_tolerance = unmatched_bank_for_tolerance.copy()

    if not unmatched_internal_for_tolerance.empty and not unmatched_bank_for_tolerance.empty:
        st.info("Attempting date tolerance matching for remaining unmatched records (Cellulant NGN)...")
        matched_with_tolerance, remaining_internal_after_tolerance, remaining_bank_after_tolerance = \
            perform_date_tolerance_matching(
                unmatched_internal_for_tolerance,
                unmatched_bank_for_tolerance,
                tolerance_days=3
            )

    # --- 7. Final Summary of Reconciliation ---
    # Combine all matched dataframes for total counts and amounts
    all_matched_dfs = [matched_exact, matched_with_tolerance]
    all_matched_dfs = [df for df in all_matched_dfs if not df.empty]  # Filter out empty DFs

    if all_matched_dfs:
        matched_total = pd.concat(all_matched_dfs, ignore_index=True)
    else:
        matched_total = pd.DataFrame(columns=[
            'Date_Internal', 'Amount_Internal', 'Date_Match_Internal', 'Source_Internal', 'ID_Internal',
            'Date_Bank', 'Amount_Bank', 'Date_Match_Bank', 'Source_Bank', 'ID_Bank', 'Amount_Rounded'
        ])

    total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
    total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0

    remaining_unmatched_internal_amount = remaining_internal_after_tolerance['Amount'].sum() if 'Amount' in remaining_internal_after_tolerance.columns else 0
    remaining_unmatched_bank_amount = remaining_bank_after_tolerance['Amount'].sum() if 'Amount' in remaining_bank_after_tolerance.columns else 0

    summary = {
        "Total Internal Records (for recon)": len(cellulant_hex_df_recon),
        "Total Bank Statement Records (for recon)": len(cellulant_bank_df_recon),
        "Total Internal Credits (Original)": total_internal_credits,
        "Total Bank Credits (Original)": total_bank_credits,
        "Overall Discrepancy (Original)": discrepancy_amount,
        "Total Matched Transactions (All Stages)": len(matched_total),
        "Total Matched Amount (Internal)": total_matched_amount_internal,
        "Total Matched Amount (Bank)": total_matched_amount_bank,
        "Unmatched Internal Records (Final)": len(remaining_internal_after_tolerance),
        "Unmatched Bank Records (Final)": len(remaining_bank_after_tolerance),
        "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
        "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
        "Currency": extracted_currency
    }

    # --- 8. Return the results ---
    return matched_total, remaining_internal_after_tolerance, remaining_bank_after_tolerance, summary

def reconcile_verto(internal_file_obj, bank_file_obj, recon_month=None, recon_year=None):
    """
    Performs reconciliation for Verto Nigeria (NGN) with:
    1. Exact matching (same date + amount)
    2. Date tolerance matching (±3 days)
    3. Same-day aggregation matching
    """
    try:
        # Initialize empty DataFrames with proper columns
        empty_df = pd.DataFrame(columns=[
            'Date_Internal', 'Amount_Internal', 'Date_Bank', 'Amount_Bank', 'Date_Diff'
        ])
        empty_unmatched = pd.DataFrame(columns=['Date', 'Amount'])
        
        # --- 1. Load datasets with multiple header attempts ---
        verto_hex_df = read_uploaded_file(internal_file_obj, header=0)
        
        # Try multiple header rows for bank file
        for header_row in [8, 0]:  # Try header=8 first, then header=0
            verto_bank_df = read_uploaded_file(bank_file_obj, header=header_row)
            if 'Date' in verto_bank_df.columns and 'Credit' in verto_bank_df.columns:
                st.success(f"Bank file loaded successfully with header={header_row}")
                break
        else:
            st.error("Could not find required columns ('Date', 'Credit') in bank file")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

        # --- Debug: Show raw data preview ---
        with st.expander("Raw Data Preview"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("Internal Records (first 5):")
                st.write(verto_hex_df.head())
            with col2:
                st.write("Bank Records (first 5):")
                st.write(verto_bank_df.head())

        # --- 2. Preprocess internal records ---
        verto_hex_df.columns = verto_hex_df.columns.str.strip()
        verto_hex_df = verto_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Date parsing with multiple format attempts
        verto_hex_df['Date'] = pd.to_datetime(verto_hex_df['Date'], errors='coerce')
        verto_hex_df = verto_hex_df.dropna(subset=['Date'])
        
        # Filter positive amounts
        verto_hex_df_recon = verto_hex_df[verto_hex_df['Amount'] > 0].copy()
        verto_hex_df_recon['Date_Match'] = verto_hex_df_recon['Date'].dt.date
        verto_hex_df_recon['Amount_Rounded'] = verto_hex_df_recon['Amount'].round(2)

        # --- 3. Preprocess bank records ---
        verto_bank_df.columns = verto_bank_df.columns.str.strip()
        verto_bank_df = verto_bank_df.rename(columns={
            'Verto Transaction Id': 'Transaction_ID',
            'Comment': 'Description'
        })

        # Robust date parsing for Nigerian format (day/month/year)
        verto_bank_df['Date'] = pd.to_datetime(verto_bank_df['Date'], dayfirst=True, errors='coerce')
        verto_bank_df = verto_bank_df.dropna(subset=['Date'])

        # --- Month/Year Filter ---
        if recon_month is None:
            recon_month = datetime.datetime.now().month
        if recon_year is None:
            recon_year = datetime.datetime.now().year
            
        verto_bank_df = verto_bank_df[
            (verto_bank_df['Date'].dt.month == recon_month) & 
            (verto_bank_df['Date'].dt.year == recon_year)
        ].copy()

        # Clean credit amounts
        verto_bank_df['Credit'] = (
            verto_bank_df['Credit'].astype(str)
            .str.replace('[^\d.]', '', regex=True)  # Remove all non-numeric
            .replace('', '0')
            .astype(float)
        )
        
        # Filter positive credits
        verto_bank_df = verto_bank_df[verto_bank_df['Credit'] > 0].copy()
        verto_bank_df['Amount'] = verto_bank_df['Credit']
        verto_bank_df_recon = verto_bank_df[['Date', 'Amount', 'Description', 'Transaction_ID']].copy()
        verto_bank_df_recon['Date_Match'] = verto_bank_df_recon['Date'].dt.date
        verto_bank_df_recon['Amount_Rounded'] = verto_bank_df_recon['Amount'].round(2)

        # --- Debug: Show processed data ---
        with st.expander("Processed Data Preview"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("Internal Records for Recon:")
                st.write(verto_hex_df_recon.head())
                st.write(f"Total: {len(verto_hex_df_recon)} records, {verto_hex_df_recon['Amount'].sum():,.2f} NGN")
            with col2:
                st.write("Bank Records for Recon:")
                st.write(verto_bank_df_recon.head())
                st.write(f"Total: {len(verto_bank_df_recon)} records, {verto_bank_df_recon['Amount'].sum():,.2f} NGN")
        
        # --- 2. Initial Exact Matching ---
        reconciled_df = pd.merge(
            verto_hex_df_recon.assign(Source_Internal='Internal'),
            verto_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        unmatched_internal = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        unmatched_bank = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()

        # --- 3. Date Tolerance Matching (±3 days) ---
        unmatched_internal['Date_DT'] = pd.to_datetime(unmatched_internal['Date_Match'])
        unmatched_bank['Date_DT'] = pd.to_datetime(unmatched_bank['Date_Match'])

        matched_tolerance = []
        for _, int_row in unmatched_internal.iterrows():
            matches = unmatched_bank[
                (unmatched_bank['Amount_Rounded'] == int_row['Amount_Rounded']) &
                (abs(unmatched_bank['Date_DT'] - int_row['Date_DT']) <= pd.Timedelta(days=3))
            ]
            if not matches.empty:
                bank_match = matches.iloc[0]
                matched_tolerance.append({
                    # Convert datetime.date to pd.Timestamp for consistency
                    'Date_Internal': pd.to_datetime(int_row['Date_Internal']),
                    'Amount_Internal': int_row['Amount_Internal'],
                    'Date_Bank': pd.to_datetime(bank_match['Date_Bank']),
                    'Amount_Bank': bank_match['Amount_Bank'],
                    'Date_Diff': (bank_match['Date_DT'] - int_row['Date_DT']).days
                })
                # Remove matched records
                unmatched_internal = unmatched_internal.drop(int_row.name)
                unmatched_bank = unmatched_bank.drop(bank_match.name)

        matched_tolerance_df = pd.DataFrame(matched_tolerance)

        # --- 4. Same-Day Aggregation Matching ---
        matched_aggregated = []
        if not unmatched_internal.empty:
            # Group internal transactions by date
            internal_aggregated = (
                unmatched_internal.groupby('Date_Match')
                .agg({
                    'Amount_Internal': 'sum',
                    'Amount_Rounded': 'sum'
                })
                .reset_index()
            )
            internal_aggregated['Amount_Rounded'] = internal_aggregated['Amount_Internal'].round(2)
            internal_aggregated['Date_DT'] = pd.to_datetime(internal_aggregated['Date_Match'])

            # Try matching aggregated amounts with bank records
            bank_unmatched = unmatched_bank.copy()
            bank_unmatched['Date_DT'] = pd.to_datetime(bank_unmatched['Date_Match'])

            for _, agg_row in internal_aggregated.iterrows():
                matches = bank_unmatched[
                    (bank_unmatched['Amount_Rounded'] == agg_row['Amount_Rounded']) &
                    (bank_unmatched['Date_DT'] == agg_row['Date_DT'])
                ]
                if not matches.empty:
                    bank_match = matches.iloc[0]
                    matched_aggregated.append({
                        # Convert datetime.date to pd.Timestamp for consistency
                        'Date_Internal': pd.to_datetime(agg_row['Date_Match']),
                        'Amount_Internal': agg_row['Amount_Internal'],
                        'Date_Bank': pd.to_datetime(bank_match['Date_Match']),
                        'Amount_Bank': bank_match['Amount_Bank'],
                        'Date_Diff': 0  # Same-day match
                    })
                    # Remove matched bank record
                    unmatched_bank = unmatched_bank.drop(bank_match.name)

            # Remove matched dates from internal unmatched
            if matched_aggregated:
                matched_dates = {m['Date_Internal'].date() for m in matched_aggregated} # Convert back to date for comparison
                unmatched_internal = unmatched_internal[
                    ~unmatched_internal['Date_Match'].isin(matched_dates)
                ]

        matched_aggregated_df = pd.DataFrame(matched_aggregated)

        # --- 5. Combine all matches ---
        matched_final = pd.concat([
            matched_exact[['Date_Internal', 'Amount_Internal', 'Date_Bank', 'Amount_Bank']],
            matched_tolerance_df,
            matched_aggregated_df
        ], ignore_index=True)

        # IMPORTANT: Ensure date columns are datetime64[ns] for Streamlit compatibility
        matched_final['Date_Internal'] = pd.to_datetime(matched_final['Date_Internal'])
        matched_final['Date_Bank'] = pd.to_datetime(matched_final['Date_Bank'])

        # Add 0 days diff for exact matches if not already present
        if 'Date_Diff' not in matched_final.columns:
            matched_final['Date_Diff'] = 0

        # --- 6. Prepare final unmatched records ---
        final_unmatched_internal = unmatched_internal[['Date_Match', 'Amount_Internal']].rename(
            columns={'Date_Match': 'Date', 'Amount_Internal': 'Amount'}
        ) if not unmatched_internal.empty else empty_unmatched.copy()

        final_unmatched_bank = unmatched_bank[['Date_Match', 'Amount_Bank']].rename(
            columns={'Date_Match': 'Date', 'Amount_Bank': 'Amount'}
        ) if not unmatched_bank.empty else empty_unmatched.copy()

        # IMPORTANT: Ensure date columns in unmatched are also datetime64[ns]
        final_unmatched_internal['Date'] = pd.to_datetime(final_unmatched_internal['Date'])
        final_unmatched_bank['Date'] = pd.to_datetime(final_unmatched_bank['Date'])


        # --- 7. Generate Summary ---
        total_matched = len(matched_final)
        total_internal = len(verto_hex_df_recon)
        accuracy = (total_matched / total_internal * 100) if total_internal > 0 else 0

        summary = {
            "Provider name": "Verto",
            "Currency": "NGN",
            "Month & Year": f"{recon_month}/{recon_year}",
            "# of Transactions": total_matched,
            "Partner Statement": verto_bank_df_recon['Amount'].sum(),
            "Treasury Records": verto_hex_df_recon['Amount'].sum(),
            "Variance": verto_hex_df_recon['Amount'].sum() - verto_bank_df_recon['Amount'].sum(),
            "% accuracy": f"{accuracy:.2f}%",
            "Status": "Matched" if final_unmatched_internal.empty and final_unmatched_bank.empty else "Partial",
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Comments": "",
            "Matching Breakdown": {
                "Exact Matches": len(matched_exact),
                "Tolerance Matches": len(matched_tolerance_df),
                "Aggregated Matches": len(matched_aggregated_df)
            }
        }
        return matched_final, final_unmatched_internal, final_unmatched_bank, summary

    except Exception as e:
        st.error(f"Reconciliation error: {str(e)}")
        return empty_df, empty_unmatched, empty_unmatched, {}
    
def reconcile_fincra(internal_file_obj, bank_file_obj, recon_month=None, recon_year=None):
    """
    Performs reconciliation for Fincra Nigeria (NGN) with:
    1. Exact matching (same date + amount)
    2. Date tolerance matching (±3 days)
    """
    try:
        # Initialize empty DataFrames with proper columns
        empty_df = pd.DataFrame(columns=[
            'Date_Internal', 'Amount_Internal', 'Date_Bank', 'Amount_Bank', 'Date_Diff'
        ])
        empty_unmatched = pd.DataFrame(columns=['Date', 'Amount'])
        
        # --- 1. Load datasets ---
        fincra_hex_df = read_uploaded_file(internal_file_obj, header=0)
        fincra_bank_df = read_uploaded_file(bank_file_obj, header=0)

        # Check if essential columns exist in bank file
        if 'Date Initiated' not in fincra_bank_df.columns or 'Amount Received' not in fincra_bank_df.columns:
            st.error("Could not find required columns ('Date Initiated', 'Amount Received') in Fincra bank file.")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

        # --- Debug: Show raw data preview ---
        with st.expander("Raw Data Preview (Fincra)"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("Internal Records (first 5):")
                st.write(fincra_hex_df.head())
            with col2:
                st.write("Bank Records (first 5):")
                st.write(fincra_bank_df.head())

        # --- 2. Preprocessing for internal_df (Internal Records - Fincra Hex) ---
        fincra_hex_df.columns = fincra_hex_df.columns.str.strip()
        fincra_hex_df = fincra_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })
        fincra_hex_df['Date'] = pd.to_datetime(fincra_hex_df['Date'], errors='coerce')
        fincra_hex_df = fincra_hex_df.dropna(subset=['Date'])
        
        fincra_hex_df_recon = fincra_hex_df[fincra_hex_df['Amount'] > 0].copy()
        fincra_hex_df_recon = fincra_hex_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        fincra_hex_df_recon['Date_Match'] = fincra_hex_df_recon['Date'].dt.date
        fincra_hex_df_recon['Amount_Rounded'] = fincra_hex_df_recon['Amount'].round(2)

        # --- 3. Preprocessing for bank_df (Bank Statements - Fincra Specific) ---
        fincra_bank_df.columns = fincra_bank_df.columns.str.strip()
        fincra_bank_df = fincra_bank_df.rename(columns={
            'Date Initiated': 'Date',
            'Amount Received': 'Credit',
            'Reference': 'Transaction_ID'
        })

        # Robust date parsing for Fincra format (day/month/year, time GMT)
        fincra_bank_df['Date'] = pd.to_datetime(
            fincra_bank_df['Date'], 
            format='%d/%m/%Y, %I:%M:%S %p GMT%z', # Specific Fincra format
            errors='coerce'
        )
        fincra_bank_df = fincra_bank_df.dropna(subset=['Date'])

        # Filter by 'Status' == 'approved'
        if 'Status' in fincra_bank_df.columns:
            fincra_bank_df = fincra_bank_df[
                fincra_bank_df['Status'].astype(str).str.lower() == 'approved'
            ].copy()
        else:
            st.warning("'Status' column not found in Fincra bank file. Skipping status filter.")

        # Clean credit amounts
        fincra_bank_df['Credit'] = (
            fincra_bank_df['Credit'].astype(str)
            .str.replace(',', '', regex=False) # Remove commas
            .str.replace('[^\d.]', '', regex=True) # Remove other non-numeric except dot
            .replace('', '0')
            .astype(float)
        )
        
        fincra_bank_df = fincra_bank_df[fincra_bank_df['Credit'] > 0].copy()
        fincra_bank_df['Amount'] = fincra_bank_df['Credit']
        fincra_bank_df_recon = fincra_bank_df[['Date', 'Amount', 'Transaction_ID']].copy()
        fincra_bank_df_recon['Date_Match'] = fincra_bank_df_recon['Date'].dt.date
        fincra_bank_df_recon['Amount_Rounded'] = fincra_bank_df_recon['Amount'].round(2)

        # --- Month/Year Filter ---
        if recon_month is None:
            recon_month = datetime.datetime.now().month
        if recon_year is None:
            recon_year = datetime.datetime.now().year
            
        fincra_bank_df_recon = fincra_bank_df_recon[
            (fincra_bank_df_recon['Date'].dt.month == recon_month) & 
            (fincra_bank_df_recon['Date'].dt.year == recon_year)
        ].copy()
        
        fincra_hex_df_recon = fincra_hex_df_recon[
            (fincra_hex_df_recon['Date'].dt.month == recon_month) & 
            (fincra_hex_df_recon['Date'].dt.year == recon_year)
        ].copy()

        # --- Debug: Show processed data ---
        with st.expander("Processed Data Preview (Fincra)"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("Internal Records for Recon:")
                st.write(fincra_hex_df_recon.head())
                st.write(f"Total: {len(fincra_hex_df_recon)} records, {fincra_hex_df_recon['Amount'].sum():,.2f} NGN")
            with col2:
                st.write("Bank Records for Recon:")
                st.write(fincra_bank_df_recon.head())
                st.write(f"Total: {len(fincra_bank_df_recon)} records, {fincra_bank_df_recon['Amount'].sum():,.2f} NGN")
        
        # --- 4. Initial Exact Matching ---
        reconciled_df = pd.merge(
            fincra_hex_df_recon.assign(Source_Internal='Internal'),
            fincra_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        unmatched_internal = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        unmatched_bank = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()

        # --- 5. Date Tolerance Matching (±3 days) ---
        unmatched_internal['Date_DT'] = pd.to_datetime(unmatched_internal['Date_Match'])
        unmatched_bank['Date_DT'] = pd.to_datetime(unmatched_bank['Date_Match'])

        matched_tolerance = []
        date_tolerance = pd.Timedelta(days=3)

        # To avoid modifying the DataFrame while iterating, iterate over a copy or use indices carefully
        # Here, we'll collect matched indices and then drop them outside the loop
        matched_internal_indices = []
        matched_bank_indices = []

        for idx_int, int_row in unmatched_internal.iterrows():
            internal_date = int_row['Date_DT']
            internal_amount = int_row['Amount_Rounded']
            
            # Find potential matches in unmatched_bank that haven't been matched yet
            potential_matches = unmatched_bank[
                (~unmatched_bank.index.isin(matched_bank_indices)) & # Ensure not already matched
                (unmatched_bank['Amount_Rounded'] == internal_amount) &
                (abs(unmatched_bank['Date_DT'] - internal_date) <= date_tolerance)
            ]
            
            if not potential_matches.empty:
                bank_match = potential_matches.iloc[0] # Take the first match
                matched_tolerance.append({
                    'Date_Internal': pd.to_datetime(int_row['Date_Match']), # Convert date.date to datetime
                    'Amount_Internal': int_row['Amount_Internal'],
                    'Date_Bank': pd.to_datetime(bank_match['Date_Match']), # Convert date.date to datetime
                    'Amount_Bank': bank_match['Amount_Bank'],
                    'Date_Diff': (bank_match['Date_DT'] - internal_date).days
                })
                matched_internal_indices.append(idx_int)
                matched_bank_indices.append(bank_match.name)

        matched_tolerance_df = pd.DataFrame(matched_tolerance)

        # Remove matched records from the original unmatched DataFrames
        unmatched_internal = unmatched_internal.drop(matched_internal_indices, errors='ignore')
        unmatched_bank = unmatched_bank.drop(matched_bank_indices, errors='ignore')

        # --- 6. Combine all matches ---
        matched_final = pd.concat([
            matched_exact[['Date_Internal', 'Amount_Internal', 'Date_Bank', 'Amount_Bank']],
            matched_tolerance_df[['Date_Internal', 'Amount_Internal', 'Date_Bank', 'Amount_Bank', 'Date_Diff']]
        ], ignore_index=True)

        # Ensure date columns are datetime64[ns] for Streamlit compatibility
        matched_final['Date_Internal'] = pd.to_datetime(matched_final['Date_Internal'])
        matched_final['Date_Bank'] = pd.to_datetime(matched_final['Date_Bank'])

        # Add 0 days diff for exact matches if not already present
        if 'Date_Diff' not in matched_final.columns:
            matched_final['Date_Diff'] = 0

        # --- 7. Prepare final unmatched records ---
        final_unmatched_internal = unmatched_internal[['Date_Match', 'Amount_Internal']].rename(
            columns={'Date_Match': 'Date', 'Amount_Internal': 'Amount'}
        ) if not unmatched_internal.empty else empty_unmatched.copy()

        final_unmatched_bank = unmatched_bank[['Date_Match', 'Amount_Bank']].rename(
            columns={'Date_Match': 'Date', 'Amount_Bank': 'Amount'}
        ) if not unmatched_bank.empty else empty_unmatched.copy()

        # Ensure date columns in unmatched are also datetime64[ns]
        final_unmatched_internal['Date'] = pd.to_datetime(final_unmatched_internal['Date'])
        final_unmatched_bank['Date'] = pd.to_datetime(final_unmatched_bank['Date'])

        # --- 8. Generate Summary ---
        total_matched = len(matched_final)
        total_internal = len(fincra_hex_df_recon)
        accuracy = (total_matched / total_internal * 100) if total_internal > 0 else 0

        summary = {
            "Provider name": "Fincra",
            "Currency": "NGN",
            "Month & Year": f"{recon_month}/{recon_year}",
            "# of Transactions": total_matched,
            "Partner Statement": fincra_bank_df_recon['Amount'].sum(),
            "Treasury Records": fincra_hex_df_recon['Amount'].sum(),
            "Variance": fincra_hex_df_recon['Amount'].sum() - fincra_bank_df_recon['Amount'].sum(),
            "% accuracy": f"{accuracy:.2f}%",
            "Status": "Matched" if final_unmatched_internal.empty and final_unmatched_bank.empty else "Partial",
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Comments": "",
            "Matching Breakdown": {
                "Exact Matches": len(matched_exact),
                "Tolerance Matches": len(matched_tolerance_df)
            }
        }

        return matched_final, final_unmatched_internal, final_unmatched_bank, summary

    except Exception as e:
        st.error(f"Fincra Reconciliation error: {str(e)}")
        return empty_df, empty_unmatched, empty_unmatched, {}
    
def reconcile_zenith(internal_file_obj: BytesIO, bank_file_obj: BytesIO):
    """
    Performs reconciliation for Zenith Nigeria.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel, with header=3 for bank).
    Includes date tolerance matching (up to 3 days).
    
    Returns:
        tuple: A tuple containing:
            - matched_transactions (pd.DataFrame): DataFrame of matched transactions.
            - unmatched_internal (pd.DataFrame): DataFrame of unmatched internal records.
            - unmatched_bank (pd.DataFrame): DataFrame of unmatched bank records.
            - summary (dict): A dictionary containing reconciliation summary statistics.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = pd.DataFrame(columns=[
        'Date_Internal', 'Amount_Internal', 'ID_Internal',
        'Date_Bank', 'Amount_Bank', 'ID_Bank',
        'Amount_Rounded'
    ])
    unmatched_internal = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    unmatched_bank = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    summary = {}

    try:
        # 1. Load the datasets
        zenith_ng_hex_df = read_uploaded_file(internal_file_obj, header=0) # Assuming internal has no header issues
        zenith_ng_bank_df = read_uploaded_file(bank_file_obj, header=3) # As per provided code
    except Exception as e:
        st.error(f"Error reading files for Zenith NG: {e}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    if zenith_ng_hex_df is None or zenith_ng_bank_df is None:
        st.error("One or both files could not be loaded for Zenith NG.")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    try:
        # 2. Preprocessing for internal_df (Internal Records)
        zenith_ng_hex_df.columns = zenith_ng_hex_df.columns.astype(str).str.strip()

        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if 'TRANSFER_ID' in zenith_ng_hex_df.columns:
            internal_required_cols.append('TRANSFER_ID')

        if not all(col in zenith_ng_hex_df.columns for col in internal_required_cols if col != 'TRANSFER_ID'):
            missing_cols = [col for col in ['TRANSFER_DATE', 'AMOUNT'] if col not in zenith_ng_hex_df.columns]
            st.error(f"Internal records (Zenith Hex) are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        zenith_ng_hex_df_processed = zenith_ng_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description'
        }).copy()

        if 'TRANSFER_ID' in zenith_ng_hex_df.columns:
            zenith_ng_hex_df_processed = zenith_ng_hex_df_processed.rename(columns={'TRANSFER_ID': 'ID'})
        else:
            zenith_ng_hex_df_processed['ID'] = 'Internal_' + zenith_ng_hex_df_processed.index.astype(str)

        zenith_ng_hex_df_processed['Date'] = pd.to_datetime(zenith_ng_hex_df_processed['Date'], errors='coerce')
        zenith_ng_hex_df_processed = zenith_ng_hex_df_processed.dropna(subset=['Date']).copy()

        zenith_ng_hex_df_recon = zenith_ng_hex_df_processed[zenith_ng_hex_df_processed['Amount'] > 0].copy()
        zenith_ng_hex_df_recon = zenith_ng_hex_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        zenith_ng_hex_df_recon.loc[:, 'Date_Match'] = zenith_ng_hex_df_recon['Date'].dt.date
        zenith_ng_hex_df_recon.loc[:, 'Amount_Rounded'] = zenith_ng_hex_df_recon['Amount'].round(2)

        if zenith_ng_hex_df_recon.empty:
            st.warning("No valid internal records found after preprocessing for Zenith NG.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Extract currency
        extracted_currency = "N/A"
        if 'CURRENCY' in zenith_ng_hex_df.columns and not zenith_ng_hex_df['CURRENCY'].empty:
            unique_currencies = zenith_ng_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])
            else:
                extracted_currency = "N/A (No currency in column)"
        else:
            extracted_currency = "NGN" # Default for Zenith Nigeria based on context


        # 3. Preprocessing for bank_df (Bank Statements) - ZENITH SPECIFIC
        zenith_ng_bank_df.columns = zenith_ng_bank_df.columns.astype(str).str.strip()

        # First remove any summary rows that don't contain proper dates
        # Check if 'Effective Date' column exists before trying to access it
        if 'Effective Date' not in zenith_ng_bank_df.columns:
            st.error("Bank statement (Zenith NG) is missing 'Effective Date' column.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary
            
        zenith_ng_bank_df = zenith_ng_bank_df[
            zenith_ng_bank_df['Effective Date'].astype(str).str.match(r'\d{2}/\d{2}/\d{4}', na=False)
        ].copy()

        bank_required_cols = ['Effective Date', 'Description/Payee/Memo', 'Credit Amount']
        if not all(col in zenith_ng_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in zenith_ng_bank_df.columns]
            st.error(f"Bank statement (Zenith NG) is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        zenith_ng_bank_df_processed = zenith_ng_bank_df.rename(columns={
            'Effective Date': 'Date',
            'Description/Payee/Memo': 'Description',
            'Credit Amount': 'Credit'
        }).copy()
        
        # Add a dummy ID for bank statements if none exists, or use existing one if relevant.
        # The original code does not explicitly rename an ID column for bank, so generate one.
        if 'Transaction Ref' in zenith_ng_bank_df.columns: # Assuming a common ref column name
             zenith_ng_bank_df_processed = zenith_ng_bank_df_processed.rename(columns={'Transaction Ref': 'ID'})
        else:
             zenith_ng_bank_df_processed['ID'] = 'Bank_' + zenith_ng_bank_df_processed.index.astype(str)

        # Convert 'Date' to datetime - Nigerian format (day/month/year)
        zenith_ng_bank_df_processed['Date'] = pd.to_datetime(zenith_ng_bank_df_processed['Date'], dayfirst=True, errors='coerce')
        zenith_ng_bank_df_processed = zenith_ng_bank_df_processed.dropna(subset=['Date']).copy()

        # Zenith Nigeria Specific Filters
        # 1. Filter for transactions with Description containing 'TRF'
        zenith_ng_bank_df_processed = zenith_ng_bank_df_processed[
            zenith_ng_bank_df_processed['Description'].astype(str).str.contains('TRF FRM NALA PAYMENTS', case=False, na=False)
        ].copy()

        # 2. Process Credit Amount - remove commas and convert to numeric
        zenith_ng_bank_df_processed['Credit'] = (
            zenith_ng_bank_df_processed['Credit'].astype(str)
            .str.replace(',', '', regex=False)
            .replace('', '0')
            .astype(float)
        )

        # 3. Filter for positive credits only
        zenith_ng_bank_df_recon = zenith_ng_bank_df_processed[
            zenith_ng_bank_df_processed['Credit'] > 0
        ].copy()

        zenith_ng_bank_df_recon['Amount'] = zenith_ng_bank_df_recon['Credit']
        zenith_ng_bank_df_recon = zenith_ng_bank_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        zenith_ng_bank_df_recon.loc[:, 'Date_Match'] = zenith_ng_bank_df_recon['Date'].dt.date
        zenith_ng_bank_df_recon.loc[:, 'Amount_Rounded'] = zenith_ng_bank_df_recon['Amount'].round(2)

        if zenith_ng_bank_df_recon.empty:
            st.warning("No valid bank records found after preprocessing for Zenith NG.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # 4. Calculate Total Amounts and Discrepancy
        total_internal_credits = zenith_ng_hex_df_recon['Amount'].sum()
        total_bank_credits = zenith_ng_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # 5. Reconciliation (transaction-level) with Date Tolerance
        # Ensure Date_Match columns have consistent types (already datetime.date from .dt.date)
        # No need to convert to string and back to datetime.
        # The perform_date_tolerance_matching helper expects datetime objects, so ensure they are.

        # Initial exact match
        reconciled_df = pd.merge(
            zenith_ng_hex_df_recon.assign(Source_Internal='Internal'),
            zenith_ng_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify initially matched transactions
        matched_initial = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_initial.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_initial.columns]
            matched_transactions = matched_initial[cols_to_select].copy()
        else:
            matched_transactions = create_empty_matched_df()


        # Prepare for tolerance matching by getting initially unmatched records
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            # Ensure 'ID' column exists, if not, create a placeholder for the helper
            if 'ID_Internal' not in unmatched_internal_initial.columns:
                 unmatched_internal_initial.loc[:, 'ID_Internal'] = 'Internal_Unmatched_' + unmatched_internal_initial.index.astype(str)

            unmatched_internal_initial = unmatched_internal_initial[[
                'Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded'
            ]].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = create_empty_unmatched_df()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])


        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            # Ensure 'ID' column exists, if not, create a placeholder for the helper
            if 'ID_Bank' not in unmatched_bank_initial.columns:
                 unmatched_bank_initial.loc[:, 'ID_Bank'] = 'Bank_Unmatched_' + unmatched_bank_initial.index.astype(str)

            unmatched_bank_initial = unmatched_bank_initial[[
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ]].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = create_empty_unmatched_df()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        
        matched_date_tolerance_df = pd.DataFrame()
        final_unmatched_internal = unmatched_internal_initial.copy()
        final_unmatched_bank = unmatched_bank_initial.copy()

        if not unmatched_internal_initial.empty and not unmatched_bank_initial.empty:
            st.info("Attempting date tolerance matching for remaining unmatched records (Zenith NG)...")
            matched_date_tolerance_df, final_unmatched_internal, final_unmatched_bank = \
                perform_date_tolerance_matching(
                    unmatched_internal_initial,
                    unmatched_bank_initial,
                    tolerance_days=3
                )
        
        # Combine matched records from initial and date tolerance stages
        matched_total = pd.concat([matched_initial, matched_date_tolerance_df], ignore_index=True)

        # Sum of amounts for summary
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0


        # 6. Summary of Reconciliation
        summary = {
            "Total Internal Records (for recon)": len(zenith_ng_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(zenith_ng_bank_df_recon),
            "Total Internal Credits (Original)": total_internal_credits,
            "Total Bank Credits (Original)": total_bank_credits,
            "Overall Discrepancy (Original)": discrepancy_amount,
            "Total Matched Transactions (All Stages)": len(matched_total),
            "Total Matched Amount (Internal)": total_matched_amount_internal,
            "Total Matched Amount (Bank)": total_matched_amount_bank,
            "Unmatched Internal Records (Final)": len(final_unmatched_internal),
            "Unmatched Bank Records (Final)": len(final_unmatched_bank),
            "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
            "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
            "Currency": extracted_currency
        }

    except Exception as e:
        st.error(f"Error during Zenith NG reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_flutterwave_ngn(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Flutterwave Nigeria (NGN) following the exact logic from Jupyter notebook.
    Includes exact match, date tolerance matching (±3 days), and same-day aggregation.
    Returns updated summary with all matching stages accounted for.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = pd.DataFrame(columns=[
        'Date_Internal', 'Amount_Internal', 'Date_Bank', 'Amount_Bank', 
        'Date_Match_Internal', 'Date_Match_Bank', 'Amount_Rounded'
    ])
    unmatched_internal = pd.DataFrame(columns=['Date_Match', 'Amount_Internal'])
    unmatched_bank = pd.DataFrame(columns=['Date_Match', 'Amount_Bank'])
    summary = {}

    try:
        # --- 1. Load the datasets ---
        FW_ng_hex_df = read_uploaded_file(internal_file_obj, header=0)
        FW_ng_bank_df = read_uploaded_file(bank_file_obj, header=0)
        
        if FW_ng_hex_df is None or FW_ng_bank_df is None:
            st.error("Failed to load one or both files")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal_df (Internal Records) ---
        FW_ng_hex_df.columns = FW_ng_hex_df.columns.str.strip()
        
        # Essential columns check
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in FW_ng_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in FW_ng_hex_df.columns]
            st.error(f"Internal records missing columns: {', '.join(missing_cols)}")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        FW_ng_hex_df = FW_ng_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        FW_ng_hex_df['Date'] = pd.to_datetime(FW_ng_hex_df['Date'], errors='coerce')
        FW_ng_hex_df = FW_ng_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        FW_ng_hex_df_recon = FW_ng_hex_df[FW_ng_hex_df['Amount'] > 0].copy()
        FW_ng_hex_df_recon['Date_Match'] = FW_ng_hex_df_recon['Date'].dt.date.astype(str)
        FW_ng_hex_df_recon['Amount_Rounded'] = FW_ng_hex_df_recon['Amount'].round(2)
        total_internal_credits = FW_ng_hex_df_recon['Amount'].sum()
        total_internal_records = len(FW_ng_hex_df_recon)

        # --- 3. Preprocessing for bank_df (Bank Statements) ---
        FW_ng_bank_df.columns = FW_ng_bank_df.columns.str.strip()
        
        # Essential columns check
        bank_required_cols = ['date', 'amount', 'type', 'remarks']
        if not all(col in FW_ng_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in FW_ng_bank_df.columns]
            st.error(f"Bank statement missing columns: {', '.join(missing_cols)}")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Filter for credits and exclude reversals
        FW_ng_bank_df = FW_ng_bank_df[
            (FW_ng_bank_df['type'].astype(str).str.upper() == 'C') & 
            (~FW_ng_bank_df['remarks'].astype(str).str.contains('rvsl', case=False, na=False))
        ].copy()

        # Rename columns
        FW_ng_bank_df = FW_ng_bank_df.rename(columns={
            'date': 'Date',
            'amount': 'Credit',
            'reference': 'Transaction_ID',
            'remarks': 'Description'
        })

        # Process dates and amounts
        FW_ng_bank_df['Date'] = pd.to_datetime(FW_ng_bank_df['Date']).dt.tz_localize(None)
        FW_ng_bank_df['Credit'] = (
            FW_ng_bank_df['Credit'].astype(str)
            .str.replace(',', '', regex=False)
            .replace('', '0')
            .astype(float))
        
        # Filter positive credits and prepare for reconciliation
        FW_ng_bank_df = FW_ng_bank_df[FW_ng_bank_df['Credit'] > 0].copy()
        FW_ng_bank_df['Amount'] = FW_ng_bank_df['Credit']
        FW_ng_bank_df_recon = FW_ng_bank_df[['Date', 'Amount', 'Description', 'Transaction_ID']].copy()
        FW_ng_bank_df_recon['Date_Match'] = FW_ng_bank_df_recon['Date'].dt.date.astype(str)
        FW_ng_bank_df_recon['Amount_Rounded'] = FW_ng_bank_df_recon['Amount'].round(2)
        total_bank_credits = FW_ng_bank_df_recon['Amount'].sum()
        total_bank_records = len(FW_ng_bank_df_recon)

        # --- 4. Initial Exact Matching ---
        reconciled_df = pd.merge(
            FW_ng_hex_df_recon.assign(Source_Internal='Internal'),
            FW_ng_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Get matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        
        # Get unmatched records
        unmatched_internal = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        unmatched_bank = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()

        # --- 5. Date Tolerance Matching (±3 days) ---
        unmatched_internal['Date_Match_DT'] = pd.to_datetime(unmatched_internal['Date_Match'])
        unmatched_bank['Date_Match_DT'] = pd.to_datetime(unmatched_bank['Date_Match'])
        
        date_tolerance = pd.Timedelta(days=3)
        matched_with_tolerance = []
        matched_internal_indices = []
        matched_bank_indices = []

        for idx, internal_row in unmatched_internal.iterrows():
            internal_date = internal_row['Date_Match_DT']
            internal_amount = internal_row['Amount_Rounded']
            
            potential_matches = unmatched_bank[
                (unmatched_bank['Amount_Rounded'] == internal_amount) &
                (abs(unmatched_bank['Date_Match_DT'] - internal_date) <= date_tolerance)
            ]
            
            if not potential_matches.empty:
                bank_match = potential_matches.iloc[0]
                matched_with_tolerance.append({
                    'Date_Internal': internal_row['Date_Internal'],
                    'Amount_Internal': internal_row['Amount_Internal'],
                    'Date_Match_Internal': internal_row['Date_Match'],
                    'Date_Bank': bank_match['Date_Bank'],
                    'Amount_Bank': bank_match['Amount_Bank'],
                    'Date_Match_Bank': bank_match['Date_Match'],
                    'Amount_Rounded': internal_amount
                })
                matched_internal_indices.append(idx)
                matched_bank_indices.append(bank_match.name)

        matched_with_tolerance_df = pd.DataFrame(matched_with_tolerance)
        final_unmatched_internal = unmatched_internal.drop(matched_internal_indices)
        final_unmatched_bank = unmatched_bank.drop(matched_bank_indices)

        # --- 6. Same-Day Aggregation Matching ---
        matched_aggregated = []
        if len(final_unmatched_internal) > 0:
            # Group internal transactions by date and sum amounts
            internal_aggregated = (
                final_unmatched_internal.groupby('Date_Match')
                .agg({'Amount_Internal': 'sum'})
                .reset_index()
            )
            internal_aggregated['Amount_Rounded'] = internal_aggregated['Amount_Internal'].round(2)
            internal_aggregated['Date_Match_DT'] = pd.to_datetime(internal_aggregated['Date_Match'])
            
            # Prepare bank records for matching
            bank_unmatched = final_unmatched_bank.copy()
            bank_unmatched['Date_Match_DT'] = pd.to_datetime(bank_unmatched['Date_Match'])
            
            matched_bank_indices_agg = []
            
            for idx, internal_row in internal_aggregated.iterrows():
                internal_date = internal_row['Date_Match_DT']
                internal_amount = internal_row['Amount_Rounded']
                
                potential_matches = bank_unmatched[
                    (bank_unmatched['Amount_Rounded'] == internal_amount) &
                    (bank_unmatched['Date_Match_DT'] == internal_date)
                ]
                
                if not potential_matches.empty:
                    bank_match = potential_matches.iloc[0]
                    matched_aggregated.append({
                        'Date_Internal': internal_row['Date_Match'],
                        'Amount_Internal': internal_row['Amount_Internal'],
                        'Date_Bank': bank_match['Date_Match'],
                        'Amount_Bank': bank_match['Amount_Bank'],
                        'Amount_Rounded': internal_amount
                    })
                    matched_bank_indices_agg.append(bank_match.name)
            
            # Update unmatched records if we found aggregated matches
            if matched_aggregated:
                matched_aggregated_df = pd.DataFrame(matched_aggregated)
                final_unmatched_bank = final_unmatched_bank.drop(matched_bank_indices_agg)
                
                # Remove the matched internal transactions
                matched_dates = [m['Date_Internal'] for m in matched_aggregated]
                final_unmatched_internal = final_unmatched_internal[
                    ~final_unmatched_internal['Date_Match'].isin(matched_dates)
                ]

        # --- 7. Combine all matches ---
        all_matched = pd.concat([
            matched_exact,
            matched_with_tolerance_df,
            pd.DataFrame(matched_aggregated) if matched_aggregated else pd.DataFrame()
        ], ignore_index=True)

        # --- 8. Prepare final output ---
        # Convert Date_Match back to datetime for final unmatched DataFrames
        if not final_unmatched_internal.empty:
            final_unmatched_internal = final_unmatched_internal[['Date_Match', 'Amount_Internal']].copy()
            final_unmatched_internal['Date_Match'] = pd.to_datetime(final_unmatched_internal['Date_Match'])
        
        if not final_unmatched_bank.empty:
            final_unmatched_bank = final_unmatched_bank[['Date_Match', 'Amount_Bank']].copy()
            final_unmatched_bank['Date_Match'] = pd.to_datetime(final_unmatched_bank['Date_Match'])

        # --- 9. Generate Summary ---
        total_matched = len(all_matched)
        total_unmatched_internal = len(final_unmatched_internal)
        total_unmatched_bank = len(final_unmatched_bank)
        
        # Calculate updated amounts after all matching stages
        total_matched_amount_internal = all_matched['Amount_Internal'].sum() if not all_matched.empty else 0
        total_matched_amount_bank = all_matched['Amount_Bank'].sum() if not all_matched.empty else 0
        
        # Calculate updated variance (using matched amounts)
        updated_variance = total_internal_credits - total_bank_credits
        
        # Calculate accuracy based on matched internal records vs total internal records
        accuracy = (total_bank_records / total_internal_records * 100) if total_internal_records > 0 else 0
        
        # Determine status based on remaining unmatched records
        status = "Matched" if total_unmatched_internal == 0 and total_unmatched_bank == 0 else "Partial"
        if total_unmatched_internal > 0 and total_unmatched_bank > 0:
            status = "Multiple Unmatched"

        summary = {
            "Provider name": "Flutterwave NGN",
            "Currency": "NGN",
            "Month & Year": datetime.datetime.now().strftime("%m/%Y"),
            "# of Transactions": total_matched,
            "Partner Statement": total_bank_credits,
            "Treasury Records": total_internal_credits,
            "Variance": updated_variance,
            "% accuracy": f"{accuracy:.2f}%",
            "Status": status,
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Comments": "",
            "Matching Breakdown": {
                "Exact Matches": len(matched_exact),
                "Tolerance Matches": len(matched_with_tolerance_df),
                "Aggregated Matches": len(matched_aggregated) if matched_aggregated else 0,
                "Unmatched Internal": total_unmatched_internal,
                "Unmatched Bank": total_unmatched_bank
            }
        }

        return all_matched, final_unmatched_internal, final_unmatched_bank, summary

    except Exception as e:
        st.error(f"Error during reconciliation: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

def load_moniepoint_bank_statement(file_obj):
    """
    Handles MoniePoint's multi-sheet Excel files, including legacy .xls format.
    It reads all sheets and combines them, using the header from the first sheet.
    """
    try:
        # Use a seek(0) to ensure the file pointer is at the beginning
        file_obj.seek(0)
        
        # Use xlrd engine for broader compatibility with .xls files
        xls = pd.ExcelFile(file_obj, engine='xlrd')
        sheet_names = xls.sheet_names
        
        if not sheet_names:
            st.warning("No sheets found in the MoniePoint bank statement.")
            return pd.DataFrame()

        # Read the first sheet to establish the columns
        df_first_sheet = pd.read_excel(xls, sheet_name=sheet_names[0], header=0)
        columns = df_first_sheet.columns.tolist()
        
        all_dfs = [df_first_sheet]

        # Read the remaining sheets without headers and apply the columns from the first sheet
        for sheet_name in sheet_names[1:]:
            df_other_sheet = pd.read_excel(xls, sheet_name=sheet_name, header=None)
            if df_other_sheet.shape[1] == len(columns):
                df_other_sheet.columns = columns
                all_dfs.append(df_other_sheet)
            else:
                st.warning(f"Skipping sheet '{sheet_name}' due to column count mismatch.")
        
        # Combine all valid sheets into a single DataFrame
        combined_df = pd.concat(all_dfs, ignore_index=True)
        return combined_df

    except Exception as e:
        st.error(f"Failed to load the MoniePoint bank statement. Error: {str(e)}")
        raise

def reconcile_moniepoint(internal_file_obj, bank_file_obj, recon_month=None, recon_year=None):
    """
    Performs reconciliation for MoniePoint Nigeria (NGN) by implementing the exact
    logic from the user's ipynb code, including multi-sheet handling, specific
    narration filters, and aggregation of split transactions within 30-minute windows.
    """
    # Initialize empty return values
    matched_total = create_empty_matched_df()
    final_unmatched_internal = create_empty_unmatched_df()
    final_unmatched_bank = create_empty_unmatched_df()
    summary = {}

    try:
        # --- 1. Load the datasets ---
        MP_ng_hex_df = read_uploaded_file(internal_file_obj, header=0)
        MP_ng_bank_df = load_moniepoint_bank_statement(bank_file_obj)

        if MP_ng_hex_df is None or MP_ng_bank_df is None or MP_ng_bank_df.empty:
            st.error("One or both files could not be loaded for MoniePoint.")
            return matched_total, final_unmatched_internal, final_unmatched_bank, summary

        # --- 2. Preprocessing for internal_df (Internal Records) ---
        MP_ng_hex_df.columns = MP_ng_hex_df.columns.str.strip()
        MP_ng_hex_df = MP_ng_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date', 'AMOUNT': 'Amount',
            'COMMENT': 'Description', 'TRANSFER_ID': 'ID'
        })
        if 'Description' not in MP_ng_hex_df.columns: MP_ng_hex_df['Description'] = ''
        if 'ID' not in MP_ng_hex_df.columns: MP_ng_hex_df['ID'] = 'Internal_' + MP_ng_hex_df.index.astype(str)

        MP_ng_hex_df['Date'] = pd.to_datetime(MP_ng_hex_df['Date'], errors='coerce')
        MP_ng_hex_df_recon = MP_ng_hex_df.dropna(subset=['Date'])
        MP_ng_hex_df_recon = MP_ng_hex_df_recon[MP_ng_hex_df_recon['Amount'] > 0].copy()
        MP_ng_hex_df_recon = MP_ng_hex_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        MP_ng_hex_df_recon['Date_Match'] = MP_ng_hex_df_recon['Date'].dt.date
        MP_ng_hex_df_recon['Amount_Rounded'] = MP_ng_hex_df_recon['Amount'].round(2)

        # --- 3. Preprocessing for bank_df (Bank Statements) - MONIEPOINT SPECIFIC ---
        def preprocess_moniepoint_bank(df):
            df.columns = df.columns.str.strip()
            required_cols = ['DATE', 'AMOUNT', 'TRANSACTION_TYPE', 'NARRATION', 'REFERENCE']
            if not all(col in df.columns for col in required_cols):
                st.error(f"Bank statement missing columns: {', '.join([c for c in required_cols if c not in df.columns])}")
                return pd.DataFrame()

            df['DATE'] = pd.to_datetime(df['DATE'], errors='coerce')
            df = df.dropna(subset=['DATE']).copy()
            if recon_month and recon_year:
                df = df[(df['DATE'].dt.month == recon_month) & (df['DATE'].dt.year == recon_year)].copy()

            df_cleaned = df[
                (df['TRANSACTION_TYPE'] == 'CREDIT') &
                (df['NARRATION'].str.contains('MFY-WT', na=False)) &
                (~df['REFERENCE'].str.contains('RVSL', na=False))
            ].copy()

            if df_cleaned.empty: return pd.DataFrame()

            def extract_sender_name(narration):
                narration = str(narration).lower()
                if 'verto financial tech' in narration or 'paga' in narration: return 'VertoFX, NGN a/c'
                elif 'sendfirst' in narration or 'duplo ltd' in narration: return 'Duplo, NGN a/c'
                elif 'esca' in narration: return 'Esca Nigeria, NGN a/c'
                elif 'resrv' in narration: return 'Resrv FX, NGN a/c'
                elif 'waza' in narration: return 'Waza, Nigeria, NGN a/c'
                elif 'flutterwave' in narration: return 'Flutterwave, NGN a/c'
                elif 'inexass' in narration: return 'AZA Finance, NGN a/c'
                elif 'nala' in narration: return 'Nala Payments'
                elif 'south one' in narration: return 'Southone NGN a/c'
                elif 'brampton' in narration: return 'Zenith Bank NG, NGN a/c'
                elif 'titan-paystack' in narration or 'multigate' in narration: return 'Multigate, NGN a/c'
                elif 'zerozilo' in narration or 'silverfile' in narration or 'palm bills' in narration: return 'Fincra, NGN a/c'
                elif 'ift technologies' in narration or 'budpay' in narration or 'bud infrastructure' in narration: return 'Torus Mara, NGN a/c'
                elif 'starks associates limited' in narration or 'shamiri' in narration or 'second jeu' in narration: return 'Straitpay (Starks), UK, NGN a/c'
                else: return 'Unknown'
            df_cleaned['SENDER_NAME'] = df_cleaned['NARRATION'].apply(extract_sender_name)

            df_combined = df_cleaned.groupby(['SENDER_NAME', pd.Grouper(key='DATE', freq='30min')]).agg({
                'AMOUNT': 'sum',
                'NARRATION': lambda x: ' // '.join(x.dropna().unique()),
                'REFERENCE': lambda x: ' // '.join(x.dropna().unique()),
                'TRANSACTION_TYPE': 'first'
            }).reset_index()
            df_combined.rename(columns={'DATE': 'Date', 'AMOUNT': 'Amount'}, inplace=True)
            return df_combined

        MP_ng_bank_df_recon = preprocess_moniepoint_bank(MP_ng_bank_df)
        
        # Add download button for preprocessed bank statement
        if MP_ng_bank_df_recon is not None and not MP_ng_bank_df_recon.empty:
            with st.expander("Download Preprocessed Bank Statement"):
                col1, col2 = st.columns(2)
                
                # CSV Download
                csv = MP_ng_bank_df_recon.to_csv(index=False).encode('utf-8')
                col1.download_button(
                    label="Download as CSV",
                    data=csv,
                    file_name="moniepoint_preprocessed_bank_statement.csv",
                    mime="text/csv"
                )
                
                # Excel Download
                excel_buffer = BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    MP_ng_bank_df_recon.to_excel(writer, index=False, sheet_name='Bank Statement')
                excel_buffer.seek(0)
                col2.download_button(
                    label="Download as Excel",
                    data=excel_buffer,
                    file_name="moniepoint_preprocessed_bank_statement.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

        if MP_ng_bank_df_recon.empty:
            st.warning("No bank records to reconcile after preprocessing.")
            final_unmatched_internal = MP_ng_hex_df_recon.rename(columns={'Date': 'Date_Internal', 'Amount': 'Amount_Internal'})
            return matched_total, final_unmatched_internal, final_unmatched_bank, summary

        MP_ng_bank_df_recon['Date_Match'] = MP_ng_bank_df_recon['Date'].dt.date
        MP_ng_bank_df_recon['Amount_Rounded'] = MP_ng_bank_df_recon['Amount'].round(2)
        MP_ng_bank_df_recon['ID'] = 'Bank_' + MP_ng_bank_df_recon.index.astype(str)

        # --- 4. Reconciliation: Exact Match ---
        reconciled_df = pd.merge(
            MP_ng_hex_df_recon, MP_ng_bank_df_recon,
            on=['Date_Match', 'Amount_Rounded'], how='outer', suffixes=('_Internal', '_Bank')
        )
        matched_exact = reconciled_df.dropna(subset=['ID_Internal', 'ID_Bank']).copy()

        # --- 5. Reconciliation: Date Tolerance Match ---
        unmatched_internal_df = reconciled_df[reconciled_df['ID_Bank'].isna()][['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'})
        unmatched_bank_df = reconciled_df[reconciled_df['ID_Internal'].isna()][['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'})

        matched_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_df, unmatched_bank_df, tolerance_days=3
        )

        # --- 6. Combine All Matches and Finalize ---
        matched_total = pd.concat([matched_exact, matched_tolerance], ignore_index=True)

        # --- 7. Generate Summary ---
        total_internal_credits = MP_ng_hex_df_recon['Amount'].sum()
        total_bank_credits = MP_ng_bank_df_recon['Amount'].sum()
        summary = {
            "Total Internal Records (for recon)": len(MP_ng_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(MP_ng_bank_df_recon),
            "Total Internal Credits (Original)": total_internal_credits,
            "Total Bank Credits (Original)": total_bank_credits,
            "Overall Discrepancy (Original)": total_internal_credits - total_bank_credits,
            "Total Matched Transactions (All Stages)": len(matched_total),
            "Unmatched Internal Records (Final)": len(final_unmatched_internal),
            "Unmatched Bank Records (Final)": len(final_unmatched_bank),
            "Unmatched Internal Amount (Final)": final_unmatched_internal['Amount'].sum(),
            "Unmatched Bank Amount (Final)": final_unmatched_bank['Amount'].sum(),
            "Currency": "NGN"
        }
        return matched_total, final_unmatched_internal, final_unmatched_bank, summary

    except Exception as e:
        st.error(f"An unexpected error occurred during MoniePoint reconciliation: {str(e)}")
        return matched_total, final_unmatched_internal, final_unmatched_bank, summary
    
# --- Helper Functions for Reports Storage ---      
def load_all_reports():
    """Loads all stored reconciliation reports with better error handling"""
    required_columns = [
        "Provider name", "Currency", "Month & Year", "# of Transactions",
        "Partner Statement", "Treasury Records", "Variance", "% accuracy", "Status",
        "Timestamp", "Comments"
    ]
    
    print(f"Attempting to load reports from: {REPORTS_FILE}")  # Debug logging
    
    # Ensure the reports directory exists
    Path(REPORTS_DIR).mkdir(parents=True, exist_ok=True)
    
    if os.path.exists(REPORTS_FILE):
        try:
            print(f"Loading existing reports file")  # Debug logging
            df = pd.read_csv(REPORTS_FILE)
            
            # Check for and add any missing columns
            for col in required_columns:
                if col not in df.columns:
                    print(f"Adding missing column: {col}")  # Debug logging
                    df[col] = None
            
            print(f"Successfully loaded {len(df)} reports")  # Debug logging
            return df[required_columns]  # Ensure column order
            
        except Exception as e:
            print(f"Error loading reports: {str(e)}")  # Debug logging
            st.error(f"Error loading reports file: {str(e)}")
            return pd.DataFrame(columns=required_columns)
    else:
        print("No existing reports file found, returning empty DataFrame")  # Debug logging
        return pd.DataFrame(columns=required_columns)

def save_all_reports(df):
    """Enhanced saving function with atomic writes and validation"""
    try:
        # Validate DataFrame structure
        required_columns = [
            "Provider name", "Currency", "Month & Year", "# of Transactions",
            "Partner Statement", "Treasury Records", "Variance", "% accuracy", "Status",
            "Timestamp", "Comments"
        ]
        
        for col in required_columns:
            if col not in df.columns:
                return False, f"Missing required column: {col}"
        
        # Ensure directory exists
        Path(REPORTS_DIR).mkdir(parents=True, exist_ok=True)
        
        # Atomic write using temporary file
        temp_path = REPORTS_FILE + ".temp"
        df.to_csv(temp_path, index=False)
        
        # Verify write was successful
        if not os.path.exists(temp_path) or os.path.getsize(temp_path) == 0:
            return False, "Failed to write temporary file"
        
        # Replace existing file
        if os.path.exists(REPORTS_FILE):
            os.remove(REPORTS_FILE)
        os.rename(temp_path, REPORTS_FILE)
        
        return True, "Successfully saved reports"
        
    except Exception as e:
        return False, f"Error saving reports: {str(e)}"
       
def get_currency_for_country(country):
    """Maps country to a currency for the report summary."""
    currency_map = {
        "Kenya": "KES",
        "Tanzania": "TZS",
        "Uganda": "UGX",
        "Ghana": "GHS",
        "Senegal & Côte d'Ivoire XOF": "XOF",
        "Rwanda": "RWF",
        "Nigeria": "NGN",
        "Cameroon XAF": "XAF"
    }
    return currency_map.get(country, "N/A")

def generate_excel_summary_row(summary_dict, provider_name, selected_country, recon_month_year):
    """Generates a standardized summary row with all required columns"""
    required_columns = [
        "Provider name", "Currency", "Month & Year", "# of Transactions",
        "Partner Statement", "Treasury Records", "Variance", "% accuracy", "Status",
        "Timestamp", "Comments"
    ]
    
    # Calculate values with defaults
    currency = summary_dict.get("Currency", get_currency_for_country(selected_country))
    total_internal = summary_dict.get("Total Internal Records (for recon)", 0)
    total_matched = summary_dict.get("Total Matched Transactions (All Stages)", 0)
    percentage_accuracy = (total_matched / total_internal * 100) if total_internal > 0 else 0
    variance = summary_dict.get("Overall Discrepancy (Original)", 0)
    status = "Matched" if abs(variance) < 0.01 else "Unmatched"
    
    # Create the row with all required columns
    row = {
        "Provider name": provider_name,
        "Currency": currency,
        "Month & Year": recon_month_year,
        "# of Transactions": total_matched + 
                           summary_dict.get("Unmatched Internal Records (Final)", 0) + 
                           summary_dict.get("Unmatched Bank Records (Final)", 0),
        "Partner Statement": summary_dict.get("Total Bank Credits (Original)", 0),
        "Treasury Records": summary_dict.get("Total Internal Credits (Original)", 0),
        "Variance": variance,
        "% accuracy": f"{percentage_accuracy:.2f}%",
        "Status": status,
        "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Comments": ""
    }
    
    # Ensure all required columns are present (fill with None if missing)
    for col in required_columns:
        if col not in row:
            row[col] = None
    
    return row

# --- Placeholder Reconciliation Function (for banks not yet implemented) ---
def placeholder_reconcile(internal_file_obj, bank_file_obj, recon_month=None, recon_year=None):
    """A placeholder function for reconciliation."""
    st.info("Reconciliation logic for this bank is not yet implemented.")
    return (
        create_empty_matched_df(),
        create_empty_unmatched_df(),
        create_empty_unmatched_df(),
        {
            "Total Internal Credits": 0.00,
            "Total Bank Credits": 0.00,
            "Discrepancy": 0.00,
            "Total Internal Records": 0,
            "Total Bank Records": 0,
            "Matched Transactions": 0,
            "Unmatched Internal Records": 0,
            "Unmatched Bank Records": 0
        }
    )
# Mapping bank names to their respective reconciliation functions
# Use .get() to safely retrieve specific functions, default to placeholder
RECONCILIATION_FUNCTIONS = {
    bank: globals().get(f"reconcile_{bank.replace(' ', '_').replace('(', '').replace(')', '').lower()}", placeholder_reconcile)
    for country_banks in COUNTRIES_BANKS.values()
    for bank in country_banks
}
# Manually map specific functions that don't follow the direct naming convention
RECONCILIATION_FUNCTIONS["Equity KE"] = reconcile_equity_ke
RECONCILIATION_FUNCTIONS["Cellulant KE"] = reconcile_cellulant_ke
RECONCILIATION_FUNCTIONS["Zamupay PYCS"] = reconcile_zamupay
RECONCILIATION_FUNCTIONS["Selcom TZ"] = reconcile_selcom_tz
RECONCILIATION_FUNCTIONS["Equity TZ"] = reconcile_equity_tz
RECONCILIATION_FUNCTIONS["Cellulant TZ"] = reconcile_cellulant_tz
RECONCILIATION_FUNCTIONS["Fincra NGN"] = reconcile_fincra
RECONCILIATION_FUNCTIONS["Flutterwave NGN"] = reconcile_flutterwave_ngn
RECONCILIATION_FUNCTIONS["Moniepoint"] = reconcile_moniepoint

# --- Streamlit UI Page Functions ---
def homepage():
    """Displays the country and bank selection page, with all options visible and organized."""
    st.header("Treasury Flows Reconciliation")
    st.write("Select a country to see its providers, then click on a partner to begin reconciliation.")
    st.divider() # Visual separator
    st.subheader("Country:")
    # Horizontal Radio Buttons for Country Selection
    selected_country = st.radio("Select to see banking partners:",
        options=list(COUNTRIES_BANKS.keys()),
        index=0, # Default to the first country
        horizontal=True,
        key="country_radio_selection"
    )

    st.markdown("---") # Small separator
    st.subheader(f"Banking Partners in {selected_country}")
    
    # Display banks for the selected country in a responsive grid of "cards"
    banks_for_selected_country = COUNTRIES_BANKS.get(selected_country, [])
    
    # Use columns to create a grid layout for bank cards
    num_columns = 3 
    cols = st.columns(num_columns)

    for i, bank in enumerate(banks_for_selected_country):
        with cols[i % num_columns]: # Distribute cards across columns
            # Use st.container to create the card-like effect
            with st.container(border=True):
                #st.markdown(f"**{bank}**") # Simple bold text for bank name
                # Streamlit button to trigger navigation
                if st.button(f"{bank.split('(')[0].strip()}", key=f"bank_card_button_{selected_country}_{bank}", use_container_width=True):
                    st.session_state.page = "reconciliation"
                    st.session_state.selected_bank = bank
                    st.rerun() # Rerun the app to switch page

def reconciliation_page():
    st.header(f"Reconcile: {st.session_state.selected_bank}")
    
    # Only show month/year filter for Nigerian banks
    if st.session_state.selected_bank in MONTH_FILTER_BANKS:
        recon_date = st.date_input("Reconciliation Month", 
                                 value=datetime.date.today().replace(day=1),
                                 help="Select the month/year for reconciliation")
        recon_month = recon_date.month
        recon_year = recon_date.year
    else:
        recon_month = None
        recon_year = None
    
    # File Uploaders
    col1, col2 = st.columns(2)
    with col1:
        internal_file = st.file_uploader("Internal Records (CSV)", type=["csv"])
    with col2:
        bank_file = st.file_uploader("Bank Statement (CSV/Excel)", type=["csv", "xlsx", "xls"])

    if st.button("Run Reconciliation", type="primary"):
        if not all([internal_file, bank_file]):
            st.warning("Please upload both files")
            return
            
        with st.spinner("Processing..."):
            # Get the appropriate reconciliation function
            recon_func = RECONCILIATION_FUNCTIONS.get(st.session_state.selected_bank, placeholder_reconcile)
            
            try:
                # Call the reconciliation function with appropriate parameters
                if st.session_state.selected_bank in MONTH_FILTER_BANKS:
                    matched, unmatched_int, unmatched_bank, summary = recon_func(
                        internal_file,
                        bank_file,
                        recon_month=recon_month,
                        recon_year=recon_year
                    )
                else:
                    # For non-Nigerian banks, call with just the two required parameters
                    matched, unmatched_int, unmatched_bank, summary = recon_func(
                        internal_file,
                        bank_file
                    )
                
                # Display Results
                st.success("Reconciliation Complete ✅")
                
                # Summary Metrics
                cols = st.columns(5)
                cols[0].metric("Total Matched", summary.get("# of Transactions", summary.get("Total Matched Transactions (All Stages)", 0)))
                
                accuracy = summary.get("% accuracy", 
                                    f"{(summary.get('Total Matched Transactions (All Stages)', 0) / summary.get('Total Internal Records (for recon)', 1) * 100):.2f}%"
                                    if summary.get('Total Internal Records (for recon)', 0) > 0 
                                    else "0%")
                cols[1].metric("Accuracy", accuracy)
                
                treasury_total = summary.get("Treasury Records", summary.get("Total Internal Credits (Original)", 0))
                cols[2].metric("Treasury Total", f"{treasury_total:,.2f}")
                
                bank_total = summary.get("Partner Statement", summary.get("Total Bank Credits (Original)", 0))
                cols[3].metric("Bank Total", f"{bank_total:,.2f}")

                variance = summary.get("Variance", summary.get("Overall Discrepancy (Original)", 0))
                variance_formatted = f"{abs(variance):,.2f}"

                # Determine color and direction
                if variance > 0:
                    variance_label = "Over (Treasury > Bank)"
                    delta_color = "inverse"
                elif variance < 0:
                    variance_label = "Under (Treasury < Bank)"
                    delta_color = "normal"
                else:
                    variance_label = "Balanced"
                    delta_color = "off"

                cols[4].metric(
                    "Variance", 
                    variance_formatted,
                    delta=variance_label,
                    delta_color=delta_color
                )
                
                # Results Tabs - Updated to handle display properly
                tab1, tab2, tab3 = st.tabs(["Unmatched Internal", "Unmatched Bank", "Matched"])
                
                with tab1:
                    if unmatched_int.empty:
                        st.success("All internal records matched")
                    else:
                        st.dataframe(unmatched_int)
                
                with tab2:
                    if unmatched_bank.empty:
                        st.success("All bank records matched")
                    else:
                        st.dataframe(unmatched_bank)

                with tab3:
                    if matched.empty:
                        st.info("No matches found")
                    else:
                        st.dataframe(matched)
                
            except Exception as e:
                st.error(f"Error during reconciliation: {str(e)}")
                st.error("Please check your input files and try again.")

    # Add a back button to return to the homepage
    if st.button("Back to Home"):
        st.session_state.page = "home"
        st.rerun()

def reports_page():
    st.header("Reconciliation Reports History")
    
    # Debug section
    with st.expander("Debug Information", expanded=True):
        st.write(f"Reports file: {REPORTS_FILE}")
        st.write(f"File exists: {os.path.exists(REPORTS_FILE)}")
        
        if os.path.exists(REPORTS_FILE):
            try:
                disk_reports = pd.read_csv(REPORTS_FILE)
                st.write(f"Reports on disk: {len(disk_reports)}")
                st.write("Latest report:")
                st.write(disk_reports.iloc[-1] if len(disk_reports) > 0 else "No reports")
            except Exception as e:
                st.error(f"Error reading reports file: {str(e)}")
        
        st.write("Session state reports:")
        if 'all_reconciliation_reports' in st.session_state:
            st.write(f"Count: {len(st.session_state.all_reconciliation_reports)}")
            st.write(st.session_state.all_reconciliation_reports)
        else:
            st.write("No reports in session state")
    
    # Main display
    try:
        # Always load fresh from disk first
        reports_df = load_all_reports()
        
        # Update session state
        st.session_state.all_reconciliation_reports = reports_df
        
        if not reports_df.empty:
            st.subheader("All Reports")
            
            # Display each report in an expandable section
            for idx, report in reports_df.iterrows():
                with st.expander(f"{report['Provider name']} - {report['Month & Year']} - {report['Status']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Partner Statement", f"{report['Partner Statement']:,.2f}")
                        st.metric("Accuracy", report['% accuracy'])
                    with col2:
                        st.metric("Treasury Records", f"{report['Treasury Records']:,.2f}")
                        st.metric("Variance", f"{report['Variance']:,.2f}")
                    
                    st.write(f"**Transactions:** {report['# of Transactions']}")
                    st.write(f"**Date:** {report['Timestamp']}")
                    if pd.notna(report['Comments']) and report['Comments'].strip():
                        st.write("**Comments:**", report['Comments'])
            
            # Download button
            st.download_button(
                "Download All Reports",
                data=reports_df.to_csv(index=False).encode('utf-8'),
                file_name="reconciliation_reports.csv",
                mime="text/csv"
            )
        else:
            st.info("No reports found. Please perform a reconciliation and save the summary.")
            
    except Exception as e:
        st.error(f"Error loading reports: {str(e)}")
    
    if st.button("Refresh Reports"):
        st.rerun()
    
    if st.button("Back to Home"):
        st.session_state.page = "home"
        st.rerun()

# --- Main Application Logic ---
def main():
    """Main function to run the Streamlit application."""
    # Set page configuration for wider layout and title
    st.set_page_config(layout="wide", page_title="Treasury Reconciliation App")

    # Removed the custom CSS for a simpler design
    # st.markdown(""" ... </style> """, unsafe_allow_html=True)

    # Initialize session state variables for page navigation and data storage
    # Session state persists values across reruns, crucial for multi-page behavior
    if "page" not in st.session_state:
        st.session_state.page = "home"
    if "selected_bank" not in st.session_state:
        st.session_state.selected_bank = ""
    if "unmatched_internal_df" not in st.session_state:
        st.session_state.unmatched_internal_df = None
    if "unmatched_bank_df" not in st.session_state:
        st.session_state.unmatched_bank_df = None
    # --- Initialize session state for all reconciliation reports ---
    if 'all_reconciliation_reports' not in st.session_state:
        st.session_state.all_reconciliation_reports = load_all_reports()

    # Sidebar navigation for easy access to different sections
    with st.sidebar:
        st.title("Navigation")
        if st.button("Home"):
            st.session_state.page = "home"
            st.session_state.selected_bank = "" # Reset bank selection
            st.rerun()
        if st.button("Reconciliation"):
            # If a bank is already selected, go directly to reconciliation.
            # Otherwise, redirect to home to select one.
            if st.session_state.selected_bank:
                st.session_state.page = "reconciliation"
            else:
                st.warning("Please select a bank from the Home page first to proceed to Reconciliation.")
                st.session_state.page = "home" # Ensure user goes to home to select
            st.rerun()
        if st.button("Reports"):
            st.session_state.page = "reports"
            st.rerun()

    # Render the appropriate page based on the current session state
    if st.session_state.page == "home":
        homepage()
    elif st.session_state.page == "reconciliation":
        # Ensure a bank is selected before rendering the reconciliation page content
        if st.session_state.selected_bank:
            reconciliation_page()
        else:
            # This case should ideally be caught by the sidebar navigation, but as a fallback
            st.warning("No bank selected for reconciliation. Please go back to the Home page to select one.")
            homepage() # Display homepage if reconciliation is attempted without a selection
    elif st.session_state.page == "reports":
        reports_page()

# Entry point for the Streamlit application
if __name__ == "__main__":
    main()
