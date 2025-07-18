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
import msoffcrypto

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
    "Kenya": ["Pesaswap", "Zamupay PYCS", "Equity KE", "Cellulant KE", "Mpesa KE", "I&M KES", "I&M USD Sending", "I&M USD Receiving", "NCBA KES", "NCBA USD"],
    "Tanzania": ["NMB", "UBA", "Mpesa TZ", "Selcom TZ", "CRDB TZS", "CRDB USD", "Equity TZ", "Cellulant TZ", "I&M TZS", "I&M USD (TZ)"],
    "Uganda": ["Pegasus", "Flutterwave Ug", "Equity UGX", "Equity Ug USD"],
    "Ghana": ["Flutterwave GHS", "Fincra GHS", "Zeepay"],
    "Senegal & Côte d'Ivoire XOF": ["Aza Finance XOF", "Hub2 IC", "Hub2 SEN"],
    "Rwanda": ["I&M RWF", "I&M USD (RWF)", "Kremit", "Flutterwave RWF"],
    "Cameroon XAF": ["Peex", "Pawapay", "Aza Finance XAF", "Hub2 XAF"],
    "Nigeria": ["Moniepoint", "Verto", "Cellulant NGN", "Flutterwave NGN", "Fincra NGN", "Zenith"]
}

MONTH_FILTER_BANKS = ["Verto", "Moniepoint"] # Needing month filter

def decrypt_excel(file_obj, password):
    file_obj.seek(0)
    decrypted_file = BytesIO()
    office_file = msoffcrypto.OfficeFile(file_obj)
    office_file.load_key(password=str(138362))
    office_file.decrypt(decrypted_file)
    decrypted_file.seek(0)
    return decrypted_file

def check_duplicates(internal_df, bank_name):
    """Check for duplicate amounts in internal records and return them with comments."""
    duplicates = internal_df[internal_df.duplicated(['COMMENT', 'AMOUNT'], keep=False)].copy()
    
    if not duplicates.empty:
        duplicates = duplicates.sort_values('AMOUNT')
        duplicates['Bank'] = bank_name
        # Select relevant columns
        duplicates = duplicates[['TRANSFER_DATE', 'AMOUNT', 'COMMENT', 'Bank', 'RECEIVER_NAME']]
        duplicates.columns = ['Date', 'Amount', 'Comment', 'Bank', 'RECEIVER_NAME']
        return duplicates
    return pd.DataFrame()

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
    
# --- Helper Functions for Duplicate Checking ---
def highlight_duplicates(df):
    """Highlight duplicate amounts in a DataFrame."""
    duplicates = df.duplicated(['Amount'], keep=False)
    return df.style.apply(lambda x: ['background: yellow' if duplicates[i] else '' for i in range(len(df))])

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
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
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
        "Currency": extracted_currency,
        "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
    }

    # --- 7. Return the results ---
    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_zamupay(internal_file_obj, bank_file_obj):
    """
    Performs comprehensive reconciliation for Zamupay (PYCS).
    Incorporates exact match, 3-day date tolerance, and split transaction aggregation.
    Expects internal_file_obj (CSV) and bank_file_obj (CSV with header=2).
    Returns matched_total, final_unmatched_internal, final_unmatched_bank dataframes,
    and a summary dictionary.
    """
    try:
        zamupay_internal_df = read_uploaded_file(internal_file_obj, header=0)
        zamupay_bank_df = read_uploaded_file(bank_file_obj, header=2)  # Changed to header=2

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


    # --- 2. Preprocessing for Zamupay Bank Statements (New Format) ---
    zamupay_bank_df.columns = zamupay_bank_df.columns.astype(str).str.strip()

    # Essential columns check for bank statements
    bank_required_cols = ['Value Date', 'Amount', 'Transaction Type']
    if not all(col in zamupay_bank_df.columns for col in bank_required_cols):
        missing_cols = [col for col in bank_required_cols if col not in zamupay_bank_df.columns]
        st.error(f"Bank statement (Zamupay) is missing essential columns: {', '.join(missing_cols)}.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), {}

    # Filter for Credit transactions only
    zamupay_bank_df = zamupay_bank_df[
        zamupay_bank_df['Transaction Type'].astype(str).str.upper() == 'CREDIT'
    ].copy()

    zamupay_bank_df = zamupay_bank_df.rename(columns={
        'Value Date': 'Date',
        'Amount': 'Amount',
        'Reference': 'Description'
    })
    
    zamupay_bank_df['Date'] = pd.to_datetime(zamupay_bank_df['Date'], errors='coerce')
    zamupay_bank_df = zamupay_bank_df.dropna(subset=['Date']).copy()

    # Clean amount column - remove commas and convert to float
    zamupay_bank_df['Amount'] = (
        zamupay_bank_df['Amount'].astype(str)
        .str.replace(',', '', regex=False)
        .astype(float)
    )

    # Filter positive amounts
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

def reconcile_pesaswap(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Pesaswap Kenya with enhanced file handling.
    Supports both .xlsx and .xls formats with multiple engine attempts.
    """
    # Initialize empty DataFrames
    matched_transactions = pd.DataFrame(columns=[
        'Date_Internal', 'Amount_Internal', 'ID_Internal',
        'Date_Bank', 'Amount_Bank', 'ID_Bank',
        'Amount_Rounded'
    ])
    unmatched_internal = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    unmatched_bank = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    summary = {}

    try:
        # --- 1. Load internal records ---
        internal_file_obj.seek(0)  # Ensure file pointer is at the start
        pesaswap_hex_df = read_uploaded_file(internal_file_obj, header=0)
        if pesaswap_hex_df is None:
            st.error("Failed to load internal records file.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Load bank statement with multiple attempts ---
        pesaswap_bank_df = None
        file_name = bank_file_obj.name.lower() if hasattr(bank_file_obj, 'name') else "unknown"
        
        # Decrypt the bank file first
        try:
            decrypted_bank_file = decrypt_excel(bank_file_obj, password='YourPasswordHere')
        except Exception as e:
            st.error(f"Failed to decrypt bank file: {e}")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Now try loading with different engines
        for engine in ['openpyxl']:
            for header_row in [15]:
                try:
                    decrypted_bank_file.seek(0)
                    pesaswap_bank_df = pd.read_excel(
                        decrypted_bank_file,
                        engine=engine,
                        header=header_row
                    )
                    # Validate that we have the required columns
                    if 'Transaction Date' in pesaswap_bank_df.columns and 'Credit Amount' in pesaswap_bank_df.columns:
                        st.success(f"Bank file loaded successfully with engine={engine}, header={header_row}")
                        break
                except Exception as e:
                    continue
            if pesaswap_bank_df is not None:
                break

        if pesaswap_bank_df is None:
            st.error("Could not load bank statement with any engine/header combination.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 3. Preprocessing for internal records ---
        pesaswap_hex_df.columns = pesaswap_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in pesaswap_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in pesaswap_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        pesaswap_hex_df = pesaswap_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        pesaswap_hex_df['Date'] = pd.to_datetime(pesaswap_hex_df['Date'], errors='coerce')
        pesaswap_hex_df = pesaswap_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        pesaswap_hex_df_recon = pesaswap_hex_df[pesaswap_hex_df['Amount'] > 0].copy()
        pesaswap_hex_df_recon['Date_Match'] = pesaswap_hex_df_recon['Date'].dt.date
        pesaswap_hex_df_recon['Amount_Rounded'] = pesaswap_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "KES"  # Default for Pesaswap Kenya
        if 'CURRENCY' in pesaswap_hex_df.columns and not pesaswap_hex_df['CURRENCY'].empty:
            unique_currencies = pesaswap_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 4. Preprocessing for bank statements (Pesaswap Specific) ---
        pesaswap_bank_df.columns = pesaswap_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Transaction Date', 'Credit Amount', 'Transaction Details']
        if not all(col in pesaswap_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in pesaswap_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        pesaswap_bank_df = pesaswap_bank_df.rename(columns={
            'Transaction Date': 'Date',
            'Credit Amount': 'Credit',
            'Transaction Details': 'Description',
            'Transaction ID': 'ID'
        })

        # Convert dates
        pesaswap_bank_df['Date'] = pd.to_datetime(pesaswap_bank_df['Date'], errors='coerce')
        pesaswap_bank_df = pesaswap_bank_df.dropna(subset=['Date']).copy()

        # Pesaswap Specific Filters
        # 1. Filter for positive credits only
        pesaswap_bank_df['Credit'] = pd.to_numeric(
            pesaswap_bank_df['Credit'].astype(str).str.replace(',', '', regex=False),
            errors='coerce'
        ).fillna(0)
        pesaswap_bank_df = pesaswap_bank_df[pesaswap_bank_df['Credit'] > 0].copy()

        # 2. Filter for transactions containing 'Nala' in Description
        pesaswap_bank_df = pesaswap_bank_df[
            pesaswap_bank_df['Description'].astype(str).str.contains('Nala', case=False, na=False)
        ].copy()

        if pesaswap_bank_df.empty:
            st.warning("No bank records found after applying Pesaswap filters (positive credits with 'Nala' in description).")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        pesaswap_bank_df['Amount'] = pesaswap_bank_df['Credit']
        pesaswap_bank_df_recon = pesaswap_bank_df[['Date', 'Amount', 'Description', 'ID']].copy()
        pesaswap_bank_df_recon['Date_Match'] = pesaswap_bank_df_recon['Date'].dt.date
        pesaswap_bank_df_recon['Amount_Rounded'] = pesaswap_bank_df_recon['Amount'].round(2)

        # --- 5. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = pesaswap_hex_df_recon['Amount'].sum()
        total_bank_credits = pesaswap_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 6. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            pesaswap_hex_df_recon.assign(Source_Internal='Internal'),
            pesaswap_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()

        # --- 7. Prepare initially unmatched records for tolerance matching ---
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[[
                'Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded'
            ]].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[[
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ]].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 8. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance = pd.DataFrame()
        final_unmatched_internal = unmatched_internal_initial.copy()
        final_unmatched_bank = unmatched_bank_initial.copy()

        if not unmatched_internal_initial.empty and not unmatched_bank_initial.empty:
            st.info("Attempting date tolerance matching for remaining unmatched records (Pesaswap)...")
            matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = \
                perform_date_tolerance_matching(
                    unmatched_internal_initial,
                    unmatched_bank_initial,
                    tolerance_days=3
                )

        # --- 9. Combine all matched records ---
        matched_total = pd.concat([matched_exact, matched_with_tolerance], ignore_index=True)

        # --- 10. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Total Internal Records (for recon)": len(pesaswap_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(pesaswap_bank_df_recon),
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
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during Pesaswap reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary       

def reconcile_mpesa_ke(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for M-Pesa Kenya.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=6).
    Filters for credit transactions where 'Paid In' > 0.
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
        # --- 1. Load the datasets ---
        mpesa_hex_df = read_uploaded_file(internal_file_obj, header=0)
        mpesa_bank_df = read_uploaded_file(bank_file_obj, header=6)
        
        if mpesa_hex_df is None or mpesa_bank_df is None:
            st.error("One or both files could not be loaded for M-Pesa KE.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        mpesa_hex_df.columns = mpesa_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in mpesa_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in mpesa_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        mpesa_hex_df = mpesa_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        mpesa_hex_df['Date'] = pd.to_datetime(mpesa_hex_df['Date'], errors='coerce')
        mpesa_hex_df = mpesa_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        mpesa_hex_df_recon = mpesa_hex_df[mpesa_hex_df['Amount'] > 0].copy()
        mpesa_hex_df_recon['Date_Match'] = mpesa_hex_df_recon['Date'].dt.date
        mpesa_hex_df_recon['Amount_Rounded'] = mpesa_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "KES"  # Default for M-Pesa Kenya
        if 'CURRENCY' in mpesa_hex_df.columns and not mpesa_hex_df['CURRENCY'].empty:
            unique_currencies = mpesa_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (M-Pesa KE Specific) ---
        mpesa_bank_df.columns = mpesa_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Completion Time', 'Paid In', 'Details']
        if not all(col in mpesa_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in mpesa_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Rename columns
        mpesa_bank_df = mpesa_bank_df.rename(columns={
            'Completion Time': 'Date',
            'Paid In': 'Credit',
            'Details': 'Description',
            'Receipt No.': 'ID'
        })

        # Convert date - M-Pesa KE format is DD-MM-YYYY HH:MM:SS
        mpesa_bank_df['Date'] = pd.to_datetime(mpesa_bank_df['Date'], dayfirst=True, errors='coerce')
        mpesa_bank_df = mpesa_bank_df.dropna(subset=['Date']).copy()

        # Clean and convert amount (remove commas)
        mpesa_bank_df['Credit'] = (
            mpesa_bank_df['Credit'].astype(str)
            .str.replace(',', '', regex=False)
            .replace('', '0')
            .astype(float)
        )

        # Filter for positive credits and Nala transactions
        mpesa_bank_df_recon = mpesa_bank_df[
            (mpesa_bank_df['Credit'] > 0) &
            (mpesa_bank_df['Description'].str.contains('NALA', case=False, na=False))
        ].copy()

        if mpesa_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for NALA transactions.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        mpesa_bank_df_recon['Amount'] = mpesa_bank_df_recon['Credit']
        mpesa_bank_df_recon = mpesa_bank_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        mpesa_bank_df_recon['Date_Match'] = mpesa_bank_df_recon['Date'].dt.date
        mpesa_bank_df_recon['Amount_Rounded'] = mpesa_bank_df_recon['Amount'].round(2)

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = mpesa_hex_df_recon['Amount'].sum()
        total_bank_credits = mpesa_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            mpesa_hex_df_recon.assign(Source_Internal='Internal'),
            mpesa_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[[
                'Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded'
            ]].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[[
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ]].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance = pd.DataFrame()
        final_unmatched_internal = unmatched_internal_initial.copy()
        final_unmatched_bank = unmatched_bank_initial.copy()

        if not unmatched_internal_initial.empty and not unmatched_bank_initial.empty:
            st.info("Attempting date tolerance matching for remaining unmatched records (M-Pesa KE)...")
            matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = \
                perform_date_tolerance_matching(
                    unmatched_internal_initial,
                    unmatched_bank_initial,
                    tolerance_days=3
                )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Total Internal Records (for recon)": len(mpesa_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(mpesa_bank_df_recon),
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
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during M-Pesa KE reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary  

def reconcile_i_and_m_kes(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for I&M Bank Kenya (KES).
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=12).
    Filters for withdrawal transactions matching specific narration patterns and excludes:
    - Transactions containing 'charge' in description
    - Transactions containing 'excise duty' in description
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = create_empty_matched_df()
    unmatched_internal = create_empty_unmatched_df()
    unmatched_bank = create_empty_unmatched_df()
    summary = {}

    try:
        # --- 1. Load the datasets ---
        im_hex_df = read_uploaded_file(internal_file_obj, header=0)
        im_bank_df = read_uploaded_file(bank_file_obj, header=12)

        if im_hex_df is None or im_bank_df is None:
            st.error("One or both files could not be loaded for I&M KES.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        im_hex_df.columns = im_hex_df.columns.astype(str).str.strip()

        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in im_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in im_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_hex_df = im_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date', 'AMOUNT': 'Amount',
            'COMMENT': 'Description', 'TRANSFER_ID': 'ID'
        })
        if 'ID' not in im_hex_df.columns: im_hex_df['ID'] = 'Internal_' + im_hex_df.index.astype(str)


        im_hex_df['Date'] = pd.to_datetime(im_hex_df['Date'], errors='coerce')
        im_hex_df = im_hex_df.dropna(subset=['Date']).copy()

        im_hex_df_recon = im_hex_df[im_hex_df['Amount'] > 0].copy()
        im_hex_df_recon['Date_Match'] = im_hex_df_recon['Date'].dt.date
        im_hex_df_recon['Amount_Rounded'] = im_hex_df_recon['Amount'].round(2)

        extracted_currency = "KES"
        if 'CURRENCY' in im_hex_df.columns and not im_hex_df['CURRENCY'].empty:
            unique_currencies = im_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (I&M KE Specific) ---
        im_bank_df.columns = im_bank_df.columns.astype(str).str.strip()

        bank_required_cols = ['Transaction Date', 'Value Date', 'Description / Narration',
                            'Transaction Reference', 'Withdrawal', 'Deposit', 'Balance']
        if not all(col in im_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in im_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_bank_df = im_bank_df.rename(columns={
            'Transaction Date': 'Transaction_Date', 'Value Date': 'Value_Date',
            'Description / Narration': 'Narration', 'Transaction Reference': 'ID',
            'Withdrawal': 'Withdrawal', 'Deposit': 'Deposit', 'Balance': 'Balance'
        })

        im_bank_df['Date'] = pd.to_datetime(im_bank_df['Value_Date'], dayfirst=True, errors='coerce')
        im_bank_df['Date'] = im_bank_df['Date'].fillna(
            pd.to_datetime(im_bank_df['Transaction_Date'], dayfirst=True, errors='coerce')
        )
        im_bank_df = im_bank_df.dropna(subset=['Date']).copy()

        im_bank_df['Withdrawal'] = pd.to_numeric(
            im_bank_df['Withdrawal'].astype(str).str.replace(',', '', regex=False).replace('', '0'),
            errors='coerce'
        ).fillna(0)

        narration_patterns = [
            'Nala transfer to', 'Nala fund Pesaswap', 'I and m transfer to',
            'nala fund zamupay', 'transfer to'
        ]
        pattern = '|'.join([re.escape(phrase) for phrase in narration_patterns])
        regex_pattern = re.compile(pattern, flags=re.IGNORECASE)

        im_bank_df_recon = im_bank_df[
            (im_bank_df['Withdrawal'] > 0) &
            (im_bank_df['Narration'].astype(str).str.contains(regex_pattern, na=False)) &
            (~im_bank_df['Narration'].astype(str).str.contains('charge', case=False, na=False)) &
            (~im_bank_df['Narration'].astype(str).str.contains('excise duty', case=False, na=False))
        ].copy()

        if im_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for specified narration patterns.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_bank_df_recon['Amount'] = im_bank_df_recon['Withdrawal']
        im_bank_df_recon = im_bank_df_recon[['Date', 'Amount', 'Narration', 'ID']].copy()
        im_bank_df_recon['Date_Match'] = im_bank_df_recon['Date'].dt.date
        im_bank_df_recon['Amount_Rounded'] = im_bank_df_recon['Amount'].round(2)

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = im_hex_df_recon['Amount'].sum()
        total_bank_withdrawals = im_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_withdrawals

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            im_hex_df_recon.assign(Source_Internal='Internal'),
            im_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'}).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = create_empty_unmatched_df()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'}).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = create_empty_unmatched_df()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        summary = {
            "Total Internal Records (for recon)": len(im_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(im_bank_df_recon),
            "Total Internal Credits (Original)": total_internal_credits,
            "Total Bank Credits (Original)": total_bank_withdrawals, # Note: using bank withdrawals here
            "Overall Discrepancy (Original)": discrepancy_amount,
            "Total Matched Transactions (All Stages)": len(matched_total),
            "Unmatched Internal Records (Final)": len(final_unmatched_internal),
            "Unmatched Bank Records (Final)": len(final_unmatched_bank),
            "Unmatched Internal Amount (Final)": final_unmatched_internal['Amount'].sum(),
            "Unmatched Bank Amount (Final)": final_unmatched_bank['Amount'].sum(),
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_withdrawals / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during I&M KES reconciliation processing: {str(e)}")
        return create_empty_matched_df(), create_empty_unmatched_df(), create_empty_unmatched_df(), {}

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_i_and_m_usd_ke_sending(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for I&M Bank Kenya (USD).
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=12).
    Filters for withdrawal transactions matching specific narration patterns and excludes:
    - Transactions containing 'charger' in description
    - Transactions containing 'excise duty' in description
    - Transactions containing 'tt' in description
    Includes only transactions with:
    - 'transfer to' in description
    - 'buy usd/kes' in description
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = create_empty_matched_df()
    unmatched_internal = create_empty_unmatched_df()
    unmatched_bank = create_empty_unmatched_df()
    summary = {}

    try:
        # --- 1. Load the datasets ---
        im_usd_hex_df = read_uploaded_file(internal_file_obj, header=0)
        im_usd_bank_df = read_uploaded_file(bank_file_obj, header=12)

        if im_usd_hex_df is None or im_usd_bank_df is None:
            st.error("One or both files could not be loaded for I&M USD KE.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        im_usd_hex_df.columns = im_usd_hex_df.columns.astype(str).str.strip()

        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in im_usd_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in im_usd_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_usd_hex_df = im_usd_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date', 
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        im_usd_hex_df['Date'] = pd.to_datetime(im_usd_hex_df['Date'], errors='coerce')
        im_usd_hex_df = im_usd_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        im_usd_hex_df_recon = im_usd_hex_df[im_usd_hex_df['Amount'] > 0].copy()
        im_usd_hex_df_recon['Date_Match'] = im_usd_hex_df_recon['Date'].dt.date
        im_usd_hex_df_recon['Amount_Rounded'] = im_usd_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "USD"
        if 'CURRENCY' in im_usd_hex_df.columns and not im_usd_hex_df['CURRENCY'].empty:
            unique_currencies = im_usd_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (I&M USD KE Specific) ---
        im_usd_bank_df.columns = im_usd_bank_df.columns.astype(str).str.strip()

        bank_required_cols = ['Transaction Date', 'Value Date', 'Description / Narration',
                            'Transaction Reference', 'Withdrawal', 'Deposit', 'Balance']
        if not all(col in im_usd_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in im_usd_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_usd_bank_df = im_usd_bank_df.rename(columns={
            'Transaction Date': 'Transaction_Date', 
            'Value Date': 'Value_Date',
            'Description / Narration': 'Narration',
            'Transaction Reference': 'ID',
            'Withdrawal': 'Withdrawal',
            'Deposit': 'Deposit',
            'Balance': 'Balance'
        })

        # Use Value Date if available, otherwise Transaction Date
        im_usd_bank_df['Date'] = pd.to_datetime(im_usd_bank_df['Value_Date'], dayfirst=True, errors='coerce')
        im_usd_bank_df['Date'] = im_usd_bank_df['Date'].fillna(
            pd.to_datetime(im_usd_bank_df['Transaction_Date'], dayfirst=True, errors='coerce')
        )
        im_usd_bank_df = im_usd_bank_df.dropna(subset=['Date']).copy()

        # Clean withdrawal amounts
        im_usd_bank_df['Withdrawal'] = pd.to_numeric(
            im_usd_bank_df['Withdrawal'].astype(str).str.replace(',', '', regex=False).replace('', '0'),
            errors='coerce'
        ).fillna(0)

        # I&M USD KE Specific Filters
        include_patterns = ['transfer to', 'buy usd/kes']
        exclude_patterns = ['charge', 'excise duty', 'tt']
        
        # Create regex patterns
        include_regex = '|'.join([re.escape(pattern) for pattern in include_patterns])
        exclude_regex = '|'.join([re.escape(pattern) for pattern in exclude_patterns])
        
        im_usd_bank_df_recon = im_usd_bank_df[
            (im_usd_bank_df['Withdrawal'] > 0) &
            (im_usd_bank_df['Narration'].astype(str).str.contains(include_regex, case=False, na=False)) &
            (~im_usd_bank_df['Narration'].astype(str).str.contains(exclude_regex, case=False, na=False))
        ].copy()

        if im_usd_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for specified narration patterns.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_usd_bank_df_recon['Amount'] = im_usd_bank_df_recon['Withdrawal']
        im_usd_bank_df_recon = im_usd_bank_df_recon[['Date', 'Amount', 'Narration', 'ID']].copy()
        im_usd_bank_df_recon['Date_Match'] = im_usd_bank_df_recon['Date'].dt.date
        im_usd_bank_df_recon['Amount_Rounded'] = im_usd_bank_df_recon['Amount'].round(2)

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = im_usd_hex_df_recon['Amount'].sum()
        total_bank_withdrawals = im_usd_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_withdrawals

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            im_usd_hex_df_recon.assign(Source_Internal='Internal'),
            im_usd_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'}).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = create_empty_unmatched_df()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'}).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = create_empty_unmatched_df()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        summary = {
            "Total Internal Records (for recon)": len(im_usd_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(im_usd_bank_df_recon),
            "Total Internal Credits (Original)": total_internal_credits,
            "Total Bank Credits (Original)": total_bank_withdrawals,  # Using bank withdrawals here
            "Overall Discrepancy (Original)": discrepancy_amount,
            "Total Matched Transactions (All Stages)": len(matched_total),
            "Unmatched Internal Records (Final)": len(final_unmatched_internal),
            "Unmatched Bank Records (Final)": len(final_unmatched_bank),
            "Unmatched Internal Amount (Final)": final_unmatched_internal['Amount'].sum(),
            "Unmatched Bank Amount (Final)": final_unmatched_bank['Amount'].sum(),
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_withdrawals / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during I&M USD KE reconciliation processing: {str(e)}")
        return create_empty_matched_df(), create_empty_unmatched_df(), create_empty_unmatched_df(), {}

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_i_and_m_usd_ke_receiving(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for I&M Bank Kenya (USD).
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=12).
    Filters for withdrawal transactions matching specific narration patterns and excludes:
    - Transactions containing 'charger' in description
    - Transactions containing 'excise duty' in description
    - Transactions containing 'tt' in description
    Includes only transactions with:
    - 'transfer to' in description
    - 'buy usd/kes' in description
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = create_empty_matched_df()
    unmatched_internal = create_empty_unmatched_df()
    unmatched_bank = create_empty_unmatched_df()
    summary = {}

    try:
        # --- 1. Load the datasets ---
        im_usd_hex_df = read_uploaded_file(internal_file_obj, header=0)
        im_usd_bank_df = read_uploaded_file(bank_file_obj, header=12)

        if im_usd_hex_df is None or im_usd_bank_df is None:
            st.error("One or both files could not be loaded for I&M USD KE.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        im_usd_hex_df.columns = im_usd_hex_df.columns.astype(str).str.strip()

        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in im_usd_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in im_usd_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_usd_hex_df = im_usd_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date', 
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        im_usd_hex_df['Date'] = pd.to_datetime(im_usd_hex_df['Date'], errors='coerce')
        im_usd_hex_df = im_usd_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        im_usd_hex_df_recon = im_usd_hex_df[im_usd_hex_df['Amount'] > 0].copy()
        im_usd_hex_df_recon['Date_Match'] = im_usd_hex_df_recon['Date'].dt.date
        im_usd_hex_df_recon['Amount_Rounded'] = im_usd_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "USD"
        if 'CURRENCY' in im_usd_hex_df.columns and not im_usd_hex_df['CURRENCY'].empty:
            unique_currencies = im_usd_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (I&M USD KE Specific) ---
        im_usd_bank_df.columns = im_usd_bank_df.columns.astype(str).str.strip()

        bank_required_cols = ['Transaction Date', 'Value Date', 'Description / Narration',
                            'Transaction Reference', 'Withdrawal', 'Deposit', 'Balance']
        if not all(col in im_usd_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in im_usd_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_usd_bank_df = im_usd_bank_df.rename(columns={
            'Transaction Date': 'Transaction_Date', 
            'Value Date': 'Value_Date',
            'Description / Narration': 'Narration',
            'Transaction Reference': 'ID',
            'Withdrawal': 'Withdrawal',
            'Deposit': 'Deposit',
            'Balance': 'Balance'
        })

        # Use Value Date if available, otherwise Transaction Date
        im_usd_bank_df['Date'] = pd.to_datetime(im_usd_bank_df['Value_Date'], dayfirst=True, errors='coerce')
        im_usd_bank_df['Date'] = im_usd_bank_df['Date'].fillna(
            pd.to_datetime(im_usd_bank_df['Transaction_Date'], dayfirst=True, errors='coerce')
        )
        im_usd_bank_df = im_usd_bank_df.dropna(subset=['Date']).copy()

        # Clean Deposit amounts
        im_usd_bank_df['Deposit'] = pd.to_numeric(
            im_usd_bank_df['Deposit'].astype(str).str.replace(',', '', regex=False).replace('', '0'),
            errors='coerce'
        ).fillna(0)

        # I&M USD KE Specific Filters
        include_patterns = ['tt bo']
        exclude_patterns = ['charge', 'excise duty']
        
        # Create regex patterns
        include_regex = '|'.join([re.escape(pattern) for pattern in include_patterns])
        exclude_regex = '|'.join([re.escape(pattern) for pattern in exclude_patterns])
        
        im_usd_bank_df_recon = im_usd_bank_df[
            (im_usd_bank_df['Deposit'] > 0) &
            (im_usd_bank_df['Narration'].astype(str).str.contains(include_regex, case=False, na=False)) &
            (~im_usd_bank_df['Narration'].astype(str).str.contains(exclude_regex, case=False, na=False))
        ].copy()

        if im_usd_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for specified narration patterns.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_usd_bank_df_recon['Amount'] = im_usd_bank_df_recon['Deposit']
        im_usd_bank_df_recon = im_usd_bank_df_recon[['Date', 'Amount', 'Narration', 'ID']].copy()
        im_usd_bank_df_recon['Date_Match'] = im_usd_bank_df_recon['Date'].dt.date
        im_usd_bank_df_recon['Amount_Rounded'] = im_usd_bank_df_recon['Amount'].round(2)

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = im_usd_hex_df_recon['Amount'].sum()
        total_bank_withdrawals = im_usd_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_withdrawals

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            im_usd_hex_df_recon.assign(Source_Internal='Internal'),
            im_usd_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'}).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = create_empty_unmatched_df()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'}).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = create_empty_unmatched_df()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        summary = {
            "Total Internal Records (for recon)": len(im_usd_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(im_usd_bank_df_recon),
            "Total Internal Credits (Original)": total_internal_credits,
            "Total Bank Credits (Original)": total_bank_withdrawals,  # Using bank withdrawals here
            "Overall Discrepancy (Original)": discrepancy_amount,
            "Total Matched Transactions (All Stages)": len(matched_total),
            "Unmatched Internal Records (Final)": len(final_unmatched_internal),
            "Unmatched Bank Records (Final)": len(final_unmatched_bank),
            "Unmatched Internal Amount (Final)": final_unmatched_internal['Amount'].sum(),
            "Unmatched Bank Amount (Final)": final_unmatched_bank['Amount'].sum(),
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_withdrawals / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during I&M USD KE reconciliation processing: {str(e)}")
        return create_empty_matched_df(), create_empty_unmatched_df(), create_empty_unmatched_df(), {}

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_ncba_kes(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for NCBA Kenya (KES).
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=17).
    Filters for credit transactions where 'Credit' > 0 after converting '-' to 0.
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
        # --- 1. Load the datasets ---
        ncba_hex_df = read_uploaded_file(internal_file_obj, header=0)
        ncba_bank_df = read_uploaded_file(bank_file_obj, header=17)  # Header at row 18 (0-indexed 17)
        
        if ncba_hex_df is None or ncba_bank_df is None:
            st.error("One or both files could not be loaded for NCBA KES.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        ncba_hex_df.columns = ncba_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in ncba_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in ncba_hex_df.columns]
            st.error(f"Internal records (NCBA Hex) are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        ncba_hex_df = ncba_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        ncba_hex_df['Date'] = pd.to_datetime(ncba_hex_df['Date'], errors='coerce')
        ncba_hex_df = ncba_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        ncba_hex_df_recon = ncba_hex_df[ncba_hex_df['Amount'] > 0].copy()
        ncba_hex_df_recon['Date_Match'] = ncba_hex_df_recon['Date'].dt.date
        ncba_hex_df_recon['Amount_Rounded'] = ncba_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "KES"
        if 'CURRENCY' in ncba_hex_df.columns and not ncba_hex_df['CURRENCY'].empty:
            unique_currencies = ncba_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (NCBA KE Specific) ---
        ncba_bank_df.columns = ncba_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Transaction Date', 'Credit', 'Reference Number']
        if not all(col in ncba_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in ncba_bank_df.columns]
            st.error(f"Bank statement (NCBA KE) is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        ncba_bank_df = ncba_bank_df.rename(columns={
            'Transaction Date': 'Date',
            'Credit': 'Credit',
            'Reference Number': 'ID',
            'Description': 'Description'
        })

        # Convert dates - NCBA format is YYYY-MM-DD
        ncba_bank_df['Date'] = pd.to_datetime(ncba_bank_df['Date'], errors='coerce')
        ncba_bank_df = ncba_bank_df.dropna(subset=['Date']).copy()

        # Clean credit amounts - replace '-' with 0 and convert to numeric
        ncba_bank_df['Credit'] = ncba_bank_df['Credit'].replace('-', '0').astype(str)
        ncba_bank_df['Credit'] = (
            ncba_bank_df['Credit'].str.replace(',', '', regex=False)
            .astype(float)
            .fillna(0)
        )

        # Filter for positive credits
        ncba_bank_df_recon = ncba_bank_df[ncba_bank_df['Credit'] > 0].copy()
        ncba_bank_df_recon['Amount'] = ncba_bank_df_recon['Credit']
        ncba_bank_df_recon = ncba_bank_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        ncba_bank_df_recon['Date_Match'] = ncba_bank_df_recon['Date'].dt.date
        ncba_bank_df_recon['Amount_Rounded'] = ncba_bank_df_recon['Amount'].round(2)

        if ncba_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for positive credits.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = ncba_hex_df_recon['Amount'].sum()
        total_bank_credits = ncba_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            ncba_hex_df_recon.assign(Source_Internal='Internal'),
            ncba_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Total Internal Records (for recon)": len(ncba_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(ncba_bank_df_recon),
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
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during NCBA KES reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_ncba_usd(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for NCBA Bank USD accounts with enhanced matching:
    1. Exact matching (same date + amount)
    2. Date tolerance matching (±3 days)
    3. Aggregated matching (sum of credits by date)
    
    Bank statement processing:
    - Uses header=17
    - Handles '-' in Credit column by converting to 0
    - Converts Credit amounts to numeric values
    - Filters for positive credits (>0) with Description containing 'Money remittance imited'
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = pd.DataFrame(columns=[
        'Date_Internal', 'Amount_Internal', 'ID_Internal',
        'Date_Bank', 'Amount_Bank', 'ID_Bank',
        'Amount_Rounded', 'Match_Type'
    ])
    unmatched_internal = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    unmatched_bank = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    summary = {}

    try:
        # --- 1. Load the datasets ---
        ncba_hex_df = read_uploaded_file(internal_file_obj, header=0)
        ncba_bank_df = read_uploaded_file(bank_file_obj, header=17)
        
        if ncba_hex_df is None or ncba_bank_df is None:
            st.error("One or both files could not be loaded for NCBA USD.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        ncba_hex_df.columns = ncba_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in ncba_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in ncba_hex_df.columns]
            st.error(f"Internal records (NCBA Hex) are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        ncba_hex_df = ncba_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        ncba_hex_df['Date'] = pd.to_datetime(ncba_hex_df['Date'], errors='coerce')
        ncba_hex_df = ncba_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        ncba_hex_df_recon = ncba_hex_df[ncba_hex_df['Amount'] > 0].copy()
        ncba_hex_df_recon['Date_Match'] = ncba_hex_df_recon['Date'].dt.date
        ncba_hex_df_recon['Amount_Rounded'] = ncba_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "USD"
        if 'CURRENCY' in ncba_hex_df.columns and not ncba_hex_df['CURRENCY'].empty:
            unique_currencies = ncba_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (NCBA USD Specific) ---
        ncba_bank_df.columns = ncba_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Transaction Date', 'Credit', 'Description', 'Reference Number']
        if not all(col in ncba_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in ncba_bank_df.columns]
            st.error(f"Bank statement (NCBA USD) is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        ncba_bank_df = ncba_bank_df.rename(columns={
            'Transaction Date': 'Date',
            'Credit': 'Credit',
            'Reference Number': 'ID',
            'Description': 'Description'
        })

        # Convert dates - NCBA format is typically YYYY-MM-DD
        ncba_bank_df['Date'] = pd.to_datetime(ncba_bank_df['Date'], errors='coerce')
        ncba_bank_df = ncba_bank_df.dropna(subset=['Date']).copy()

        # Clean credit amounts - replace '-' with 0 and convert to numeric
        ncba_bank_df['Credit'] = ncba_bank_df['Credit'].replace('-', '0').astype(str)
        ncba_bank_df['Credit'] = (
            ncba_bank_df['Credit'].str.replace(',', '', regex=False)
            .astype(float)
            .fillna(0)
        )

        # NCBA USD Specific Filters
        # 1. Filter for positive credits
        # 2. Filter for transactions containing 'Nala money remittance imited' in Description
        ncba_bank_df_recon = ncba_bank_df[
            (ncba_bank_df['Credit'] > 0) &
            (ncba_bank_df['Description'].astype(str).str.contains('NALA MONEY REMITTANCE LIMITED', case=False, na=False))
        ].copy()

        if ncba_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for positive credits with 'Nala money remittance imited' in description.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        ncba_bank_df_recon['Amount'] = ncba_bank_df_recon['Credit']
        ncba_bank_df_recon = ncba_bank_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        ncba_bank_df_recon['Date_Match'] = ncba_bank_df_recon['Date'].dt.date
        ncba_bank_df_recon['Amount_Rounded'] = ncba_bank_df_recon['Amount'].round(2)

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = ncba_hex_df_recon['Amount'].sum()
        total_bank_credits = ncba_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            ncba_hex_df_recon.assign(Source_Internal='Internal'),
            ncba_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_exact = matched_exact[cols_to_select].copy()
            matched_exact['Match_Type'] = 'Exact'

        # Prepare initially unmatched records for next stages
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, remaining_internal_after_tolerance, remaining_bank_after_tolerance = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )
        if not matched_with_tolerance.empty:
            matched_with_tolerance['Match_Type'] = 'Tolerance'

        # --- 7. Aggregated Matching (sum of credits by date) ---
        matched_aggregated = pd.DataFrame()
        
        if not remaining_internal_after_tolerance.empty and not remaining_bank_after_tolerance.empty:
            st.info("Attempting aggregated matching by summing bank credits per date...")
            
            # Group bank transactions by date and sum amounts
            bank_aggregated = remaining_bank_after_tolerance.groupby('Date')['Amount'].sum().reset_index()
            bank_aggregated['Amount_Rounded'] = bank_aggregated['Amount'].round(2)
            
            # Merge with internal records on date and rounded amount
            temp_internal = remaining_internal_after_tolerance.copy()
            temp_internal['Amount_Rounded'] = temp_internal['Amount'].round(2)
            
            matched_aggregated = pd.merge(
                temp_internal,
                bank_aggregated,
                on=['Date', 'Amount_Rounded'],
                how='inner'
            )
            
            if not matched_aggregated.empty:
                # Get the original bank transactions that contributed to the aggregated match
                matched_bank_ids = []
                for _, row in matched_aggregated.iterrows():
                    date_match = row['Date']
                    contributing_trans = remaining_bank_after_tolerance[
                        (remaining_bank_after_tolerance['Date'] == date_match)
                    ]
                    matched_bank_ids.extend(contributing_trans['ID'].tolist())
                
                # Create the matched records
                matched_aggregated = matched_aggregated.rename(columns={
                    'Amount_x': 'Amount_Internal',
                    'Amount_y': 'Amount_Bank'
                })
                matched_aggregated['Match_Type'] = 'Aggregated'
                
                # Remove matched records from unmatched sets
                remaining_internal_after_tolerance = remaining_internal_after_tolerance[
                    ~remaining_internal_after_tolerance['ID'].isin(matched_aggregated['ID'].tolist())
                ]
                remaining_bank_after_tolerance = remaining_bank_after_tolerance[
                    ~remaining_bank_after_tolerance['ID'].isin(matched_bank_ids)
                ]

        # --- 8. Combine all matched records ---
        matched_total = pd.concat([
            matched_exact,
            matched_with_tolerance,
            matched_aggregated
        ], ignore_index=True)

        # Final unmatched records
        final_unmatched_internal = remaining_internal_after_tolerance.copy()
        final_unmatched_bank = remaining_bank_after_tolerance.copy()

        # --- 9. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Total Internal Records (for recon)": len(ncba_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(ncba_bank_df_recon),
            "Total Internal Credits (Original)": total_internal_credits,
            "Total Bank Credits (Original)": total_bank_credits,
            "Overall Discrepancy (Original)": discrepancy_amount,
            "Total Matched Transactions (All Stages)": len(matched_total),
            "Exact Matches": len(matched_exact) if not matched_exact.empty else 0,
            "Tolerance Matches": len(matched_with_tolerance) if not matched_with_tolerance.empty else 0,
            "Aggregated Matches": len(matched_aggregated) if not matched_aggregated.empty else 0,
            "Total Matched Amount (Internal)": total_matched_amount_internal,
            "Total Matched Amount (Bank)": total_matched_amount_bank,
            "Unmatched Internal Records (Final)": len(final_unmatched_internal),
            "Unmatched Bank Records (Final)": len(final_unmatched_bank),
            "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
            "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during NCBA USD reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_nmb(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for NMB Bank Tanzania.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=23).
    Filters for credit transactions where 'Credit' > 0.
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
        # --- 1. Load the datasets ---
        nmb_hex_df = read_uploaded_file(internal_file_obj, header=0)
        nmb_bank_df = read_uploaded_file(bank_file_obj, header=23)  # Header at row 24 (0-indexed 23)
        
        if nmb_hex_df is None or nmb_bank_df is None:
            st.error("One or both files could not be loaded for NMB.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        nmb_hex_df.columns = nmb_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in nmb_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in nmb_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        nmb_hex_df = nmb_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        nmb_hex_df['Date'] = pd.to_datetime(nmb_hex_df['Date'], errors='coerce')
        nmb_hex_df = nmb_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        nmb_hex_df_recon = nmb_hex_df[nmb_hex_df['Amount'] > 0].copy()
        nmb_hex_df_recon['Date_Match'] = nmb_hex_df_recon['Date'].dt.date
        nmb_hex_df_recon['Amount_Rounded'] = nmb_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "TZS"
        if 'CURRENCY' in nmb_hex_df.columns and not nmb_hex_df['CURRENCY'].empty:
            unique_currencies = nmb_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (NMB Specific) ---
        nmb_bank_df.columns = nmb_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Date', 'Credit', 'Reference Number']
        if not all(col in nmb_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in nmb_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        nmb_bank_df = nmb_bank_df.rename(columns={
            'Date': 'Date',
            'Credit': 'Credit',
            'Reference Number': 'ID',
            'Description': 'Description'
        })

        # Convert dates - NMB format is typically DD/MM/YYYY
        nmb_bank_df['Date'] = pd.to_datetime(nmb_bank_df['Date'], dayfirst=True, errors='coerce')
        nmb_bank_df = nmb_bank_df.dropna(subset=['Date']).copy()

        # Clean credit amounts - remove commas and convert to numeric
        nmb_bank_df['Credit'] = (
            nmb_bank_df['Credit'].astype(str)
            .str.replace(',', '', regex=False)
            .astype(float)
            .fillna(0)
        )

        # Filter for positive credits
        nmb_bank_df_recon = nmb_bank_df[nmb_bank_df['Credit'] > 0].copy()
        nmb_bank_df_recon['Amount'] = nmb_bank_df_recon['Credit']
        nmb_bank_df_recon = nmb_bank_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        nmb_bank_df_recon['Date_Match'] = nmb_bank_df_recon['Date'].dt.date
        nmb_bank_df_recon['Amount_Rounded'] = nmb_bank_df_recon['Amount'].round(2)

        if nmb_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for positive credits.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = nmb_hex_df_recon['Amount'].sum()
        total_bank_credits = nmb_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            nmb_hex_df_recon.assign(Source_Internal='Internal'),
            nmb_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Total Internal Records (for recon)": len(nmb_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(nmb_bank_df_recon),
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
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during NMB reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_i_and_m_tzs(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for I&M Bank Tanzania (TZS).
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=17).
    Filters for credit transactions where 'Credit' > 0.
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = create_empty_matched_df()
    unmatched_internal = create_empty_unmatched_df()
    unmatched_bank = create_empty_unmatched_df()
    summary = {}

    try:
        # --- 1. Load the datasets ---
        im_tz_hex_df = read_uploaded_file(internal_file_obj, header=0)
        im_tz_bank_df = read_uploaded_file(bank_file_obj, header=17)
        
        if im_tz_hex_df is None or im_tz_bank_df is None:
            st.error("One or both files could not be loaded for I&M TZS.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        im_tz_hex_df.columns = im_tz_hex_df.columns.astype(str).str.strip()

        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in im_tz_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in im_tz_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_tz_hex_df = im_tz_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date', 
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        im_tz_hex_df['Date'] = pd.to_datetime(im_tz_hex_df['Date'], errors='coerce')
        im_tz_hex_df = im_tz_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        im_tz_hex_df_recon = im_tz_hex_df[im_tz_hex_df['Amount'] > 0].copy()
        im_tz_hex_df_recon['Date_Match'] = im_tz_hex_df_recon['Date'].dt.date
        im_tz_hex_df_recon['Amount_Rounded'] = im_tz_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "TZS"
        if 'CURRENCY' in im_tz_hex_df.columns and not im_tz_hex_df['CURRENCY'].empty:
            unique_currencies = im_tz_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (I&M TZS Specific) ---
        im_tz_bank_df.columns = im_tz_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Transaction Date', 'Value Date', 'Description', 
                            'Tran Id', 'Cheque ID', 'Debit', 'Credit', 'Balance (TZS)']
        if not all(col in im_tz_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in im_tz_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Rename columns for consistency
        im_tz_bank_df = im_tz_bank_df.rename(columns={
            'Transaction Date': 'Transaction_Date',
            'Value Date': 'Value_Date',
            'Tran Id': 'ID',
            'Cheque ID': 'Cheque_ID',
            'Balance (TZS)': 'Balance'
        })

        # Use Value Date if available, otherwise Transaction Date
        im_tz_bank_df['Date'] = pd.to_datetime(im_tz_bank_df['Value_Date'], dayfirst=True, errors='coerce')
        im_tz_bank_df['Date'] = im_tz_bank_df['Date'].fillna(
            pd.to_datetime(im_tz_bank_df['Transaction_Date'], dayfirst=True, errors='coerce'))
        im_tz_bank_df = im_tz_bank_df.dropna(subset=['Date']).copy()
        
        # Improved credit amount cleaning - handle text values and convert to numeric
        im_tz_bank_df['Credit'] = im_tz_bank_df['Credit'].astype(str)
        
        # Remove any non-numeric characters except decimal point and negative sign
        im_tz_bank_df['Credit'] = im_tz_bank_df['Credit'].str.replace(r'[^\d.-]', '', regex=True)
        
        # Convert to numeric, coercing errors to NaN
        im_tz_bank_df['Credit'] = pd.to_numeric(im_tz_bank_df['Credit'], errors='coerce')
        
        # Fill NaN values with 0 (for text entries that couldn't be converted)
        im_tz_bank_df['Credit'] = im_tz_bank_df['Credit'].fillna(0)

        # Filter for positive credits
        im_tz_bank_df_recon = im_tz_bank_df[im_tz_bank_df['Credit'] > 0].copy()
        im_tz_bank_df_recon['Amount'] = im_tz_bank_df_recon['Credit']
        im_tz_bank_df_recon = im_tz_bank_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        im_tz_bank_df_recon['Date_Match'] = im_tz_bank_df_recon['Date'].dt.date
        im_tz_bank_df_recon['Amount_Rounded'] = im_tz_bank_df_recon['Amount'].round(2)

        if im_tz_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for positive credits.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = im_tz_hex_df_recon['Amount'].sum()
        total_bank_credits = im_tz_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            im_tz_hex_df_recon.assign(Source_Internal='Internal'),
            im_tz_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank'))
        
        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = create_empty_unmatched_df()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = create_empty_unmatched_df()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        summary = {
            "Total Internal Records (for recon)": len(im_tz_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(im_tz_bank_df_recon),
            "Total Internal Credits (Original)": total_internal_credits,
            "Total Bank Credits (Original)": total_bank_credits,
            "Overall Discrepancy (Original)": discrepancy_amount,
            "Total Matched Transactions (All Stages)": len(matched_total),
            "Unmatched Internal Records (Final)": len(final_unmatched_internal),
            "Unmatched Bank Records (Final)": len(final_unmatched_bank),
            "Unmatched Internal Amount (Final)": final_unmatched_internal['Amount'].sum(),
            "Unmatched Bank Amount (Final)": final_unmatched_bank['Amount'].sum(),
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during I&M TZS reconciliation processing: {str(e)}")
        return create_empty_matched_df(), create_empty_unmatched_df(), create_empty_unmatched_df(), {}

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_mpesa_tz(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for M-Pesa Tanzania.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=5).
    Filters for credit transactions where 'Paid In' > 0 and 'Opposite Party' contains 'Nala money'.
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
        # --- 1. Load the datasets ---
        mpesa_hex_df = read_uploaded_file(internal_file_obj, header=0)
        mpesa_bank_df = read_uploaded_file(bank_file_obj, header=5)  # Header at row 6 (0-indexed 5)
        
        if mpesa_hex_df is None or mpesa_bank_df is None:
            st.error("One or both files could not be loaded for M-Pesa TZ.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        mpesa_hex_df.columns = mpesa_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in mpesa_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in mpesa_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        mpesa_hex_df = mpesa_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        mpesa_hex_df['Date'] = pd.to_datetime(mpesa_hex_df['Date'], errors='coerce')
        mpesa_hex_df = mpesa_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        mpesa_hex_df_recon = mpesa_hex_df[mpesa_hex_df['Amount'] > 0].copy()
        mpesa_hex_df_recon['Date_Match'] = mpesa_hex_df_recon['Date'].dt.date
        mpesa_hex_df_recon['Amount_Rounded'] = mpesa_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "TZS"
        if 'CURRENCY' in mpesa_hex_df.columns and not mpesa_hex_df['CURRENCY'].empty:
            unique_currencies = mpesa_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (M-Pesa TZ Specific) ---
        mpesa_bank_df.columns = mpesa_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Completion Time', 'Paid In', 'Opposite Party', 'Receipt No.']
        if not all(col in mpesa_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in mpesa_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        mpesa_bank_df = mpesa_bank_df.rename(columns={
            'Completion Time': 'Date',
            'Paid In': 'Credit',
            'Opposite Party': 'Description',
            'Receipt No.': 'ID'
        })

        # Convert dates - M-Pesa format is typically DD-MM-YYYY HH:MM:SS
        mpesa_bank_df['Date'] = pd.to_datetime(mpesa_bank_df['Date'], dayfirst=True, errors='coerce')
        mpesa_bank_df = mpesa_bank_df.dropna(subset=['Date']).copy()

        # Clean credit amounts - remove commas and convert to numeric
        mpesa_bank_df['Credit'] = (
            mpesa_bank_df['Credit'].astype(str)
            .str.replace(',', '', regex=False)
            .astype(float)
            .fillna(0)
        )

        # M-Pesa TZ Specific Filters
        # 1. Filter for positive credits
        # 2. Filter for transactions containing 'Nala money' in Opposite Party
        mpesa_bank_df_recon = mpesa_bank_df[
            (mpesa_bank_df['Credit'] > 0) &
            (mpesa_bank_df['Description'].astype(str).str.contains('Nala money', case=False, na=False))
        ].copy()

        if mpesa_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for positive credits with 'Nala money' in description.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        mpesa_bank_df_recon['Amount'] = mpesa_bank_df_recon['Credit']
        mpesa_bank_df_recon = mpesa_bank_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        mpesa_bank_df_recon['Date_Match'] = mpesa_bank_df_recon['Date'].dt.date
        mpesa_bank_df_recon['Amount_Rounded'] = mpesa_bank_df_recon['Amount'].round(2)

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = mpesa_hex_df_recon['Amount'].sum()
        total_bank_credits = mpesa_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            mpesa_hex_df_recon.assign(Source_Internal='Internal'),
            mpesa_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Total Internal Records (for recon)": len(mpesa_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(mpesa_bank_df_recon),
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
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during M-Pesa TZ reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

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
        "Currency": extracted_currency,
        "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
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
        "Currency": extracted_currency,
        "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
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
        "Currency": extracted_currency,
        "% accuracy": f"{(total_bank_credits_original / total_internal_credits_original * 100):.2f}%" if total_internal_credits_original != 0 else "N/A"
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
        "Currency": extracted_currency,
        "% accuracy": f"{(total_bank_credits_original / total_internal_credits_original * 100):.2f}%" if total_internal_credits_original != 0 else "N/A"
    }

    # --- 10. Return the results ---
    return final_matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_zeepay(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Zeepay Ghana.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=0).
    Filters for records where 'Total Receipts' > 0.
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
        # --- 1. Load the datasets ---
        zeepay_hex_df = read_uploaded_file(internal_file_obj, header=0)
        zeepay_bank_df = read_uploaded_file(bank_file_obj, header=0)
        
        if zeepay_hex_df is None or zeepay_bank_df is None:
            st.error("One or both files could not be loaded for Zeepay.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        zeepay_hex_df.columns = zeepay_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in zeepay_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in zeepay_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        zeepay_hex_df = zeepay_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        zeepay_hex_df['Date'] = pd.to_datetime(zeepay_hex_df['Date'], errors='coerce')
        zeepay_hex_df = zeepay_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        zeepay_hex_df_recon = zeepay_hex_df[zeepay_hex_df['Amount'] > 0].copy()
        zeepay_hex_df_recon['Date_Match'] = zeepay_hex_df_recon['Date'].dt.date
        zeepay_hex_df_recon['Amount_Rounded'] = zeepay_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "GHS"  # Default for Zeepay Ghana
        if 'CURRENCY' in zeepay_hex_df.columns and not zeepay_hex_df['CURRENCY'].empty:
            unique_currencies = zeepay_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (Zeepay Specific) ---
        zeepay_bank_df.columns = zeepay_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Date', 'Total Receipts']
        if not all(col in zeepay_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in zeepay_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Clean and convert 'Total Receipts' to numeric
        zeepay_bank_df['Total Receipts'] = (
            zeepay_bank_df['Total Receipts'].astype(str)
            .str.replace(',', '', regex=False)
            .astype(float)
            .fillna(0)
        )

        # Filter for positive receipts
        zeepay_bank_df_recon = zeepay_bank_df[zeepay_bank_df['Total Receipts'] > 0].copy()
        
        # Convert dates
        zeepay_bank_df_recon['Date'] = pd.to_datetime(zeepay_bank_df_recon['Date'], errors='coerce')
        zeepay_bank_df_recon = zeepay_bank_df_recon.dropna(subset=['Date']).copy()

        # Prepare for reconciliation
        zeepay_bank_df_recon['Amount'] = zeepay_bank_df_recon['Total Receipts']
        zeepay_bank_df_recon = zeepay_bank_df_recon[['Date', 'Amount', 'Description', 'Bank & Txn Ref']].copy()
        zeepay_bank_df_recon = zeepay_bank_df_recon.rename(columns={'Bank & Txn Ref': 'ID'})
        zeepay_bank_df_recon['Date_Match'] = zeepay_bank_df_recon['Date'].dt.date
        zeepay_bank_df_recon['Amount_Rounded'] = zeepay_bank_df_recon['Amount'].round(2)

        if zeepay_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for positive receipts.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = zeepay_hex_df_recon['Amount'].sum()
        total_bank_credits = zeepay_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            zeepay_hex_df_recon.assign(Source_Internal='Internal'),
            zeepay_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Total Internal Records (for recon)": len(zeepay_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(zeepay_bank_df_recon),
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
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during Zeepay reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_fincra_ghs(internal_file_obj, bank_file_obj):
    try:
        # Initialize empty DataFrames with proper columns
        empty_df = pd.DataFrame(columns=[
            'Date_Internal', 'Amount_Internal', 'ID_Internal',
            'Date_Bank', 'Amount_Bank', 'ID_Bank',
            'Amount_Rounded', 'Date_Difference_Days'
        ])
        empty_unmatched = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
        
        # --- 1. Load datasets ---
        fincra_hex_df = read_uploaded_file(internal_file_obj, header=0)
        fincra_bank_df = read_uploaded_file(bank_file_obj, header=0)

        if fincra_hex_df is None or fincra_bank_df is None:
            return empty_df, empty_unmatched, empty_unmatched, {}

        # Check if essential columns exist in bank file
        required_bank_columns = ['Date Initiated', 'Amount Received', 'Currency', 'Status']
        if not all(col in fincra_bank_df.columns for col in required_bank_columns):
            st.error(f"Could not find required columns ({required_bank_columns}) in Fincra bank file.")
            return empty_df, empty_unmatched, empty_unmatched, {}

        # --- 2. Preprocessing for internal_df ---
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

        # Calculate total internal credits
        total_internal_credits = fincra_hex_df_recon['Amount'].sum()
        total_internal_records = len(fincra_hex_df_recon)

        # --- 3. Preprocessing for bank_df (Bank Statements - Fincra Specific
        fincra_bank_df.columns = fincra_bank_df.columns.str.strip()

        # 1. Apply filters first
        fincra_bank_df = fincra_bank_df[
            (fincra_bank_df['Amount Received'] > 0) &
            (fincra_bank_df['Currency'].astype(str).str.strip().str.upper() == 'GHS') &
            (fincra_bank_df['Status'].astype(str).str.strip().str.lower() == 'approved')
        ].copy()

        # 2. Parse dates (simple version without timezone)
      
        # In your bank data preprocessing:
        fincra_bank_df['Date'] = pd.to_datetime(
            fincra_bank_df['Date Initiated'].str.split(' GMT').str[0],
            format='%d/%m/%Y, %I:%M:%S %p',
            errors='coerce'
        ).dt.tz_localize(None)

        # 3. Clean amounts
        fincra_bank_df['Credit'] = (
            fincra_bank_df['Amount Received'].astype(str)
            .str.replace(',', '')
            .str.replace(r'[^\d\.]', '', regex=True)
            .replace('', '0')
            .astype(float)
        )

        # 4. Prepare for reconciliation
        fincra_bank_df_recon = fincra_bank_df[['Date', 'Credit', 'Reference']].rename(columns={
            'Credit': 'Amount',
            'Reference': 'ID'
        }).copy()
        fincra_bank_df_recon['Date_Match'] = fincra_bank_df_recon['Date'].dt.date
        fincra_bank_df_recon['Amount_Rounded'] = fincra_bank_df_recon['Amount'].round(2)

        # Calculate total bank credits
        total_bank_credits = fincra_bank_df_recon['Amount'].sum()
        total_bank_records = len(fincra_bank_df_recon)

        # --- 4. Initial Exact Matching ---
        reconciled_df = pd.merge(
            fincra_hex_df_recon.assign(Source_Internal='Internal'),
            fincra_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Extract matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            matched_exact = matched_exact[[
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank',
                'Amount_Rounded'
            ]].copy()
            matched_exact['Date_Difference_Days'] = 0  # Exact matches have 0 day difference

        # Prepare unmatched records for tolerance matching
        unmatched_internal = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal.empty:
            unmatched_internal = unmatched_internal[[
                'Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded'
            ]].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal['Date'] = pd.to_datetime(unmatched_internal['Date'])

        unmatched_bank = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank.empty:
            unmatched_bank = unmatched_bank[[
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ]].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank['Date'] = pd.to_datetime(unmatched_bank['Date'])

        # --- 5. Date Tolerance Matching (±3 days) ---
        matched_tolerance = pd.DataFrame()
        if not unmatched_internal.empty and not unmatched_bank.empty:
            matched_tolerance, unmatched_internal, unmatched_bank = perform_date_tolerance_matching(
                unmatched_internal,
                unmatched_bank,
                tolerance_days=3
            )

        # --- 6. Combine all matches ---
        matched_final = pd.concat([matched_exact, matched_tolerance], ignore_index=True)

        # --- 7. Prepare final unmatched records ---
        final_unmatched_internal = unmatched_internal.copy()
        final_unmatched_bank = unmatched_bank.copy()

        # --- 8. Generate Summary ---
        total_matched = len(matched_final)
        total_internal = len(fincra_hex_df_recon)
        accuracy = (total_matched / total_internal * 100) if total_internal > 0 else 0

        summary = {
            "Provider name": "Fincra GHS",
            "Currency": "GHS",
            "# of Transactions": total_matched,
            "Partner Statement": total_bank_credits,
            "Treasury Records": total_internal_credits,
            "Variance": total_internal_credits - total_bank_credits,
            "% accuracy": f"{accuracy:.2f}%",
            "Status": "Matched" if final_unmatched_internal.empty and final_unmatched_bank.empty else "Partial",
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Comments": "",
            "Matching Breakdown": {
                "Exact Matches": len(matched_exact),
                "Tolerance Matches": len(matched_tolerance)
            }
        }

        return matched_final, final_unmatched_internal, final_unmatched_bank, summary

    except Exception as e:
        st.error(f"Fincra Reconciliation error: {str(e)}")
        return empty_df, empty_unmatched, empty_unmatched, {}
    
def reconcile_flutterwave_ghs(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Flutterwave Ghana (GHS).
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=0).
    Filters bank records for type='C' (credits) and amount > 0.
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
        # --- 1. Load the datasets ---
        flutterwave_hex_df = read_uploaded_file(internal_file_obj, header=0)
        flutterwave_bank_df = read_uploaded_file(bank_file_obj, header=0)
        
        if flutterwave_hex_df is None or flutterwave_bank_df is None:
            st.error("One or both files could not be loaded for Flutterwave GHS.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        flutterwave_hex_df.columns = flutterwave_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in flutterwave_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in flutterwave_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        flutterwave_hex_df = flutterwave_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        flutterwave_hex_df['Date'] = pd.to_datetime(flutterwave_hex_df['Date'], errors='coerce')
        flutterwave_hex_df = flutterwave_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        flutterwave_hex_df_recon = flutterwave_hex_df[flutterwave_hex_df['Amount'] > 0].copy()
        flutterwave_hex_df_recon['Date_Match'] = flutterwave_hex_df_recon['Date'].dt.date
        flutterwave_hex_df_recon['Amount_Rounded'] = flutterwave_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "GHS"
        if 'CURRENCY' in flutterwave_hex_df.columns and not flutterwave_hex_df['CURRENCY'].empty:
            unique_currencies = flutterwave_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (Flutterwave GHS Specific) ---
        flutterwave_bank_df.columns = flutterwave_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['type', 'amount', 'date', 'reference']
        if not all(col in flutterwave_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in flutterwave_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Filter for credit transactions (type='C'), positive amounts and exclude reversals
        flutterwave_bank_df = flutterwave_bank_df[
            (flutterwave_bank_df['type'].astype(str).str.upper() == 'C') &
            (~flutterwave_bank_df['remarks'].astype(str).str.contains('rvsl', case=False, na=False)) &
            (pd.to_numeric(flutterwave_bank_df['amount'], errors='coerce') > 0)
        ].copy()

        # Clean and convert amount
        flutterwave_bank_df['amount'] = pd.to_numeric(
            flutterwave_bank_df['amount'].astype(str).str.replace(',', '', regex=False),
            errors='coerce'
        ).fillna(0)

        # Convert dates
        flutterwave_bank_df['date'] = pd.to_datetime(flutterwave_bank_df['date'], errors='coerce')
        flutterwave_bank_df = flutterwave_bank_df.dropna(subset=['date']).copy()

        # Prepare bank recon dataframe
        flutterwave_bank_df_recon = flutterwave_bank_df.rename(columns={
            'date': 'Date',
            'amount': 'Amount',
            'reference': 'ID',
            'narration': 'Description'
        }).copy()

        flutterwave_bank_df_recon = flutterwave_bank_df_recon[['Date', 'Amount', 'Description', 'ID']]
        flutterwave_bank_df_recon['Date_Match'] = flutterwave_bank_df_recon['Date'].dt.date
        flutterwave_bank_df_recon['Amount_Rounded'] = flutterwave_bank_df_recon['Amount'].round(2)

        if flutterwave_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for type='C' and amount>0.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = flutterwave_hex_df_recon['Amount'].sum()
        total_bank_credits = flutterwave_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            flutterwave_hex_df_recon.assign(Source_Internal='Internal'),
            flutterwave_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Total Internal Records (for recon)": len(flutterwave_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(flutterwave_bank_df_recon),
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
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during Flutterwave GHS reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary
        
def reconcile_aza_xof(internal_file_obj, bank_file_obj, sheet_name=None):
    """
    Performs reconciliation for AZA Finance XOF.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (Excel with multiple sheets).
    sheet_name: Optional sheet name to use from the bank statement Excel file.
    """
    # Initialize empty DataFrames
    matched_transactions = pd.DataFrame(columns=[
        'Date_Internal', 'Amount_Internal', 'ID_Internal',
        'Date_Bank', 'Amount_Bank', 'ID_Bank',
        'Amount_Rounded'
    ])
    unmatched_internal = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    unmatched_bank = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    summary = {}

    try:
        # --- 1. Load internal records ---
        aza_hex_df = read_uploaded_file(internal_file_obj, header=0)
        if aza_hex_df is None:
            st.error("Failed to load internal records file.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Load bank statement ---
        aza_bank_df = read_uploaded_file(bank_file_obj, header=0)
        
        # --- 3. Preprocessing for internal records ---
        aza_hex_df.columns = aza_hex_df.columns.astype(str).str.strip()
        
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in aza_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in aza_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        aza_hex_df = aza_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        aza_hex_df['Date'] = pd.to_datetime(aza_hex_df['Date'], errors='coerce')
        aza_hex_df = aza_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        aza_hex_df_recon = aza_hex_df[aza_hex_df['Amount'] > 0].copy()
        aza_hex_df_recon['Date_Match'] = aza_hex_df_recon['Date'].dt.date
        aza_hex_df_recon['Amount_Rounded'] = aza_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "XOF"
        if 'CURRENCY' in aza_hex_df.columns and not aza_hex_df['CURRENCY'].empty:
            unique_currencies = aza_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 4. Preprocessing for bank statements (AZA XOF Specific) ---
        aza_bank_df.columns = aza_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Transaction Type', 'Date', 'Credits', 'Input Currency']
        if not all(col in aza_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in aza_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary
        
        # For bank statements (AZA XOF Specific)
        aza_bank_df['Date'] = pd.to_datetime(aza_bank_df['Date'], errors='coerce')

        # Clean credits
        aza_bank_df['Credits'] = (
            aza_bank_df['Credits'].astype(str)
            .str.replace('[^\d.]', '', regex=True)
            .replace('', '0')
            .astype(float)
            .round(2)
        )

        # Filter records
        aza_bank_df = aza_bank_df[
            (aza_bank_df['Transaction Type'].str.strip().str.upper() == 'CREDIT') &
            (aza_bank_df['Input Currency'].str.strip().str.upper() == 'XOF') &
            (aza_bank_df['Credits'] > 0)
        ].copy()

        # Handle ID column
        id_col = 'BitPesa ID' if 'BitPesa ID' in aza_bank_df.columns else 'Transaction Reference'
        if id_col not in aza_bank_df.columns:
            aza_bank_df['ID'] = 'Bank_' + aza_bank_df.index.astype(str)
        else:
            aza_bank_df = aza_bank_df.rename(columns={id_col: 'ID'})

        # Rename columns for consistency
        aza_bank_df = aza_bank_df.rename(columns={
            'Date': 'Date',
            'Credits': 'Amount',
            'TXN Narration': 'Description'
        })

        # Filter positive amounts
        aza_bank_df_recon = aza_bank_df[['Date', 'Amount', 'Description', 'ID']].copy()
        aza_bank_df_recon['Date_Match'] = aza_bank_df_recon['Date'].dt.date
        aza_bank_df_recon['Amount_Rounded'] = aza_bank_df_recon['Amount'].round(2)

        if aza_bank_df_recon.empty:
            st.warning("No valid bank records found after applying AZA XOF filters.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 5. Calculate Total Amounts and Discrepancy ---
        total_internal_credits = aza_hex_df_recon['Amount'].sum()
        total_bank_credits = aza_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 6. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            aza_hex_df_recon.assign(Source_Internal='Internal'),
            aza_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 7. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 8. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Provider name": "Aza Finance XOF",
            "Month & Year": datetime.datetime.now().strftime("%m/%Y"),
            "# of Transactions": len(matched_total),
            "Partner Statement": total_bank_credits, 
            "Treasury Records": total_internal_credits, 
            "Variance": discrepancy_amount, 
            "Total Matched Amount (Internal)": total_matched_amount_internal,
            "Total Matched Amount (Bank)": total_matched_amount_bank,
            "Unmatched Internal Records (Final)": len(final_unmatched_internal),
            "Unmatched Bank Records (Final)": len(final_unmatched_bank),
            "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
            "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
            "Currency": extracted_currency,
            "Status": "Matched" if abs(discrepancy_amount) < 0.01 else "Unmatched",
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Comments": "",
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A",
            "Bank Records Filter": "Transaction Type=CREDIT, Input Currency=XOF, Credits>0"
        }

    except Exception as e:
        st.error(f"Error during AZA XOF reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary
    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_hub2_ic_xof(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Hub2 XOF.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=0).
    Filters bank records for type='deposit' and amount > 0 after converting text to numeric.
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = create_empty_matched_df()
    unmatched_internal = create_empty_unmatched_df()
    unmatched_bank = create_empty_unmatched_df()
    summary = {}

    try:
        # --- 1. Load the datasets ---
        hub2_hex_df = read_uploaded_file(internal_file_obj, header=0)
        hub2_bank_df = read_uploaded_file(bank_file_obj, header=0)

        if hub2_hex_df is None or hub2_bank_df is None:
            st.error("One or both files could not be loaded for Hub2 XOF.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        hub2_hex_df.columns = hub2_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in hub2_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in hub2_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        hub2_hex_df = hub2_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date', 
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        hub2_hex_df['Date'] = pd.to_datetime(hub2_hex_df['Date'], errors='coerce')
        hub2_hex_df = hub2_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        hub2_hex_df_recon = hub2_hex_df[hub2_hex_df['Amount'] > 0].copy()
        hub2_hex_df_recon['Date_Match'] = hub2_hex_df_recon['Date'].dt.date
        hub2_hex_df_recon['Amount_Rounded'] = hub2_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "XOF"
        if 'CURRENCY' in hub2_hex_df.columns and not hub2_hex_df['CURRENCY'].empty:
            unique_currencies = hub2_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (Hub2 XOF Specific) ---
        hub2_bank_df.columns = hub2_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['type', 'amount', 'createdAtDate']
        if not all(col in hub2_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in hub2_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Filter for deposits and positive amounts
        hub2_bank_df = hub2_bank_df[
            (hub2_bank_df['type'].astype(str).str.strip().str.lower() == 'deposit')
        ].copy()

        # Clean amount - remove commas and convert to numeric
        hub2_bank_df['amount'] = (
            hub2_bank_df['amount'].astype(str)
            .str.replace(',', '', regex=False)
            .replace('', '0')
            .astype(float)
        )

        # Filter positive amounts
        hub2_bank_df = hub2_bank_df[hub2_bank_df['amount'] > 0].copy()

        # Handle ID column - use transactionId if available
        id_col = 'transactionId' if 'transactionId' in hub2_bank_df.columns else 'reference'
        if id_col not in hub2_bank_df.columns:
            hub2_bank_df['ID'] = 'Bank_' + hub2_bank_df.index.astype(str)
        else:
            hub2_bank_df = hub2_bank_df.rename(columns={id_col: 'ID'})

        # Convert dates
        hub2_bank_df['Date'] = pd.to_datetime(hub2_bank_df['createdAtDate'], errors='coerce')
        hub2_bank_df = hub2_bank_df.dropna(subset=['Date']).copy()

        # Prepare bank recon dataframe
        hub2_bank_df_recon = hub2_bank_df.rename(columns={
            'amount': 'Amount',
            'reference': 'Description'
        }).copy()

        hub2_bank_df_recon = hub2_bank_df_recon[['Date', 'Amount', 'Description', 'ID']]
        hub2_bank_df_recon['Date_Match'] = hub2_bank_df_recon['Date'].dt.date
        hub2_bank_df_recon['Amount_Rounded'] = hub2_bank_df_recon['Amount'].round(2)

        if hub2_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for deposits and positive amounts.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = hub2_hex_df_recon['Amount'].sum()
        total_bank_credits = hub2_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            hub2_hex_df_recon.assign(Source_Internal='Internal'),
            hub2_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = create_empty_unmatched_df()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = create_empty_unmatched_df()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Provider name": "Hub2 XOF",
            "Currency": extracted_currency,
            "Month & Year": datetime.datetime.now().strftime("%m/%Y"),
            "# of Transactions": len(matched_total),
            "Partner Statement": total_bank_credits,
            "Treasury Records": total_internal_credits,
            "Variance": discrepancy_amount,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A",
            "Status": "Matched" if abs(discrepancy_amount) < 0.01 else "Unmatched",
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Comments": "",
            "Bank Records Filter": "type=deposit, amount>0"
        }

    except Exception as e:
        st.error(f"Error during Hub2 XOF reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary


def reconcile_kremit(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Kremit.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=0).
    Filters bank records for Amount > 0 and Ref containing 'IB Local Transfer'.
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = create_empty_matched_df()
    unmatched_internal = create_empty_unmatched_df()
    unmatched_bank = create_empty_unmatched_df()
    summary = {}

    try:
        # --- 1. Load the datasets ---
        kremit_hex_df = read_uploaded_file(internal_file_obj, header=0)
        kremit_bank_df = read_uploaded_file(bank_file_obj, header=0)

        if kremit_hex_df is None or kremit_bank_df is None:
            st.error("One or both files could not be loaded for Kremit.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        kremit_hex_df.columns = kremit_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in kremit_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in kremit_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        kremit_hex_df = kremit_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date', 
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        kremit_hex_df['Date'] = pd.to_datetime(kremit_hex_df['Date'], errors='coerce')
        kremit_hex_df = kremit_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        kremit_hex_df_recon = kremit_hex_df[kremit_hex_df['Amount'] > 0].copy()
        kremit_hex_df_recon['Date_Match'] = kremit_hex_df_recon['Date'].dt.date
        kremit_hex_df_recon['Amount_Rounded'] = kremit_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "RWF"
        if 'CURRENCY' in kremit_hex_df.columns and not kremit_hex_df['CURRENCY'].empty:
            unique_currencies = kremit_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (Kremit Specific) ---
        kremit_bank_df.columns = kremit_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Ref', 'Amount']
        if not all(col in kremit_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in kremit_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Filter for IB Local Transfer and positive amounts
        kremit_bank_df = kremit_bank_df[
            (kremit_bank_df['Ref'].astype(str).str.contains('IB Local Transfer', case=False)) &
            (kremit_bank_df['Amount'].astype(str).str.replace(',', '', regex=False).astype(float) > 0)
        ].copy()

        # Clean amount - remove commas and convert to numeric
        kremit_bank_df['Amount'] = (
            kremit_bank_df['Amount'].astype(str)
            .str.replace(',', '', regex=False)
            .replace('', '0')
            .astype(float)
        )

        # Extract date from Ref field (format: "2025-06-30 Z291139 000002069853 IB Local Transfer...")
        def extract_kremit_date(ref):
            try:
                # First 10 characters should be YYYY-MM-DD
                date_str = str(ref)[:10]
                return pd.to_datetime(date_str, errors='coerce')
            except:
                return pd.NaT

        kremit_bank_df['Date'] = kremit_bank_df['Ref'].apply(extract_kremit_date)
        kremit_bank_df = kremit_bank_df.dropna(subset=['Date']).copy()

        # Handle ID column - use Ref as ID
        kremit_bank_df['ID'] = kremit_bank_df['Ref']

        # Prepare bank recon dataframe
        kremit_bank_df_recon = kremit_bank_df[['Date', 'Amount', 'ID']].copy()
        kremit_bank_df_recon['Date_Match'] = kremit_bank_df_recon['Date'].dt.date
        kremit_bank_df_recon['Amount_Rounded'] = kremit_bank_df_recon['Amount'].round(2)

        if kremit_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for IB Local Transfer and positive amounts.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = kremit_hex_df_recon['Amount'].sum()
        total_bank_credits = kremit_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            kremit_hex_df_recon.assign(Source_Internal='Internal'),
            kremit_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = create_empty_unmatched_df()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = create_empty_unmatched_df()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Provider name": "Kremit",
            "Currency": extracted_currency,
            "Month & Year": datetime.datetime.now().strftime("%m/%Y"),
            "# of Transactions": len(matched_total),
            "Partner Statement": total_bank_credits,
            "Treasury Records": total_internal_credits,
            "Variance": discrepancy_amount,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A",
            "Status": "Matched" if abs(discrepancy_amount) < 0.01 else "Unmatched",
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Comments": "",
            "Bank Records Filter": "Ref contains 'IB Local Transfer', amount>0"
        }

    except Exception as e:
        st.error(f"Error during Kremit reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_flutterwave_rwf(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for Flutterwave RWF.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=0).
    Filters bank records for type='C' and amount > 0.
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = create_empty_matched_df()
    unmatched_internal = create_empty_unmatched_df()
    unmatched_bank = create_empty_unmatched_df()
    summary = {}

    try:
        # --- 1. Load the datasets ---
        fw_rwf_hex_df = read_uploaded_file(internal_file_obj, header=0)
        fw_rwf_bank_df = read_uploaded_file(bank_file_obj, header=0)

        if fw_rwf_hex_df is None or fw_rwf_bank_df is None:
            st.error("One or both files could not be loaded for Flutterwave RWF.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        fw_rwf_hex_df.columns = fw_rwf_hex_df.columns.astype(str).str.strip()

        # Essential columns for internal records
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in fw_rwf_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in fw_rwf_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        fw_rwf_hex_df = fw_rwf_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date', 
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        fw_rwf_hex_df['Date'] = pd.to_datetime(fw_rwf_hex_df['Date'], errors='coerce')
        fw_rwf_hex_df = fw_rwf_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        fw_rwf_hex_df_recon = fw_rwf_hex_df[fw_rwf_hex_df['Amount'] > 0].copy()
        fw_rwf_hex_df_recon['Date_Match'] = fw_rwf_hex_df_recon['Date'].dt.date
        fw_rwf_hex_df_recon['Amount_Rounded'] = fw_rwf_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "RWF"  # Default for Flutterwave RWF
        if 'CURRENCY' in fw_rwf_hex_df.columns and not fw_rwf_hex_df['CURRENCY'].empty:
            unique_currencies = fw_rwf_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (Flutterwave RWF Specific) ---
        fw_rwf_bank_df.columns = fw_rwf_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['type', 'amount', 'date', 'reference']
        if not all(col in fw_rwf_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in fw_rwf_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Filter for credits (type='C') and and exclude reversals
        fw_rwf_bank_df = fw_rwf_bank_df[
            (fw_rwf_bank_df['type'].astype(str).str.strip().str.upper() == 'C') &
            (~fw_rwf_bank_df['remarks'].astype(str).str.contains('rvsl', case=False, na=False))
        ].copy()

        # Clean amount - remove commas and convert to numeric
        fw_rwf_bank_df['amount'] = (
            fw_rwf_bank_df['amount'].astype(str)
            .str.replace(',', '', regex=False)
            .replace('', '0')
            .astype(float)
        )

        # Convert dates (handling timezone if present)
        fw_rwf_bank_df['date'] = pd.to_datetime(fw_rwf_bank_df['date'], utc=True)
        fw_rwf_bank_df['date'] = fw_rwf_bank_df['date'].dt.tz_localize(None)  # Remove timezone
        fw_rwf_bank_df = fw_rwf_bank_df.dropna(subset=['date']).copy()

        # Handle ID column - use reference if available
        id_col = 'reference' if 'reference' in fw_rwf_bank_df.columns else 'narration'
        if id_col not in fw_rwf_bank_df.columns:
            fw_rwf_bank_df['ID'] = 'Bank_' + fw_rwf_bank_df.index.astype(str)
        else:
            fw_rwf_bank_df = fw_rwf_bank_df.rename(columns={id_col: 'ID'})

        # Prepare bank recon dataframe
        fw_rwf_bank_df_recon = fw_rwf_bank_df.rename(columns={
            'date': 'Date',
            'amount': 'Amount',
            'narration': 'Description'
        }).copy()

        fw_rwf_bank_df_recon = fw_rwf_bank_df_recon[['Date', 'Amount', 'Description', 'ID']]
        fw_rwf_bank_df_recon['Date_Match'] = fw_rwf_bank_df_recon['Date'].dt.date
        fw_rwf_bank_df_recon['Amount_Rounded'] = fw_rwf_bank_df_recon['Amount'].round(2)

        if fw_rwf_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for credits and positive amounts.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = fw_rwf_hex_df_recon['Amount'].sum()
        total_bank_credits = fw_rwf_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            fw_rwf_hex_df_recon.assign(Source_Internal='Internal'),
            fw_rwf_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = create_empty_unmatched_df()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = create_empty_unmatched_df()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Provider name": "Flutterwave RWF",
            "Currency": extracted_currency,
            "Month & Year": datetime.datetime.now().strftime("%m/%Y"),
            "# of Transactions": len(matched_total),
            "Partner Statement": total_bank_credits,
            "Treasury Records": total_internal_credits,
            "Variance": discrepancy_amount,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A",
            "Status": "Matched" if abs(discrepancy_amount) < 0.01 else "Unmatched",
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Comments": "",
            "Bank Records Filter": "type=C, amount>0"
        }

    except Exception as e:
        st.error(f"Error during Flutterwave RWF reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_i_and_m_rwf(internal_file_obj, bank_file_obj):
    """
    Performs reconciliation for I&M Bank Rwanda (RWF).
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (CSV/Excel with header=17).
    Filters for credit transactions where 'Credit' > 0 after converting text to numeric.
    Returns matched, unmatched_internal, unmatched_bank dataframes and a summary dictionary.
    """
    # Initialize empty DataFrames with proper columns
    matched_transactions = create_empty_matched_df()
    unmatched_internal = create_empty_unmatched_df()
    unmatched_bank = create_empty_unmatched_df()
    summary = {}

    try:
        # --- 1. Load the datasets ---
        im_rwf_hex_df = read_uploaded_file(internal_file_obj, header=0)
        im_rwf_bank_df = read_uploaded_file(bank_file_obj, header=17)

        if im_rwf_hex_df is None or im_rwf_bank_df is None:
            st.error("One or both files could not be loaded for I&M RWF.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Preprocessing for internal records ---
        im_rwf_hex_df.columns = im_rwf_hex_df.columns.astype(str).str.strip()

        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in im_rwf_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in im_rwf_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        im_rwf_hex_df = im_rwf_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date', 
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        im_rwf_hex_df['Date'] = pd.to_datetime(im_rwf_hex_df['Date'], errors='coerce')
        im_rwf_hex_df = im_rwf_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        im_rwf_hex_df_recon = im_rwf_hex_df[im_rwf_hex_df['Amount'] > 0].copy()
        im_rwf_hex_df_recon['Date_Match'] = im_rwf_hex_df_recon['Date'].dt.date
        im_rwf_hex_df_recon['Amount_Rounded'] = im_rwf_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "RWF"
        if 'CURRENCY' in im_rwf_hex_df.columns and not im_rwf_hex_df['CURRENCY'].empty:
            unique_currencies = im_rwf_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 3. Preprocessing for bank statements (I&M RWF Specific) ---
        im_rwf_bank_df.columns = im_rwf_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Transaction Date', 'Value Date', 'Description', 
                            'Tran Id', 'Cheque ID', 'Debit', 'Credit', 'Balance (RWF)']
        if not all(col in im_rwf_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in im_rwf_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # Rename columns for consistency
        im_rwf_bank_df = im_rwf_bank_df.rename(columns={
            'Transaction Date': 'Transaction_Date',
            'Value Date': 'Value_Date',
            'Tran Id': 'ID',
            'Cheque ID': 'Cheque_ID',
            'Balance (RWF)': 'Balance'
        })

        # Use Value Date if available, otherwise Transaction Date
        im_rwf_bank_df['Date'] = pd.to_datetime(im_rwf_bank_df['Value_Date'], dayfirst=True, errors='coerce')
        im_rwf_bank_df['Date'] = im_rwf_bank_df['Date'].fillna(
            pd.to_datetime(im_rwf_bank_df['Transaction_Date'], dayfirst=True, errors='coerce'))
        im_rwf_bank_df = im_rwf_bank_df.dropna(subset=['Date']).copy()
        
        # Robust credit amount cleaning - handle text values and convert to numeric
        im_rwf_bank_df['Credit'] = im_rwf_bank_df['Credit'].astype(str)
        
        # Remove any non-numeric characters except decimal point and negative sign
        im_rwf_bank_df['Credit'] = im_rwf_bank_df['Credit'].str.replace(r'[^\d.-]', '', regex=True)
        
        # Convert to numeric, coercing errors to NaN
        im_rwf_bank_df['Credit'] = pd.to_numeric(im_rwf_bank_df['Credit'], errors='coerce')
        
        # Fill NaN values with 0 (for text entries that couldn't be converted)
        im_rwf_bank_df['Credit'] = im_rwf_bank_df['Credit'].fillna(0)

        # Filter for positive credits
        im_rwf_bank_df_recon = im_rwf_bank_df[im_rwf_bank_df['Credit'] > 0].copy()
        im_rwf_bank_df_recon['Amount'] = im_rwf_bank_df_recon['Credit']
        im_rwf_bank_df_recon = im_rwf_bank_df_recon[['Date', 'Amount', 'Description', 'ID']].copy()
        im_rwf_bank_df_recon['Date_Match'] = im_rwf_bank_df_recon['Date'].dt.date
        im_rwf_bank_df_recon['Amount_Rounded'] = im_rwf_bank_df_recon['Amount'].round(2)

        if im_rwf_bank_df_recon.empty:
            st.warning("No valid bank records found after filtering for positive credits.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 4. Calculate Total Amounts and Discrepancy (before reconciliation) ---
        total_internal_credits = im_rwf_hex_df_recon['Amount'].sum()
        total_bank_credits = im_rwf_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 5. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            im_rwf_hex_df_recon.assign(Source_Internal='Internal'),
            im_rwf_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank'))
        
        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = create_empty_unmatched_df()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = create_empty_unmatched_df()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 6. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 7. Summary of Reconciliation ---
        summary = {
            "Total Internal Records (for recon)": len(im_rwf_hex_df_recon),
            "Total Bank Statement Records (for recon)": len(im_rwf_bank_df_recon),
            "Total Internal Credits (Original)": total_internal_credits,
            "Total Bank Credits (Original)": total_bank_credits,
            "Overall Discrepancy (Original)": discrepancy_amount,
            "Total Matched Transactions (All Stages)": len(matched_total),
            "Unmatched Internal Records (Final)": len(final_unmatched_internal),
            "Unmatched Bank Records (Final)": len(final_unmatched_bank),
            "Unmatched Internal Amount (Final)": final_unmatched_internal['Amount'].sum(),
            "Unmatched Bank Amount (Final)": final_unmatched_bank['Amount'].sum(),
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during I&M RWF reconciliation processing: {str(e)}")
        return create_empty_matched_df(), create_empty_unmatched_df(), create_empty_unmatched_df(), {}

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

def reconcile_aza_xaf(internal_file_obj, bank_file_obj, sheet_name=None):
    """
    Performs reconciliation for AZA Finance XAF.
    Expects internal_file_obj (CSV/Excel) and bank_file_obj (Excel with multiple sheets).
    sheet_name: Optional sheet name to use from the bank statement Excel file.
    """
    # Initialize empty DataFrames
    matched_transactions = pd.DataFrame(columns=[
        'Date_Internal', 'Amount_Internal', 'ID_Internal',
        'Date_Bank', 'Amount_Bank', 'ID_Bank',
        'Amount_Rounded'
    ])
    unmatched_internal = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    unmatched_bank = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
    summary = {}

    try:
        # --- 1. Load internal records ---
        aza_hex_df = read_uploaded_file(internal_file_obj, header=0)
        if aza_hex_df is None:
            st.error("Failed to load internal records file.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 2. Load bank statement ---
        aza_bank_df = read_uploaded_file(bank_file_obj, header=0)
        
        # --- 3. Preprocessing for internal records ---
        aza_hex_df.columns = aza_hex_df.columns.astype(str).str.strip()
        
        internal_required_cols = ['TRANSFER_DATE', 'AMOUNT']
        if not all(col in aza_hex_df.columns for col in internal_required_cols):
            missing_cols = [col for col in internal_required_cols if col not in aza_hex_df.columns]
            st.error(f"Internal records are missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        aza_hex_df = aza_hex_df.rename(columns={
            'TRANSFER_DATE': 'Date',
            'AMOUNT': 'Amount',
            'COMMENT': 'Description',
            'TRANSFER_ID': 'ID'
        })

        # Convert and filter dates
        aza_hex_df['Date'] = pd.to_datetime(aza_hex_df['Date'], errors='coerce')
        aza_hex_df = aza_hex_df.dropna(subset=['Date']).copy()

        # Filter positive amounts and prepare for reconciliation
        aza_hex_df_recon = aza_hex_df[aza_hex_df['Amount'] > 0].copy()
        aza_hex_df_recon['Date_Match'] = aza_hex_df_recon['Date'].dt.date
        aza_hex_df_recon['Amount_Rounded'] = aza_hex_df_recon['Amount'].round(2)

        # --- Extract currency from internal_df ---
        extracted_currency = "XAF"
        if 'CURRENCY' in aza_hex_df.columns and not aza_hex_df['CURRENCY'].empty:
            unique_currencies = aza_hex_df['CURRENCY'].dropna().unique()
            if unique_currencies.size > 0:
                extracted_currency = str(unique_currencies[0])

        # --- 4. Preprocessing for bank statements (AZA XAF Specific) ---
        aza_bank_df.columns = aza_bank_df.columns.astype(str).str.strip()

        # Essential columns for bank statements
        bank_required_cols = ['Transaction Type', 'Date', 'Credits', 'Input Currency']
        if not all(col in aza_bank_df.columns for col in bank_required_cols):
            missing_cols = [col for col in bank_required_cols if col not in aza_bank_df.columns]
            st.error(f"Bank statement is missing essential columns: {', '.join(missing_cols)}.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary
        
        # For bank statements (AZA XAF Specific)
        aza_bank_df['Date'] = pd.to_datetime(aza_bank_df['Date'], errors='coerce')

        # Clean credits
        aza_bank_df['Credits'] = (
            aza_bank_df['Credits'].astype(str)
            .str.replace('[^\d.]', '', regex=True)
            .replace('', '0')
            .astype(float)
            .round(2)
        )

        # Filter records - Changed XOF to XAF here
        aza_bank_df = aza_bank_df[
            (aza_bank_df['Transaction Type'].str.strip().str.upper() == 'CREDIT') &
            (aza_bank_df['Input Currency'].str.strip().str.upper() == 'XAF') & 
            (aza_bank_df['Credits'] > 0)
        ].copy()

        # Handle ID column
        id_col = 'BitPesa ID' if 'BitPesa ID' in aza_bank_df.columns else 'Transaction Reference'
        if id_col not in aza_bank_df.columns:
            aza_bank_df['ID'] = 'Bank_' + aza_bank_df.index.astype(str)
        else:
            aza_bank_df = aza_bank_df.rename(columns={id_col: 'ID'})

        # Rename columns for consistency
        aza_bank_df = aza_bank_df.rename(columns={
            'Date': 'Date',
            'Credits': 'Amount',
            'TXN Narration': 'Description'
        })

        # Filter positive amounts
        aza_bank_df_recon = aza_bank_df[['Date', 'Amount', 'Description', 'ID']].copy()
        aza_bank_df_recon['Date_Match'] = aza_bank_df_recon['Date'].dt.date
        aza_bank_df_recon['Amount_Rounded'] = aza_bank_df_recon['Amount'].round(2)

        if aza_bank_df_recon.empty:
            st.warning("No valid bank records found after applying AZA XAF filters.")
            return matched_transactions, unmatched_internal, unmatched_bank, summary

        # --- 5. Calculate Total Amounts and Discrepancy ---
        total_internal_credits = aza_hex_df_recon['Amount'].sum()
        total_bank_credits = aza_bank_df_recon['Amount'].sum()
        discrepancy_amount = total_internal_credits - total_bank_credits

        # --- 6. Reconciliation (Exact Match) ---
        reconciled_df = pd.merge(
            aza_hex_df_recon.assign(Source_Internal='Internal'),
            aza_bank_df_recon.assign(Source_Bank='Bank'),
            on=['Date_Match', 'Amount_Rounded'],
            how='outer',
            suffixes=('_Internal', '_Bank')
        )

        # Identify matched transactions
        matched_exact = reconciled_df.dropna(subset=['Source_Internal', 'Source_Bank']).copy()
        if not matched_exact.empty:
            cols_to_select = [col for col in [
                'Date_Internal', 'Amount_Internal', 'ID_Internal',
                'Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded'
            ] if col in matched_exact.columns]
            matched_transactions = matched_exact[cols_to_select].copy()

        # Prepare initially unmatched records for tolerance matching
        unmatched_internal_initial = reconciled_df[reconciled_df['Source_Bank'].isna()].copy()
        if not unmatched_internal_initial.empty:
            unmatched_internal_initial = unmatched_internal_initial[['Date_Internal', 'Amount_Internal', 'ID_Internal', 'Amount_Rounded']].rename(columns={
                'Date_Internal': 'Date', 'Amount_Internal': 'Amount', 'ID_Internal': 'ID'
            }).copy()
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])
        else:
            unmatched_internal_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_internal_initial['Date'] = pd.to_datetime(unmatched_internal_initial['Date'])

        unmatched_bank_initial = reconciled_df[reconciled_df['Source_Internal'].isna()].copy()
        if not unmatched_bank_initial.empty:
            unmatched_bank_initial = unmatched_bank_initial[['Date_Bank', 'Amount_Bank', 'ID_Bank', 'Amount_Rounded']].rename(columns={
                'Date_Bank': 'Date', 'Amount_Bank': 'Amount', 'ID_Bank': 'ID'
            }).copy()
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])
        else:
            unmatched_bank_initial = pd.DataFrame(columns=['Date', 'Amount', 'ID', 'Amount_Rounded'])
            unmatched_bank_initial['Date'] = pd.to_datetime(unmatched_bank_initial['Date'])

        # --- 7. Reconciliation with Date Tolerance (3 days) ---
        matched_with_tolerance, final_unmatched_internal, final_unmatched_bank = perform_date_tolerance_matching(
            unmatched_internal_initial, unmatched_bank_initial, tolerance_days=3
        )

        # Combine all matched records
        matched_total = pd.concat([matched_transactions, matched_with_tolerance], ignore_index=True)

        # --- 8. Summary of Reconciliation ---
        total_matched_amount_internal = matched_total['Amount_Internal'].sum() if 'Amount_Internal' in matched_total.columns else 0
        total_matched_amount_bank = matched_total['Amount_Bank'].sum() if 'Amount_Bank' in matched_total.columns else 0
        remaining_unmatched_internal_amount = final_unmatched_internal['Amount'].sum() if 'Amount' in final_unmatched_internal.columns else 0
        remaining_unmatched_bank_amount = final_unmatched_bank['Amount'].sum() if 'Amount' in final_unmatched_bank.columns else 0

        summary = {
            "Provider name": "Aza Finance XAF",  # Changed XOF to XAF
            "Month & Year": datetime.datetime.now().strftime("%m/%Y"),
            "# of Transactions": len(matched_total),
            "Partner Statement": total_bank_credits, 
            "Treasury Records": total_internal_credits, 
            "Variance": discrepancy_amount, 
            "Total Matched Amount (Internal)": total_matched_amount_internal,
            "Total Matched Amount (Bank)": total_matched_amount_bank,
            "Unmatched Internal Records (Final)": len(final_unmatched_internal),
            "Unmatched Bank Records (Final)": len(final_unmatched_bank),
            "Unmatched Internal Amount (Final)": remaining_unmatched_internal_amount,
            "Unmatched Bank Amount (Final)": remaining_unmatched_bank_amount,
            "Currency": extracted_currency,
            "Status": "Matched" if abs(discrepancy_amount) < 0.01 else "Unmatched",
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Comments": "",
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A",
            "Bank Records Filter": "Transaction Type=CREDIT, Input Currency=XAF, Credits>0"  # Changed XOF to XAF
        }

    except Exception as e:
        st.error(f"Error during AZA XAF reconciliation processing: {str(e)}")  # Changed XOF to XAF
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
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A",
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
        "Currency": extracted_currency,
        "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
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

        # Calculate total internal credits
        total_internal_credits = verto_hex_df_recon['Amount'].sum()
        total_internal_records = len(verto_hex_df_recon)

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

        # clean credit amounts
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
        
        # Calculate total bank credits
        total_bank_credits = verto_bank_df_recon['Amount'].sum()
        total_bank_records = len(verto_bank_df_recon)

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
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A",
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
    
def reconcile_fincra_ngn(internal_file_obj, bank_file_obj, recon_month=None, recon_year=None):
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

        # Calculate total internal credits
        total_internal_credits = fincra_hex_df_recon['Amount'].sum()
        total_internal_records = len(fincra_hex_df_recon)

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

        # Calculate total bank credits
        total_bank_credits = fincra_bank_df_recon['Amount'].sum()
        total_bank_records = len(fincra_bank_df_recon)

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
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A",
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
            "Currency": extracted_currency,
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
        }

    except Exception as e:
        st.error(f"Error during Zenith NG reconciliation processing: {str(e)}")
        return matched_transactions, unmatched_internal, unmatched_bank, summary

    return matched_total, final_unmatched_internal, final_unmatched_bank, summary

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
                elif 'brampton' in narration: return 'Yellow card, NGN a/c'
                elif 'titan-paystack' in narration or 'multigate' in narration: return 'Multigate, NGN a/c'
                elif 'gogetter' in narration: return 'AYG'
                elif 'zerozilo' in narration or 'silverfile' in narration or 'palm bills' in narration: return 'Fincra, NGN a/c'
                elif 'ift technologies' in narration or 'budpay' in narration or 'bud infrastructure' in narration: return 'Torus Mara, NGN a/c'
                elif 'starks associates' in narration or 'shamiri' in narration or 'second jeu' in narration: return 'Straitpay (Starks), UK, NGN a/c'
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
            "Currency": "NGN",
            "% accuracy": f"{(total_bank_credits / total_internal_credits * 100):.2f}%" if total_internal_credits != 0 else "N/A"
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
    
    # Get the amounts from the summary
    treasury_total = summary_dict.get("Treasury Records", summary_dict.get("Total Internal Credits (Original)", 0))
    bank_total = summary_dict.get("Partner Statement", summary_dict.get("Total Bank Credits (Original)", 0))
    
    # Calculate the new accuracy based on amounts
    if treasury_total != 0:
        percentage_accuracy = (bank_total / treasury_total * 100)
    else:
        percentage_accuracy = 0
    
    # Create the row with all required columns
    row = {
        "Provider name": provider_name,
        "Currency": summary_dict.get("Currency", get_currency_for_country(selected_country)),
        "Month & Year": recon_month_year,
        "# of Transactions": summary_dict.get("Total Matched Transactions (All Stages)", 0),
        "Partner Statement": bank_total,
        "Treasury Records": treasury_total,
        "Variance": summary_dict.get("Overall Discrepancy (Original)", 0),
        "% accuracy": f"{percentage_accuracy:.2f}%",
        "Status": "Matched" if abs(bank_total - treasury_total) < 0.01 else "Unmatched",
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
# KE
RECONCILIATION_FUNCTIONS["Equity KE"] = reconcile_equity_ke
RECONCILIATION_FUNCTIONS["Cellulant KE"] = reconcile_cellulant_ke
RECONCILIATION_FUNCTIONS["Zamupay PYCS"] = reconcile_zamupay
RECONCILIATION_FUNCTIONS["Pesaswap"] = reconcile_pesaswap
RECONCILIATION_FUNCTIONS["Mpesa KE"] = reconcile_mpesa_ke
RECONCILIATION_FUNCTIONS["I&M KES"] = reconcile_i_and_m_kes
RECONCILIATION_FUNCTIONS["I&M USD Sending"] = reconcile_i_and_m_usd_ke_sending
RECONCILIATION_FUNCTIONS["I&M USD Receiving"] = reconcile_i_and_m_usd_ke_receiving
RECONCILIATION_FUNCTIONS["NCBA KES"] = reconcile_ncba_kes
RECONCILIATION_FUNCTIONS["NCBA USD"] = reconcile_ncba_usd

# TZ
RECONCILIATION_FUNCTIONS["Selcom TZ"] = reconcile_selcom_tz
RECONCILIATION_FUNCTIONS["NMB"] = reconcile_nmb
RECONCILIATION_FUNCTIONS["Equity TZ"] = reconcile_equity_tz
RECONCILIATION_FUNCTIONS["Cellulant TZ"] = reconcile_cellulant_tz
RECONCILIATION_FUNCTIONS["Mpesa TZ"] = reconcile_mpesa_tz
RECONCILIATION_FUNCTIONS["I&M TZS"] = reconcile_i_and_m_tzs
#RECONCILIATION_FUNCTIONS["CRDB TZS"] = reconcile_crdb_tzs
#RECONCILIATION_FUNCTIONS["CRDB USD"] = reconcile_crdb_usd
#RECONCILIATION_FUNCTIONS["UBA"] = reconcile_uba
#RECONCILIATION_FUNCTIONS["I&M USD (TZ)"] = reconcile_i_and_m_usd_tz

#UG
RECONCILIATION_FUNCTIONS["Flutterwave Ug"] = reconcile_flutterwave_ug
#RECONCILIATION_FUNCTIONS["Pegasus"] = reconcile_pegasus
#RECONCILIATION_FUNCTIONS["Equity UGX"] = reconcile_equity_ugx
#RECONCILIATION_FUNCTIONS["Equity Ug USD"] = reconcile_equity_ug_usd

#Ghana
RECONCILIATION_FUNCTIONS["Flutterwave GHS"] = reconcile_flutterwave_ghs
RECONCILIATION_FUNCTIONS["Zeepay"] = reconcile_zeepay
RECONCILIATION_FUNCTIONS["Fincra GHS"] = reconcile_fincra_ghs

#SEN
RECONCILIATION_FUNCTIONS["Aza Finance XOF"] = reconcile_aza_xof
RECONCILIATION_FUNCTIONS["Hub2 IC"] = reconcile_hub2_ic_xof
#RECONCILIATION_FUNCTIONS["Hub2 SEN"] = reconcile_hub2_sen

#RWF
RECONCILIATION_FUNCTIONS["I&M RWF"] = reconcile_i_and_m_rwf
RECONCILIATION_FUNCTIONS["Flutterwave RWF"] = reconcile_flutterwave_rwf
RECONCILIATION_FUNCTIONS["Kremit"] = reconcile_kremit
#RECONCILIATION_FUNCTIONS["I&M USD (RWF)"] = reconcile_i_and_m_usd_rwf

#CAM
RECONCILIATION_FUNCTIONS["Aza Finance XAF"] = reconcile_aza_xaf
#RECONCILIATION_FUNCTIONS["Hub2 XAF"] = reconcile_hub2_xaf
#RECONCILIATION_FUNCTIONS["Peex"] = reconcile_peex
#RECONCILIATION_FUNCTIONS["Pawapay"] = reconcile_pawapay

# NGN
RECONCILIATION_FUNCTIONS["Fincra NGN"] = reconcile_fincra_ngn
RECONCILIATION_FUNCTIONS["Flutterwave NGN"] = reconcile_flutterwave_ngn
RECONCILIATION_FUNCTIONS["Moniepoint"] = reconcile_moniepoint
RECONCILIATION_FUNCTIONS["Verto"] = reconcile_verto
RECONCILIATION_FUNCTIONS["Cellulant NGN"] = reconcile_cellulant_ngn
RECONCILIATION_FUNCTIONS["Zenith"] = reconcile_zenith

# --- Streamlit UI Page Functions ---
def homepage():
    """Displays the country and bank selection page."""
    st.header("Treasury Flows Reconciliation")
    
    st.write("Select a country to see its providers, then click on a partner to begin reconciliation.")
    st.divider()
    
    st.subheader("Country")
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
    
    # Initialize session state for Pesaswap file unlocked flag and feedback if not present
    if 'pesaswap_file_unlocked' not in st.session_state:
        st.session_state.pesaswap_file_unlocked = False
    if 'pesaswap_feedback' not in st.session_state:
        st.session_state.pesaswap_feedback = None
    
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

    # Reset Pesaswap session state if a new bank file is uploaded
    if bank_file is not None and bank_file != st.session_state.get('bank_file'):
        if 'unlocked_pesaswap_bank' in st.session_state:
            del st.session_state.unlocked_pesaswap_bank
        st.session_state.pesaswap_file_unlocked = False
        st.session_state.pesaswap_feedback = None
        # Also reset selected sheet when new file is uploaded
        if 'selected_sheet' in st.session_state:
            del st.session_state.selected_sheet

    # Store uploaded files in session state
    if internal_file is not None:
        st.session_state.internal_file = internal_file
    if bank_file is not None:
        st.session_state.bank_file = bank_file

    # Display feedback if available
    feedback_placeholder = st.empty()
    if st.session_state.pesaswap_feedback:
        if "Error" in st.session_state.pesaswap_feedback:
            feedback_placeholder.error(st.session_state.pesaswap_feedback)
        elif "successfully" in st.session_state.pesaswap_feedback.lower():
            feedback_placeholder.success(st.session_state.pesaswap_feedback)
        else:
            feedback_placeholder.warning(st.session_state.pesaswap_feedback)

    # Add duplicate check button
    check_dupes = st.checkbox("Check for duplicated records")
    
    # Check for duplicates if requested
    if check_dupes and 'internal_file' in st.session_state:
        try:
            st.session_state.internal_file.seek(0)
            internal_df = read_uploaded_file(st.session_state.internal_file, header=0)
            if internal_df is not None:
                duplicates = check_duplicates(internal_df, st.session_state.selected_bank)
                if not duplicates.empty:
                    st.warning("⚠️ Duplicate(s) found in internal records:")
                    st.dataframe(duplicates)
                    st.write("Please review these transactions and delete duplicate(s) in zazu before proceeding with reconciliation.")
                else:
                    st.write("No duplicates found, proceed with reconciliation.")
        except Exception as e:
            st.warning(f"Could not check for duplicates: {str(e)}")

    # Run reconciliation automatically if Pesaswap file is unlocked or manually via button
    run_reconciliation = st.button("Run Reconciliation", type="primary")
    
    # Trigger reconciliation if either:
    # 1. The "Run Reconciliation" button is clicked, or
    # 2. The bank is Pesaswap, the file is unlocked, and both files are uploaded
    if run_reconciliation or (
        st.session_state.selected_bank == "Pesaswap" and 
        st.session_state.pesaswap_file_unlocked and 
        'internal_file' in st.session_state and 
        'bank_file' in st.session_state):

        if 'internal_file' not in st.session_state or 'bank_file' not in st.session_state:
            feedback_placeholder.warning("Please upload both files")
            return
            
        with st.spinner("Processing reconciliation..."):
            # Get the appropriate reconciliation function
            recon_func = RECONCILIATION_FUNCTIONS.get(st.session_state.selected_bank, placeholder_reconcile)
            
            try:
                # Reset file pointers before reading
                st.session_state.internal_file.seek(0)
                st.session_state.bank_file.seek(0)
                
                # Call the reconciliation function with appropriate parameters
                if st.session_state.selected_bank in MONTH_FILTER_BANKS:
                    matched, unmatched_int, unmatched_bank, summary = recon_func(
                        st.session_state.internal_file,
                        st.session_state.bank_file,
                        recon_month=recon_month,
                        recon_year=recon_year
                    )
                elif st.session_state.selected_bank in ["Aza Finance XOF", "Aza Finance XAF"]:
                    # Pass the selected sheet for AZA Finance banks
                    matched, unmatched_int, unmatched_bank, summary = recon_func(
                        st.session_state.internal_file,
                        st.session_state.bank_file,
                        sheet_name=st.session_state.get('selected_sheet')
                    )
                else:
                    matched, unmatched_int, unmatched_bank, summary = recon_func(
                        st.session_state.internal_file,
                        st.session_state.bank_file
                    )
                
                # Reset Pesaswap session state after successful reconciliation
                if st.session_state.selected_bank == "Pesaswap":
                    st.session_state.pesaswap_file_unlocked = False
                    st.session_state.pesaswap_feedback = None
                    if 'unlocked_pesaswap_bank' in st.session_state:
                        del st.session_state.unlocked_pesaswap_bank
                
                # Display Results
                feedback_placeholder.success("Reconciliation Complete ✅")
                
                cols = st.columns(5)
                cols[0].metric("Total Matched", summary.get("# of Transactions", summary.get("Total Matched Transactions (All Stages)", 0)))
                
                accuracy = summary.get("% accuracy", 
                      f"{(summary.get('Total Bank Credits (Original)', 0) / summary.get('Total Internal Credits (Original)', 1) * 100):.2f}%"
                      if summary.get('Total Internal Credits (Original)', 0) > 0 
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
                
                # Results Tabs
                tab1, tab2, tab3, tab4 = st.tabs(["Unmatched Internal", "Unmatched Bank", "Matched", "Summary & Download"])
                
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

                with tab4:
                    # Display summary in a more readable format
                    # Calculate date range from internal records
                    date_range_start = matched['Date_Internal'].min()
                    date_range_end = matched['Date_Internal'].max()
        
                    # Create a clean summary dictionary without technical fields
                    clean_summary = {
                        "Provider": summary.get("Provider name", st.session_state.selected_bank),
                        "Currency": summary.get("Currency", "N/A"),
                        "Dates Reconciled": f"{date_range_start} to {date_range_end}",
                        "Total Transactions": summary.get("# of Transactions", summary.get("Total Matched Transactions (All Stages)", 0)),
                        "Partner Statement": summary.get("Partner Statement", summary.get("Total Bank Credits (Original)", 0)),
                        "Treasury Records": summary.get("Treasury Records", summary.get("Total Internal Credits (Original)", 0)),
                        "Variance": summary.get("Variance", summary.get("Overall Discrepancy (Original)", 0)),
                        "Accuracy": summary.get("% accuracy", 
                            f"{(summary.get('Total Bank Credits (Original)', 0) / summary.get('Total Internal Credits (Original)', 1)) * 100:.2f}%"
                            if summary.get('Total Internal Credits (Original)', 0) > 0 
                            else "0%"),
                        "Status": "Matched" if abs(summary.get("Variance", summary.get("Overall Discrepancy (Original)", 1))) < 0.01 else "Unmatched"
                    }
                    
                    # Display the clean summary
                    summary_df = pd.DataFrame.from_dict(clean_summary, orient='index', columns=['Value'])
                    st.dataframe(summary_df)
                    
                    # Convert clean summary to DataFrame for CSV export
                    report_df = pd.DataFrame([clean_summary])
                    
                    # Create CSV download button
                    csv = report_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download Summary",
                        data=csv,
                        file_name=f"{st.session_state.selected_bank}_recon_summary_{datetime.datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        key=f"download_{st.session_state.selected_bank}"
                    )

            except Exception as e:
                feedback_placeholder.error(f"Error during reconciliation: {str(e)}")
                st.error("Please check your input files and try again.")

    # Add a back button to return to the homepage
    if st.button("Back to Home"):
        st.session_state.page = "home"
        st.session_state.pesaswap_file_unlocked = False
        st.session_state.pesaswap_feedback = None
        if 'unlocked_pesaswap_bank' in st.session_state:
            del st.session_state.unlocked_pesaswap_bank
        if 'selected_sheet' in st.session_state:
            del st.session_state.selected_sheet
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
