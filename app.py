from flask import Flask, render_template, request, redirect, url_for, session, send_file, flash, jsonify
import pandas as pd
import numpy as np
import psycopg2
import os
import io
import json
import secrets
import string
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from flask_mail import Mail, Message
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Configuration from environment variables
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', '')
app.config['DB_HOST'] = os.getenv('DB_HOST', 'localhost')
app.config['DB_PORT'] = os.getenv('DB_PORT', '5432')
app.config['DB_NAME'] = os.getenv('DB_NAME', 'postgres')
app.config['DB_USER'] = os.getenv('DB_USER', 'postgres')
app.config['DB_PASSWORD'] = os.getenv('DB_PASSWORD', 'postgres')

# Email configuration for verification
app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER', 'smtp.gmail.com')
app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', 587))
app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS', 'True').lower() == 'true'
app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_USERNAME')

mail = Mail(app)

UPLOAD_FOLDER = 'uploads'

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Pandas Settings
pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)

# SQLAlchemy engine for pandas
DATABASE_URL = f"postgresql://{app.config['DB_USER']}:{app.config['DB_PASSWORD']}@{app.config['DB_HOST']}:{app.config['DB_PORT']}/{app.config['DB_NAME']}"
engine = create_engine(DATABASE_URL)

# Constants
TABLE_CUSTOMERS = "customers"
TABLE_INVOICES = "invoices"
TABLE_PRODUCTS = "products"
TABLE_EXPENSES = "expenses"
COLUMN_CUSTOMER = "Customer"
COLUMN_PRODUCT = "Product"
COLUMN_INVOICE_NO = "Invoice_No"
COLUMN_QUANTITY = "Quantity"
COLUMN_SALES_AMOUNT = "Sales_Amount"


# PostgreSQL connection function
def get_db_connection():
    conn = psycopg2.connect(
        host=app.config['DB_HOST'],
        port=app.config['DB_PORT'],
        database=app.config['DB_NAME'],
        user=app.config['DB_USER'],
        password=app.config['DB_PASSWORD']
    )
    return conn


# Generate verification token
def generate_verification_token(length=32):
    return ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(length))


# Send verification email
def send_verification_email(email, token):
    try:
        verification_url = f"{request.host_url}verify-email/{token}"
        msg = Message('Verify Your Email - ProfitScan',
                      recipients=[email])
        msg.body = f'''
Welcome to ProfitScan!

Please click the following link to verify your email address:
{verification_url}

This link will expire in 24 hours.

If you did not create an account, please ignore this email.

Best regards,
ProfitScan Team
'''
        mail.send(msg)
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False


# Decorator for login required
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.')
            return redirect(url_for('login'))
        return f(*args, **kwargs)

    return decorated_function


# Decorator for email verification required
def email_verified_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in to access this page.')
            return redirect(url_for('login'))

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT email_verified FROM users WHERE id = %s", (session['user_id'],))
        user = cursor.fetchone()
        conn.close()

        if user and not user[0]:
            flash('Please verify your email address to access this feature.')
            return redirect(url_for('verify_email_prompt'))

        return f(*args, **kwargs)

    return decorated_function


# Create users table with verification fields
def create_users_table():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'users'
        )
    """)
    table_exists = cursor.fetchone()[0]

    if table_exists:

        try:
            cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT FALSE")
            cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS verification_token TEXT")
            cursor.execute(
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS token_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            cursor.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            print("Existing users table updated with new columns")
        except Exception as e:
            print(f"Error updating users table: {e}")
            conn.rollback()
    else:

        cursor.execute('''
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                email_verified BOOLEAN DEFAULT FALSE,
                verification_token TEXT,
                token_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("New users table created with all columns")

    conn.commit()
    conn.close()


# Load CSV to database
def load_csv(file, table_name):
    df = pd.read_csv(
        file,
        thousands=",",
        decimal=".",
        na_values=["NA", "na", "N/A", "n/a", ""],
    )
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except ValueError:
            pass

    if "Product" in df.columns:
        df["Product"] = df["Product"].str.strip()

    # Use SQLAlchemy engine for pandas to_sql
    with engine.begin() as connection:
        # Drop table if exists
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        # Create new table
        df.to_sql(table_name, connection, if_exists="replace", index=False)


# Load Excel to database
def load_excel(file, table_name):
    df = pd.read_excel(file)

    if "Product" in df.columns:
        df["Product"] = df["Product"].str.strip()

    # Use SQLAlchemy engine for pandas to_sql
    with engine.begin() as connection:
        # Drop table if exists
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        # Create new table
        df.to_sql(table_name, connection, if_exists="replace", index=False)


# Load data from file
def load_data(file, table_name, required_columns):
    try:
        if not file or file.filename == '':
            return "No file selected"

        file_extension = file.filename.split(".")[-1].lower()

        if file_extension == "csv":
            load_csv(file, table_name)
        elif file_extension in ["xlsx", "xls"]:
            load_excel(file, table_name)
        else:
            return "Unsupported file type"

        # Check required columns
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", engine)

        if not set(required_columns).issubset(df.columns):
            missing = set(required_columns) - set(df.columns)
            return f"Missing required columns: {missing}"

        return {"success": True, "df": df}

    except Exception as e:
        return f"Error when trying to read the file: {e}"


# Load data from database
def load_data_from_db(table_name):
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", engine)
        return df
    except Exception as e:
        print(f"Error loading data from database: {e}")
        return pd.DataFrame()


# Merge data from different tables
def merge_data():
    try:
        # Get columns from tables
        customers_columns = pd.read_sql_query(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{TABLE_CUSTOMERS}'", engine
        )["column_name"].tolist()
        products_columns = pd.read_sql_query(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{TABLE_PRODUCTS}'", engine
        )["column_name"].tolist()

        # Remove duplicate key fields but keep Product Group columns
        if COLUMN_CUSTOMER in customers_columns:
            customers_columns.remove(COLUMN_CUSTOMER)
        if COLUMN_PRODUCT in products_columns:
            products_columns.remove(COLUMN_PRODUCT)

        # Keep Product Group 1 and Product Group 2 if they exist
        product_group_columns = []
        for col in products_columns:
            if col.lower() in ['product group 1', 'product group 2', 'product_group_1', 'product_group_2']:
                product_group_columns.append(col)
                products_columns.remove(col)

        # Create column strings for SQL query
        customers_columns_str = ", ".join(
            [f'customers."{col}"' for col in customers_columns]
        )
        products_columns_str = ", ".join(
            [f'products."{col}"' for col in products_columns]
        )
        product_group_str = ", ".join(
            [f'products."{col}"' for col in product_group_columns]
        )

        # All necessary columns for SELECT
        all_columns_str = ", ".join(
            filter(None, ["invoices.*", customers_columns_str, products_columns_str, product_group_str])
        )

        # SQL join query (LEFT JOIN)
        query = f"""
            SELECT {all_columns_str}
            FROM invoices
            LEFT JOIN customers ON invoices."{COLUMN_CUSTOMER}" = customers."{COLUMN_CUSTOMER}"
            LEFT JOIN products ON invoices."{COLUMN_PRODUCT}" = products."{COLUMN_PRODUCT}"
        """

        # Perform merge
        merged_df = pd.read_sql_query(query, engine)
        return merged_df

    except Exception as e:
        print(f"Error merging tables: {e}")
        return pd.DataFrame()


# Calculate cost
def calculate_cost(df):
    try:
        if df is None or df.empty:
            return df

        if 'Product_Cost' in df.columns:
            df['Cost_Amount'] = df['Quantity'] * df['Product_Cost']
        elif 'Cost_%' in df.columns:
            df['Cost_Amount'] = df['Sales_Amount'] * df['Cost_%']
        else:
            df['Cost_Amount'] = 0
    except Exception as e:
        print(f"Error in cost calculation: {e}")
        df['Cost_Amount'] = 0
    return df


# Get all columns with proper handling of product groups
def get_all_columns(df_list):
    all_columns = []
    for df in df_list:
        if df is not None and not df.empty:
            # Get all non-numeric columns
            non_numeric_cols = df.select_dtypes(exclude="number").columns.tolist()

            # Filter out empty columns and date columns
            non_empty_cols = [
                col
                for col in non_numeric_cols
                if (df[col].dropna() != pd.Timestamp(0)).any() and col.lower() != "date"
            ]
            all_columns.extend(non_empty_cols)

    # Remove duplicates and sort
    unique_columns = list(set(all_columns))

    # Put special columns first
    preferred_order = ['Product Group 1', 'Product Group 2', 'Product', 'Customer', 'City', 'Region']
    ordered_columns = []

    for col in preferred_order:
        if col in unique_columns:
            ordered_columns.append(col)
            unique_columns.remove(col)

    # Add remaining columns
    return ordered_columns + sorted(unique_columns)


# Process expenses
def process_expenses(df, expenses_df):
    """Adds new expense columns and distributes them."""
    try:
        if df is None or df.empty:
            return df

        unique_expenses = expenses_df["Expense"].unique()
        zero_data = pd.DataFrame(0, index=df.index, columns=unique_expenses)
        df = pd.concat([df, zero_data], axis=1)

        for expense_name in unique_expenses:
            expense_rows = expenses_df[expenses_df["Expense"] == expense_name]
            for _, row in expense_rows.iterrows():
                df = allocate_expense(df, row)
        return df
    except Exception as e:
        print(f"Expense processing error: {e}")
        return df


# Allocate expense
def allocate_expense(df, row):
    """Distributes the amounts of a single expense according to the rules."""
    try:
        expense_name = row["Expense"]
        allocations = row["Allocate_To"].split(";")
        by_tran = row["Allocate_By_Tran"] / len(allocations)
        by_value = row["Allocate_By_Value"] / len(allocations)
        total_amount = row["Amount"]

        amount_by_tran = total_amount * by_tran
        amount_by_value = total_amount * by_value

        for allocation in allocations:
            df = allocate_based_on_rules(
                df, allocation.strip(), amount_by_tran, amount_by_value, expense_name
            )
    except Exception as e:
        print(f"Cost allocation error: {e}")
    return df


# Allocate based on rules
def allocate_based_on_rules(df, allocation, amount_by_tran, amount_by_value, expense_name):
    """Applies allocation rules with support for AND (;) and OR (|) logic."""
    try:
        if allocation.lower() == "all":
            if not df.empty:
                df[expense_name] += (
                        amount_by_tran / len(df)
                        + amount_by_value * (df["Sales_Amount"] / df["Sales_Amount"].sum())
                )
        else:
            conditions = allocation.split(";")
            temp_df = df.copy()

            for condition in conditions:
                if "=" in condition:
                    key, values = condition.split("=")
                    key = key.strip()
                    value_list = [v.strip() for v in values.split("|")]

                    # Handle Product Group columns with different naming conventions
                    column_key = key
                    if key.lower() in ['product group 1', 'product_group_1']:
                        # Try different column name variations
                        possible_columns = ['Product Group 1', 'Product_Group_1', 'product_group_1']
                        for col in possible_columns:
                            if col in df.columns:
                                column_key = col
                                break
                    elif key.lower() in ['product group 2', 'product_group_2']:
                        possible_columns = ['Product Group 2', 'Product_Group_2', 'product_group_2']
                        for col in possible_columns:
                            if col in df.columns:
                                column_key = col
                                break

                    if column_key in df.columns:
                        temp_df = temp_df[temp_df[column_key].isin(value_list)]
                    else:
                        print(f"Warning: column '{key}' not found in DataFrame. Skipping condition.")
                else:
                    print(f"Invalid condition format: {condition}")

            if not temp_df.empty:
                total_sales = temp_df["Sales_Amount"].sum() or 1
                df.loc[temp_df.index, expense_name] += (
                        amount_by_tran / len(temp_df)
                        + amount_by_value * (df.loc[temp_df.index, "Sales_Amount"] / total_sales)
                )
            else:
                print(f"Warning: no rows matched the condition for '{expense_name}'.")
    except Exception as e:
        print(f"Error applying allocation rules: {e}")
    return df


# Calculate totals
def calculate_totals(df, expenses_df):
    try:
        if df is None or df.empty:
            return df

        expense_columns = []

        for expense_name in expenses_df["Expense"].unique():
            weighted_column_name = f"{expense_name}_Weighted"
            if weighted_column_name in df.columns:
                expense_columns.append(weighted_column_name)
            else:
                expense_columns.append(expense_name)


        existing_columns = [col for col in expense_columns if col in df.columns]

        if existing_columns:
            total_expense = df[existing_columns].sum(axis=1)
            df["Total_Expense"] = total_expense.round(2)
        else:
            df["Total_Expense"] = 0

        required_cols = ["Sales_Amount", "Total_Expense", "Cost_Amount"]
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0

        df["Net_Profit"] = (df["Sales_Amount"] - df["Total_Expense"] - df["Cost_Amount"]).round(2)

    except Exception as e:
        print(f"Error in calculating totals: {e}")
    return df


# Append totals row
def append_totals(df):
    try:
        if df is None or df.empty:
            return df

        numeric_cols = df.select_dtypes(include=np.number).columns
        if len(numeric_cols) == 0:
            return df

        totals = df[numeric_cols].sum()
        totals_row = pd.DataFrame([totals], index=["Total"])

        df = pd.concat([df, totals_row])
        return df
    except Exception as e:
        print(f"Error when adding totals: {e}")
        return df


# Remove empty columns
def remove_empty_columns(df):
    if df is None or df.empty:
        return df
    df = df.dropna(how="all", axis=1)
    return df


# Generate report helper
def generate_report_helper(df, allocation_factor, report_type, expenses_df):
    sum_columns = [
        "Quantity",
        "Sales_Amount",
        "Cost_Amount",
        "Total_Expense",
        "Net_Profit",
    ]

    try:

        if df is None or df.empty:
            return pd.DataFrame(), pd.DataFrame()

        missing_columns = [col for col in sum_columns if col not in df.columns]
        if missing_columns:
            print(f"Warning: Missing columns in report generation: {missing_columns}")

            for col in missing_columns:
                df[col] = 0

        if report_type == "Summary":
            if allocation_factor.lower() == "all":

                if "Total" in df.index:
                    report_df = df.loc[["Total"], sum_columns]
                else:

                    totals = df[sum_columns].sum()
                    report_df = pd.DataFrame([totals], index=["Grand Total"], columns=sum_columns)

                report_df.rename(columns={col: f"Sum of {col}" for col in sum_columns}, inplace=True)

            else:

                if allocation_factor not in df.columns:
                    print(f"Warning: Allocation factor '{allocation_factor}' not found in DataFrame")
                    return pd.DataFrame(), pd.DataFrame()

                report_df = df.groupby(allocation_factor)[sum_columns].sum()
                report_df.loc["Grand Total"] = report_df.sum()
                report_df.rename(columns={col: f"Sum of {col}" for col in sum_columns}, inplace=True)

        elif report_type == "Detailed":
            relevant_cols = sum_columns + list(expenses_df["Expense"].unique())
            numeric_cols = [col for col in relevant_cols if col in df.columns]

            if not numeric_cols:
                print("Warning: No numeric columns found for detailed report")
                return pd.DataFrame(), pd.DataFrame()

            if allocation_factor.lower() == "all":
                report_df = df[numeric_cols].copy()

                if report_df.index.name is None:
                    report_df.index = range(len(report_df))
                report_df = report_df.groupby(report_df.index).sum()
            else:
                if allocation_factor not in df.columns:
                    print(f"Warning: Allocation factor '{allocation_factor}' not found in DataFrame")
                    return pd.DataFrame(), pd.DataFrame()

                report_df = df.groupby(allocation_factor)[numeric_cols].sum()

            if "Total" not in report_df.index:
                report_df.loc["Grand Total"] = report_df.sum()

            report_df.rename(columns={col: f"Sum of {col}" for col in report_df.columns}, inplace=True)

        # Round numeric columns to integers
        numeric_columns = report_df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            report_df[col] = report_df[col].round(0).astype(int)

        display_df = report_df.copy()

        if allocation_factor.lower() != "all":
            report_df = report_df.reset_index(drop=False)

        return report_df, display_df

    except Exception as e:
        print(f"Error generating the report: {e}")

        return pd.DataFrame(), pd.DataFrame()


# Save uploaded file
def save_uploaded_file(file_storage, filename):
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    file_storage.save(file_path)
    return file_path


# Save to database
def save_to_db(df, table_name):
    with engine.begin() as connection:
        # Drop table if exists
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        # Create new table
        df.to_sql(table_name, connection, if_exists='replace', index=False)


# Check if expense table has allocation columns pre-filled
def check_allocation_columns_pre_filled(df):
    return (
            "Allocate_To" in df.columns and
            "Allocate_By_Tran" in df.columns and
            "Allocate_By_Value" in df.columns
    )


# Add missing allocation columns to expenses table
def add_missing_allocation_columns(expenses_df):
    if "Allocate_To" not in expenses_df.columns:
        expenses_df["Allocate_To"] = "all"
    if "Allocate_By_Tran" not in expenses_df.columns:
        expenses_df["Allocate_By_Tran"] = 0.5
    if "Allocate_By_Value" not in expenses_df.columns:
        expenses_df["Allocate_By_Value"] = 0.5
    if "Apply_Weight" not in expenses_df.columns:
        expenses_df["Apply_Weight"] = False
    return expenses_df


# Routes

@app.route('/')
def index():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('spa.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        confirm_password = request.form.get('confirm_password', '')

        # Basic validation
        if not email or not password:
            flash('Email and password are required.')
            return redirect(url_for('register'))

        if password != confirm_password:
            flash('Passwords do not match.')
            return redirect(url_for('register'))

        if len(password) < 8:
            flash('Password must be at least 8 characters long.')
            return redirect(url_for('register'))

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            # Check if user already exists
            cursor.execute("SELECT id FROM users WHERE email = %s", (email,))
            existing_user = cursor.fetchone()

            if existing_user:
                flash('User already exists.')
                return redirect(url_for('register'))

            # Generate verification token
            verification_token = generate_verification_token()

            # Create new user
            password_hash = generate_password_hash(password)
            cursor.execute(
                "INSERT INTO users (email, password_hash, verification_token) VALUES (%s, %s, %s)",
                (email, password_hash, verification_token)
            )
            conn.commit()

            # Send verification email
            if send_verification_email(email, verification_token):
                flash('Registration successful! Please check your email to verify your account.')
            else:
                flash('Registration successful, but we could not send the verification email. Please contact support.')

            return redirect(url_for('login'))

        except Exception as e:
            conn.rollback()
            flash('An error occurred during registration.')
            print(f"Registration error: {e}")
        finally:
            conn.close()

    return render_template('register.html')


@app.route('/verify-email/<token>')
def verify_email(token):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Find user with this token (check if token is not older than 24 hours)
        cursor.execute(
            "SELECT id, email FROM users WHERE verification_token = %s AND email_verified = FALSE AND token_created_at > NOW() - INTERVAL '24 hours'",
            (token,)
        )
        user = cursor.fetchone()

        if user:
            user_id, email = user
            # Verify email
            cursor.execute(
                "UPDATE users SET email_verified = TRUE, verification_token = NULL WHERE id = %s",
                (user_id,)
            )
            conn.commit()

            flash('Email verified successfully! You can now log in.')
            return redirect(url_for('login'))
        else:
            flash('Invalid or expired verification token.')
            return redirect(url_for('login'))

    except Exception as e:
        conn.rollback()
        flash('An error occurred during email verification.')
        print(f"Email verification error: {e}")
    finally:
        conn.close()

    return redirect(url_for('login'))


@app.route('/verify-email-prompt')
#@login_required
def verify_email_prompt():
    return render_template('verify_email.html')


@app.route('/resend-verification', methods=['POST'])
#@login_required
def resend_verification():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "SELECT email, email_verified FROM users WHERE id = %s",
            (session['user_id'],)
        )
        user = cursor.fetchone()

        if user and not user[1]:  # If not verified
            email = user[0]
            new_token = generate_verification_token()

            # Update token
            cursor.execute(
                "UPDATE users SET verification_token = %s, token_created_at = CURRENT_TIMESTAMP WHERE id = %s",
                (new_token, session['user_id'])
            )
            conn.commit()

            if send_verification_email(email, new_token):
                flash('Verification email sent! Please check your inbox.')
            else:
                flash('Failed to send verification email. Please try again later.')
        else:
            flash('Email already verified or user not found.')

    except Exception as e:
        conn.rollback()
        flash('An error occurred while resending verification email.')
        print(f"Resend verification error: {e}")
    finally:
        conn.close()

    return redirect(url_for('verify_email_prompt'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, password_hash, email_verified FROM users WHERE email = %s",
            (email,)
        )
        user = cursor.fetchone()
        conn.close()

        if user and check_password_hash(user[1], password):
            #if not user[2]:  # If email not verified
            #    session['user_id'] = user[0]
            #    session['user_email'] = email
            #   flash('Please verify your email address before accessing the application.')
            #    return redirect(url_for('verify_email_prompt'))

            session['user_id'] = user[0]
            session['user_email'] = email
            flash('Logged in successfully.')
            return redirect(url_for('index'))
        else:
            flash('Invalid email or password.')

    return render_template('login.html')


@app.route('/api/upload', methods=['POST'])
#@login_required
#@email_verified_required
def api_upload():
    try:
        customers_file = request.files.get('customers_file')
        invoices_file = request.files.get('invoices_file')
        products_file = request.files.get('products_file')
        expenses_file = request.files.get('expenses_file')

        # Required columns for each file
        customers_required = ["Customer"]
        invoices_required = ["Invoice_No", "Customer", "Product", "Quantity", "Sales_Amount"]
        products_required = ["Product"]
        expenses_required = ["Expense", "Amount"]

        # Process each file
        result = load_data(customers_file, "customers", customers_required)
        if isinstance(result, str):
            return jsonify({'success': False, 'message': f'Customers: {result}'})

        result = load_data(invoices_file, "invoices", invoices_required)
        if isinstance(result, str):
            return jsonify({'success': False, 'message': f'Invoices: {result}'})

        result = load_data(products_file, "products", products_required)
        if isinstance(result, str):
            return jsonify({'success': False, 'message': f'Products: {result}'})

        result = load_data(expenses_file, "expenses", expenses_required)
        if isinstance(result, str):
            return jsonify({'success': False, 'message': f'Expenses: {result}'})

        # Check if expenses table has allocation columns pre-filled
        expenses_df = load_data_from_db("expenses")
        allocation_columns_pre_filled = check_allocation_columns_pre_filled(expenses_df)

        # Add missing allocation columns if needed
        if not allocation_columns_pre_filled:
            expenses_df = add_missing_allocation_columns(expenses_df)
            save_to_db(expenses_df, "expenses")

        return jsonify({
            'success': True,
            'message': 'Files uploaded successfully',
            'allocation_columns_pre_filled': allocation_columns_pre_filled
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/allocation/rules')
#@login_required
#@email_verified_required
def api_allocation_rules():
    try:
        expenses_df = load_data_from_db("expenses")
        customers_df = load_data_from_db("customers")
        invoices_df = load_data_from_db("invoices")
        products_df = load_data_from_db("products")

        # Get all columns from all tables
        all_columns = get_all_columns([customers_df, invoices_df, products_df])

        # Add "All" option at the beginning
        all_columns = ["All"] + all_columns

        return jsonify({
            'success': True,
            'expenses': expenses_df.to_dict(orient='records'),
            'all_columns': all_columns
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/column_values/<column_name>')
#@login_required
#@email_verified_required
def api_column_values(column_name):
    try:
        # Handle different naming conventions for Product Groups
        actual_column_name = column_name
        if column_name.lower() in ['product group 1', 'product_group_1']:
            # Try to find the actual column name
            products_df = load_data_from_db("products")
            for col in products_df.columns:
                if col.lower() in ['product group 1', 'product_group_1']:
                    actual_column_name = col
                    break
        elif column_name.lower() in ['product group 2', 'product_group_2']:
            products_df = load_data_from_db("products")
            for col in products_df.columns:
                if col.lower() in ['product group 2', 'product_group_2']:
                    actual_column_name = col
                    break

        # Get values from all tables for the specified column
        customers_df = load_data_from_db("customers")
        invoices_df = load_data_from_db("invoices")
        products_df = load_data_from_db("products")

        all_values = set()

        if actual_column_name in customers_df.columns:
            all_values.update(customers_df[actual_column_name].dropna().unique())
        if actual_column_name in invoices_df.columns:
            all_values.update(invoices_df[actual_column_name].dropna().unique())
        if actual_column_name in products_df.columns:
            all_values.update(products_df[actual_column_name].dropna().unique())

        # Convert to list and sort
        values_list = list(all_values)
        values_list.sort()

        return jsonify({
            'success': True,
            'values': values_list
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/expenses/update', methods=['POST'])
#@login_required
#@email_verified_required
def api_expenses_update():
    try:
        updated_expenses = request.get_json()
        expenses_df = load_data_from_db("expenses")

        for i, expense_data in enumerate(updated_expenses):
            if i < len(expenses_df):
                expenses_df.at[i, "Allocate_To"] = expense_data.get('Allocate_To', 'all')

                allocation_tran_percentage = expense_data.get('Allocate_By_Tran_Percentage', 50)

                expenses_df.at[i, "Allocate_By_Tran"] = allocation_tran_percentage / 100.0
                expenses_df.at[i, "Allocate_By_Value"] = (100 - allocation_tran_percentage) / 100.0

                expenses_df.at[i, "Apply_Weight"] = expense_data.get('Apply_Weight', False)

        save_to_db(expenses_df, "expenses")

        return jsonify({'success': True, 'message': 'Expenses updated successfully'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/preview')
#@login_required
#@email_verified_required
def api_preview():
    try:

        merged_df = merge_data()
        if merged_df is None or merged_df.empty:
            return jsonify({'success': True, 'preview_html': '<p>No data available for preview</p>'})

        preview_df = merged_df.head(50)
        preview_html = preview_df.to_html(classes='data', index=False)

        return jsonify({
            'success': True,
            'preview_html': preview_html
        })
    except Exception as e:
        print(f"Error loading preview: {e}")
        return jsonify({'success': True, 'preview_html': '<p>Error loading preview</p>'})


@app.route('/api/process', methods=['POST'])
#@login_required
#@email_verified_required
def api_process():
    try:
        merged_df = merge_data()
        if merged_df is None or merged_df.empty:
            return jsonify({'success': False, 'message': 'No data to process'})

        merged_df = calculate_cost(merged_df)
        expenses_df = load_data_from_db("expenses")

        processed_df = process_expenses(merged_df, expenses_df)
        processed_df = calculate_totals(processed_df, expenses_df)
        processed_df = append_totals(processed_df)
        processed_df = remove_empty_columns(processed_df)

        save_to_db(processed_df, "processed_data")

        if processed_df is None or processed_df.empty:
            processed_html = "<p>No data available</p>"
        else:
            processed_html = processed_df.to_html(classes='data', index=False)

        if expenses_df is None or expenses_df.empty:
            expenses_html = "<p>No expenses data available</p>"
        else:
            expenses_html = expenses_df.to_html(classes='data', index=False)

        session['processed_html'] = processed_html
        session['expenses_html'] = expenses_html

        return jsonify({
            'success': True,
            'processed_html': processed_html,
            'expenses_html': expenses_html
        })
    except Exception as e:
        print(f"Error in api_process: {e}")
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/results')
#@login_required
#@email_verified_required
def api_results():
    try:

        processed_html = session.get('processed_html', '')
        expenses_html = session.get('expenses_html', '')

        if not processed_html or not expenses_html:
            processed_df = load_data_from_db("processed_data")
            expenses_df = load_data_from_db("expenses")

            if processed_df is None or processed_df.empty:
                processed_html = "<p>No data available</p>"
            else:
                processed_html = processed_df.to_html(classes='data', index=False)

            if expenses_df is None or expenses_df.empty:
                expenses_html = "<p>No expenses data available</p>"
            else:
                expenses_html = expenses_df.to_html(classes='data', index=False)


            session['processed_html'] = processed_html
            session['expenses_html'] = expenses_html

        # Get columns for dropdown
        customers_df = load_data_from_db("customers")
        invoices_df = load_data_from_db("invoices")
        products_df = load_data_from_db("products")

        all_columns = get_all_columns([customers_df, invoices_df, products_df])

        return jsonify({
            'success': True,
            'processed_html': processed_html,
            'expenses_html': expenses_html,
            'all_columns': all_columns
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/api/report', methods=['POST'])
@login_required
#@email_verified_required
def api_report():
    try:
        data = request.get_json()
        report_type = data.get('report_type')
        allocation_factor = data.get('allocation_factor', 'all')

        processed_df = load_data_from_db("processed_data")
        expenses_df = load_data_from_db("expenses")

        report_df, _ = generate_report_helper(processed_df, allocation_factor, report_type, expenses_df)

        if report_df is None or report_df.empty:
            return jsonify({
                'success': False,
                'message': 'Report generation failed: Empty result'
            })

        report_html = report_df.to_html(classes='data', index=False)

        return jsonify({
            'success': True,
            'report_html': report_html
        })
    except Exception as e:
        print(f"Error in api_report: {e}")
        return jsonify({
            'success': False,
            'message': f'Error generating report: {str(e)}'
        })


@app.route('/download/processed-excel')
@login_required
#@email_verified_required
def download_processed_excel():
    df = load_data_from_db("processed_data")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="processed_data.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@app.route('/download/expenses-excel')
@login_required
#@email_verified_required
def download_expenses_excel():
    df = load_data_from_db("expenses")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="expenses_data.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@app.route('/api/check_auth')
def check_auth():
    return jsonify({'authenticated': 'user_id' in session})


@app.route('/logout')
def logout():
    session.clear()
    flash('You have been logged out.')
    return redirect(url_for('login'))


if __name__ == '__main__':
    create_users_table()
    app.run(debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true')
