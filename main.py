import streamlit as st
import pandas as pd
import sqlite3

# Function to execute SQL query and return result as DataFrame
def execute_query(sql_query, conn):
    try:
        result_df = pd.read_sql_query(sql_query, conn)
        return result_df
    except Exception as e:
        st.error(f"Error executing SQL query: {e}")

def main():
    st.title("CSV File Explorer and SQL Query Executor")

    # File uploader for CSV files
    uploaded_file = st.file_uploader("Upload CSV file", type=["csv"])

    if uploaded_file is not None:
        # Load CSV file into DataFrame
        df = pd.read_csv(uploaded_file)
        st.write("### Data from CSV file:")
        st.write(df)

        # Display table schema (column names and data types)
        st.write("### Table Schema:")
        st.write(df.dtypes)

        # Create SQLite in-memory database and table from uploaded CSV data
        conn = sqlite3.connect(":memory:")
        table_name = "data_table"
        df.to_sql(table_name, conn, index=False)

        # Text area for SQL queries
        st.write("### SQL Query Executor:")
        sql_query = st.text_area("Enter your SQL query here")

        # Button to execute SQL query
        if st.button("Execute Query"):
            if sql_query.strip() != "":
                # Dynamically replace "table" with actual table name in SQL query
                sql_query = sql_query.replace("table", table_name)
                result_df = execute_query(sql_query, conn)
                if result_df is not None:
                    st.write("### Query Result:")
                    st.write(result_df)
            else:
                st.warning("Please enter a valid SQL query.")

if __name__ == "__main__":
    main()
