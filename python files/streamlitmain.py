import streamlit as st
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px

# Database connection settings
DB_USER = "root"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "mtb_supply"

# Create a database connection
@st.cache_resource(ttl=600)  # Refreshes every 10 minutes
def get_db_connection():
    return create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Function to load and process data based on the selected page
def load_and_process_data(table_name, start_date, end_date, lot_no, page):
    engine = get_db_connection()
    query = f"""
        SELECT * FROM {table_name}
        WHERE Test_date_time BETWEEN '{start_date}' AND '{end_date}'
        AND Lot IN ({lot_no})
    """
    df = pd.read_sql(query, con=engine)
    
    # Data Cleaning
    df = df[df['Test_status'].notnull()]
    df = df[~df['Chip_serial_no'].str[0].str.isdigit()]
    df = df[df['Chip_serial_no'].str[1].str.isdigit()]

    # Define pivoting logic
    index_columns = {
        "Lot Performance": ['Lot'],
        "Chip series": ['Chip_serial_no'],
        "Lot Chip series": ['Lot', 'Chip_serial_no'],
        "Lot chip batch chip series": ['Lot', 'Chip_batchno', 'Chip_serial_no'],
        "Detailed Data": ['Lab_name', 'Truelab_id', 'Lot', 'Chip_batchno', 'Chip_serial_no']
    }
    
    if page in index_columns:
        pivot = df.pivot_table(index=index_columns[page], values='Patient_id', columns='Test_status', aggfunc='count', margins=True)
        pivotdf = pd.DataFrame(pivot.to_records())
        
        # Specify the columns to fill NaN with 0
        columns_to_fill = ['Error-1', 'Error-1A', 'Error-2', 'Error-3', 'Error-4', 'Error-5', 'Invalid', 'Valid', 'All']
        available_columns = set(pivotdf.columns) & set(columns_to_fill)
        pivotdf[list(available_columns)] = pivotdf[list(available_columns)].fillna(0)
        
        # Ensure 'Invalid' and 'All' exist before performing the calculation
        if "Invalid" in pivotdf.columns and "All" in pivotdf.columns:
            pivotdf["INV_per"] = round(pivotdf["Invalid"] / pivotdf["All"] * 100, 2)
        else:
            pivotdf["INV_per"] = 0
    else:
        pivotdf = df
    
    return df, pivotdf

# Function for conditional formatting
def highlight_high_inv(val):
    color = 'red' if val > 7 else 'green'
    return f'color: white; background-color: {color}'


# Sidebar for date input
st.sidebar.title("Select Date Range")
start_date = st.sidebar.date_input("Start Date")
end_date = st.sidebar.date_input("End Date")

# Dropdown menu
st.sidebar.title("Select Category")
category = st.sidebar.selectbox("Choose an option:", ["17 Lakhs", "16 Lakhs Tranche 1", "16 Lakhs Tranche 2"])

# Map category to Lot numbers
lot_mapping = {
    "17 Lakhs": "'076TB', '077TB', '078TB', '079TB', '080TB', '081TB', '082TB', '083TB', '084TB', '107TB', '108TB', '110TB', '111TB', '112TB', '113TB', '114TB', '115TB', '116TB', '117TB', '118TB', '119TB', 'TB260', 'TB261', 'TB262', 'TB263', 'TB267', 'TB268', 'TB269', 'TB270', 'TB271', 'TB272', 'TB273'",
    "16 Lakhs Tranche 1": "'TB020'",
    "16 Lakhs Tranche 2": "'120TB', '121TB', '122TB', '123TB', '124TB', '125TB', '126TB', '127TB', '128TB', '129TB', '130TB', '131TB', '132TB', '133TB', '134TB', '135TB', '136TB', '137TB', '138TB', '139TB', '140TB'"
}
lot_no = lot_mapping.get(category, "''")

# Define pages
pages = {
    "Lot Performance": "all_test",
    "Chip series": "all_test",
    "Lot Chip series": "all_test",
    "Lot chip batch chip series": "all_test",
    "Detailed Data": "all_test"
}

# Main Page
st.title("📅 Data Selection")
st.write("Select a date range and category from the sidebar to filter data.")
if start_date and end_date:
    st.success(f"Showing data from {start_date} to {end_date} for {category}")
else:
    st.warning("Please select a date range.")

# Create multi-page navigation
selected_page = st.sidebar.radio("Choose a Page", list(pages.keys()))

# Display selected page's data
if start_date and end_date:
    table_name = pages[selected_page]
    
    # Load and process data
    data, data_pivot = load_and_process_data(table_name, start_date, end_date, lot_no, selected_page)
    

    # Display processed data without index & with formatting
    st.subheader(f"📊 Processed Data for {selected_page}")
    styled_data_pivot = data_pivot.style.applymap(highlight_high_inv, subset=['INV_per'])
    st.dataframe(styled_data_pivot.hide(axis='index'))
    
    # Visualization
    st.subheader("📈 Interactive Data Visualization")

    if not data_pivot.empty and "INV_per" in data_pivot.columns:
    # Convert pivot index to a column (if not already)
        index_columns = data_pivot.columns[:len(data_pivot.columns) - len(['INV_per'])].tolist()
    
        # Reshape DataFrame if necessary
        if "index" in data_pivot.columns:
            data_pivot = data_pivot.rename(columns={"index": "Category"})
        else:
            data_pivot = data_pivot.reset_index()

        # Plot using Plotly
        fig = px.line(
            data_pivot,
            x=index_columns[0],  # Use the first index column
            y="INV_per",
            title=f"INV% Trend for {selected_page}",
            labels={index_columns[0]: "Category", "INV_per": "Invalid Percentage (%)"},
            markers=True
        )
        
        # Show interactive plot
        st.plotly_chart(fig, use_container_width=True)

    else:
        st.warning("No valid data available for visualization.")