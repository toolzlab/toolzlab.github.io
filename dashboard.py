import streamlit as st
import pandas as pd
import time
import os

st.title("Trading Signal Dashboard")

def load_signals():
    filename = "signals.csv"
    if os.path.exists(filename):
        try:
            # Read the CSV file fresh each time
            df = pd.read_csv(filename)
        except Exception as e:
            st.error(f"Error loading signals: {e}")
            df = pd.DataFrame()
    else:
        df = pd.DataFrame(columns=["Timestamp", "Symbol", "Bias", "Signal Type", "Explanation", "Entry", "Stop Loss", "Take Profit", "Position Size"])
    return df

# Load signals without caching
df = load_signals()
st.write("Last updated:", time.strftime("%Y-%m-%d %H:%M:%S"))

if df.empty:
    st.info("No trade signals available yet.")
else:
    st.dataframe(df)

if st.button("Refresh Now"):
    st.experimental_rerun()