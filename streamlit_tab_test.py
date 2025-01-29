import streamlit as st

st.title("Streamlit Dropdown Navigation")

# Dropdown menu for navigation
selected_page = st.selectbox("Select a Page", ["Home", "Data", "Settings"])

# Show content based on selection
if selected_page == "Home":
    st.header("Home")
    st.write("Welcome to the Home page!")

elif selected_page == "Data":
    st.header("Data")
    st.write("Explore your data here.")

elif selected_page == "Settings":
    st.header("Settings")
    st.write("Modify your preferences.")