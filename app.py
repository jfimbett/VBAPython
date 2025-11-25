import streamlit as st

if "count" not in st.session_state:
    st.session_state.count = 0

st.write("Count:", st.session_state.count)

if st.button("Increment"):
    st.session_state.count += 1

if st.button("Reset"):
    st.session_state.count = 0