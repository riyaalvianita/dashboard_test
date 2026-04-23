import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery

st.set_page_config(page_title="BigQuery Dashboard", layout="wide")
st.title("📊 Data Terkini dari BigQuery")

# 1. Setup Kredensial
try:
    # Menggunakan file creds.json yang ada di folder yang sama
    credentials = service_account.Credentials.from_service_account_file('creds.json')
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    # Menulis Query (Contoh menggunakan public data Google)
    query = """
        SELECT date(trx_date) as tanggal, count(*) as total 
        FROM `melondata.digital_music.music_plays_20260422` 
        WHERE play_duration >= 60 
        GROUP BY 1 
        ORDER BY 1 DESC 
        LIMIT 15
    """

    # 3. Jalankan Query (dengan Caching agar cepat & hemat biaya)
    @st.cache_data
    def load_data(query):
        return client.query(query).to_dataframe()

    data = load_data(query)

    # 4. Tampilkan di Dashboard
    col1, col2 = st.columns([1, 2])

    with col1:
        st.subheader("Tabel Data")
        st.dataframe(data, use_container_width=True)

    with col2:
        st.subheader("Grafik Batang")
        st.bar_chart(data.set_index('tanggal'))

except Exception as e:
    st.error(f"Error Konfigurasi: {e}")
    st.info("Pastikan file 'creds.json' sudah benar dan library sudah terinstal.")