import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from google.oauth2 import service_account
from google.cloud import bigquery

# --- KONFIGURASI HALAMAN ---
st.set_page_config(page_title="Executive Performance Dashboard", layout="wide")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .stApp { background-color: #F8F9FA; }
    [data-testid="stMetric"] {
        background-color: white;
        padding: 15px;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        border: 1px solid #E0E0E0;
    }
    .main-title { font-size: 24px; font-weight: bold; margin-bottom: 20px; }
    </style>
    """, unsafe_allow_html=True)


try:
    # Membaca kredensial dari Streamlit Secrets (untuk cloud)
    # atau dari file lokal (untuk testing)
    if "gcp_service_account" in st.secrets:
        # Jika berjalan di Cloud
        info = st.secrets["gcp_service_account"]
        credentials = service_account.Credentials.from_service_account_info(info)
    else:
        # Jika berjalan lokal
        credentials = service_account.Credentials.from_service_account_file('creds.json')
    
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Fungsi untuk menjalankan query
    @st.cache_data(ttl=600) # Cache selama 10 menit
    def run_query(query_text):
        return client.query(query_text).to_dataframe()

    # --- DEFINISI QUERY (DARI INPUT KAMU) ---
    SQL_QUERIES = {
        "main": """ with 
  data_vcr as 
  ( select trx_id,amount,date_trunc(updated_at, MONTH) as bulan,t_id
    from gamesott.dvm 
    where status = 1 and DATE_TRUNC(date(updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH) 
  ),
  data_pay as
  (
    select 'idv' as type_trx,trx_id,amount,item,date_trunc(updated_at, MONTH) as bulan
    from gamesott.denormal_transaction
    where DATE_TRUNC(date(updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)  and upper(merchant) like '%IDV%' and accepted = 1
  ),
  data_idv_vcr as
  (
    select trx_id ,amount,item,date_trunc(updated_at, MONTH) as bulan
    from gamesott.idv_voucher
    where DATE_TRUNC(date(updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH) 
  )
,  reed_spen as (
      select dc.*, p.name publisher_name
from gamesott.idv_api dc join
gamesott.dtuumb_publisher p on dc.publisher_id = p.id
where DATE_TRUNC(date(dc.updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)  and dc.accepted = 1
    )
    ,redeem as (
      select 'idv' as type_trx,date_trunc(updated_at, MONTH) as bulan, sum(IF(product = 'idv_credit' OR product = 'idv_voucher', 1, 0)) as trx_redeem,sum(IF(product = 'idv_credit' OR product = 'idv_voucher', amount, 0)) as rev_redeem from reed_spen group by 1,2
    )
    ,spending as (
      select 'idv' as type_trx,date_trunc(updated_at, MONTH) as bulan, sum(IF(product = 'idv_credit' or product = 'idv_voucher', 0, 1)) as trx_spend,sum(IF(product = 'idv_credit' or product = 'idv_voucher', 0, amount)) as rev_spend from reed_spen group by 1,2
    )
, trx as (
  select p.bulan,count(p.trx_id) as total_trx,sum(p.amount) as purchase,sum(d.amount) as idv_voucher,
   count(p.trx_id) as trx_purchase,count(v.trx_id) as trx_voucher
   from data_pay p left outer join data_vcr v on p.trx_id=v.trx_id 
  left outer join data_idv_vcr d on v.t_id=d.trx_id 
  group by 1 order by 1
)
, alls  as (
select 'idv' as type,t.*,r.trx_redeem,r.rev_redeem,s.trx_spend,s.rev_spend from trx t left outer join redeem r on t.bulan=r.bulan
left outer join spending s on t.bulan=s.bulan
)
select  sum(total_trx) as total_trx,sum(rev_purchase) as rev_purchase,sum(rev_voucher) as rev_voucher,sum(rev_redeem) as rev_redeem,
  sum(rev_spend) as rev_spending,sum(trx_purchase) as trx_purchase,sum(trx_voucher) as trx_voucher,sum(trx_redeem) as trx_redeem,
  sum(trx_spend) as trx_spend,sum(rev_voucher)-sum(rev_purchase) as margin from (
  select sum(total_trx) as total_trx,sum(purchase) as rev_purchase,sum(idv_voucher) as rev_voucher,sum(rev_redeem) as rev_redeem,
  sum(rev_spend) as rev_spend,sum(trx_purchase) as trx_purchase,sum(trx_voucher) as trx_voucher,sum(trx_redeem) as trx_redeem,
  sum(trx_spend) as trx_spend from gamesott.ndi_idv_monthly
  union all
  select sum(total_trx) as total_trx,sum(purchase) as rev_purchase,sum(idv_voucher) as rev_voucher,sum(rev_redeem) as rev_redeem,
  sum(rev_spend) as rev_spend,sum(trx_purchase) as trx_purchase,sum(trx_voucher) as trx_voucher,sum(trx_redeem) as trx_redeem,
  sum(trx_spend) as trx_spend from alls
  union all
  select sum(total_trx) as total_trx,sum(purchase) as rev_purchase,sum(idv_voucher) as rev_voucher,sum(rev_redeem) as rev_redeem,
  sum(rev_spend) as rev_spend,sum(trx_purchase) as trx_purchase,sum(trx_voucher) as trx_voucher,sum(trx_redeem) as trx_redeem,
  sum(trx_spend) as trx_spend from gamesott.v_ndi_current_month
  ) """,
        "idv": """ with 
  data_vcr as 
  ( select trx_id,amount,date_trunc(updated_at, MONTH) as bulan,t_id
    from gamesott.dvm 
    where status = 1 and DATE_TRUNC(date(updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH) 
  ),
  data_pay as
  (
    select 'idv' as type_trx,trx_id,amount,item,date_trunc(updated_at, MONTH) as bulan
    from gamesott.denormal_transaction
    where DATE_TRUNC(date(updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)  and upper(merchant) like '%IDV%' and accepted = 1
  ),
  data_idv_vcr as
  (
    select trx_id ,amount,item,date_trunc(updated_at, MONTH) as bulan
    from gamesott.idv_voucher
    where DATE_TRUNC(date(updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH) 
  )
,  reed_spen as (
      select dc.*, p.name publisher_name
from gamesott.idv_api dc join
gamesott.dtuumb_publisher p on dc.publisher_id = p.id
where DATE_TRUNC(date(dc.updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)  and dc.accepted = 1
    )
    ,redeem as (
      select 'idv' as type_trx,date_trunc(updated_at, MONTH) as bulan, sum(IF(product = 'idv_credit' OR product = 'idv_voucher', 1, 0)) as trx_redeem,sum(IF(product = 'idv_credit' OR product = 'idv_voucher', amount, 0)) as rev_redeem from reed_spen group by 1,2
    )
    ,spending as (
      select 'idv' as type_trx,date_trunc(updated_at, MONTH) as bulan, sum(IF(product = 'idv_credit' or product = 'idv_voucher', 0, 1)) as trx_spend,sum(IF(product = 'idv_credit' or product = 'idv_voucher', 0, amount)) as rev_spend from reed_spen group by 1,2
    )
, trx as (
  select p.bulan,count(p.trx_id) as total_trx,sum(p.amount) as purchase,sum(d.amount) as idv_voucher,
   count(p.trx_id) as trx_purchase,count(v.trx_id) as trx_voucher
   from data_pay p left outer join data_vcr v on p.trx_id=v.trx_id 
  left outer join data_idv_vcr d on v.t_id=d.trx_id 
  group by 1 order by 1
)
, alls  as (
select 'idv' as type,t.*,r.trx_redeem,r.rev_redeem,s.trx_spend,s.rev_spend from trx t left outer join redeem r on t.bulan=r.bulan
left outer join spending s on t.bulan=s.bulan
)
select  sum(total_trx) as total_trx,sum(rev_purchase) as rev_purchase,sum(rev_voucher) as rev_voucher,sum(rev_redeem) as rev_redeem,
  sum(rev_spend) as rev_spending,sum(trx_purchase) as trx_purchase,sum(trx_voucher) as trx_voucher,sum(trx_redeem) as trx_redeem,
  sum(trx_spend) as trx_spend,sum(rev_voucher)-sum(rev_purchase) as margin from (
  select sum(total_trx) as total_trx,sum(purchase) as rev_purchase,sum(idv_voucher) as rev_voucher,sum(rev_redeem) as rev_redeem,
  sum(rev_spend) as rev_spend,sum(trx_purchase) as trx_purchase,sum(trx_voucher) as trx_voucher,sum(trx_redeem) as trx_redeem,
  sum(trx_spend) as trx_spend from gamesott.ndi_idv_monthly
  where type='idv'
  union all
  select sum(total_trx) as total_trx,sum(purchase) as rev_purchase,sum(idv_voucher) as rev_voucher,sum(rev_redeem) as rev_redeem,
  sum(rev_spend) as rev_spend,sum(trx_purchase) as trx_purchase,sum(trx_voucher) as trx_voucher,sum(trx_redeem) as trx_redeem,
  sum(trx_spend) as trx_spend from alls
   where type='idv'
  ) """,
        "ndi": """ with 
  data_vcr as 
  ( select trx_id,amount,date_trunc(updated_at, MONTH) as bulan,t_id
    from gamesott.dvm 
    where status = 1 and DATE_TRUNC(date(updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH) 
  ),
  data_pay as
  (
    select 'idv' as type_trx,trx_id,amount,item,date_trunc(updated_at, MONTH) as bulan
    from gamesott.denormal_transaction
    where DATE_TRUNC(date(updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)  and upper(merchant) like '%IDV%' and accepted = 1
  ),
  data_idv_vcr as
  (
    select trx_id ,amount,item,date_trunc(updated_at, MONTH) as bulan
    from gamesott.idv_voucher
    where DATE_TRUNC(date(updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH) 
  )
,  reed_spen as (
      select dc.*, p.name publisher_name
from gamesott.idv_api dc join
gamesott.dtuumb_publisher p on dc.publisher_id = p.id
where DATE_TRUNC(date(dc.updated_at), MONTH) = DATE_TRUNC(CURRENT_DATE(), MONTH)  and dc.accepted = 1
    )
    ,redeem as (
      select 'idv' as type_trx,date_trunc(updated_at, MONTH) as bulan, sum(IF(product = 'idv_credit' OR product = 'idv_voucher', 1, 0)) as trx_redeem,sum(IF(product = 'idv_credit' OR product = 'idv_voucher', amount, 0)) as rev_redeem from reed_spen group by 1,2
    )
    ,spending as (
      select 'idv' as type_trx,date_trunc(updated_at, MONTH) as bulan, sum(IF(product = 'idv_credit' or product = 'idv_voucher', 0, 1)) as trx_spend,sum(IF(product = 'idv_credit' or product = 'idv_voucher', 0, amount)) as rev_spend from reed_spen group by 1,2
    )
, trx as (
  select p.bulan,count(p.trx_id) as total_trx,sum(p.amount) as purchase,sum(d.amount) as idv_voucher,
   count(p.trx_id) as trx_purchase,count(v.trx_id) as trx_voucher
   from data_pay p left outer join data_vcr v on p.trx_id=v.trx_id 
  left outer join data_idv_vcr d on v.t_id=d.trx_id 
  group by 1 order by 1
)
, alls  as (
select 'idv' as type,t.*,r.trx_redeem,r.rev_redeem,s.trx_spend,s.rev_spend from trx t left outer join redeem r on t.bulan=r.bulan
left outer join spending s on t.bulan=s.bulan
)
select  sum(total_trx) as total_trx,sum(rev_purchase) as rev_purchase,sum(rev_voucher) as rev_voucher,sum(rev_redeem) as rev_redeem,
  sum(rev_spend) as rev_spending,sum(trx_purchase) as trx_purchase,sum(trx_voucher) as trx_voucher,sum(trx_redeem) as trx_redeem,
  sum(trx_spend) as trx_spend from (
  select sum(total_trx) as total_trx,sum(purchase) as rev_purchase,sum(idv_voucher) as rev_voucher,sum(rev_redeem) as rev_redeem,
  sum(rev_spend) as rev_spend,sum(trx_purchase) as trx_purchase,sum(trx_voucher) as trx_voucher,sum(trx_redeem) as trx_redeem,
  sum(trx_spend) as trx_spend from gamesott.ndi_idv_monthly
  where type='ndi'
  union all
  select sum(total_trx) as total_trx,sum(purchase) as rev_purchase,sum(idv_voucher) as rev_voucher,sum(rev_redeem) as rev_redeem,
  sum(rev_spend) as rev_spend,sum(trx_purchase) as trx_purchase,sum(trx_voucher) as trx_voucher,sum(trx_redeem) as trx_redeem,
  sum(trx_spend) as trx_spend from gamesott.v_ndi_current_month
  ) """
    }

    # --- LOAD DATA ---
    df_main = run_query(SQL_QUERIES["main"])
    df_idv = run_query(SQL_QUERIES["idv"])
    df_ndi = run_query(SQL_QUERIES["ndi"])

    # Mengambil nilai untuk Metric Cards (Row 1)
    # Kita ambil dari df_main (biasanya hanya 1 baris hasil sum)
    row_main = df_main.iloc[0]

    # --- UI: HEADER ---
    st.markdown('<p class="main-title">Executive Performance Dashboard</p>', unsafe_allow_html=True)

    # --- ROW 1: METRIC CARDS ---
    m1, m2, m3, m4 = st.columns(4)
    
    # Fungsi pembantu format Rupiah sederhana
    def format_rp(val):
        return f"Rp {val/1e9:.2f} B" if val >= 1e9 else f"Rp {val:,.0f}"

    with m1:
        st.metric(label="TOTAL REVENUE (PURCHASE)", 
                  value=format_rp(row_main['rev_purchase']),
                  delta=f"{row_main['total_trx']:,} Trx")
    with m2:
        st.metric(label="TOTAL VOUCHER REVENUE", 
                  value=format_rp(row_main['rev_voucher']),
                  delta=f"{row_main['trx_voucher']:,} Trx")
    with m3:
        st.metric(label="REDEEM REVENUE", 
                  value=format_rp(row_main['rev_redeem']),
                  delta=f"{row_main['trx_redeem']:,} Trx")
    with m4:
        # Menghitung margin (sesuai query kamu: rev_voucher - rev_purchase)
        st.metric(label="TOTAL SPENDING", 
                  value=format_rp(row_main['rev_spending']),
                  delta=f"{row_main['trx_spend']:,} Trx")

    st.markdown("""
    <style>
    /* Menyembunyikan ikon panah pada delta metric */
    [data-testid="stMetricDelta"] svg {
        display: none;
    }
    /* Memastikan teks delta tetap berwarna hijau */
    [data-testid="stMetricDelta"] > div {
        color: #09ab3b !important;
    }
    </style>
    """, unsafe_allow_html=True)
    st.markdown("### IDV & NDI PERFORMANCE SUMMARY")

    # --- ROW 2: CHART & DETAIL ---
    col_chart, col_detail = st.columns([1, 2])

    with col_chart:
        # Grafik Donut Kontribusi (IDV vs NDI)
        rev_idv = df_idv['rev_purchase'].iloc[0]
        rev_ndi = df_ndi['rev_purchase'].iloc[0]
        
        fig = go.Figure(data=[go.Pie(
            labels=['IDV REVENUE', 'NDI REVENUE'], 
            values=[rev_idv, rev_ndi], 
            hole=.7,
            marker_colors=['#4C6EF5', '#FD7E14'],
            textinfo='percent',
            hoverinfo='label+value'
        )])
        
        # PERHATIKAN: Baris-baris di bawah ini harus sejajar (masuk ke dalam 'with col_chart')
        total_rev = rev_idv + rev_ndi

        fig.update_layout(
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5
            ),
            annotations=[dict(
                text=f'TOTAL<br><b>{format_rp(total_rev)}</b>', 
                x=0.5, y=0.5, 
                font_size=18, 
                showarrow=False
            )],
            margin=dict(t=0, b=50, l=0, r=0),
            height=400
        )

        st.plotly_chart(fig, use_container_width=True)

    with col_detail:
        # Rincian Unit Bisnis
        sub1, sub2 = st.columns(2)
        
        row_idv = df_idv.iloc[0]
        row_ndi = df_ndi.iloc[0]

        with sub1:
            with st.container(border=True):
                st.markdown("<p style='color: #4C6EF5; font-weight: bold;'>PERFORMA IDV</p>", unsafe_allow_html=True)
                st.write(f"Revenue: **{format_rp(row_idv['rev_purchase'])}**")
                st.write(f"Topup Revenue: **{format_rp(row_idv['rev_voucher'])}**")
                st.write(f"Saldo: **{format_rp(row_idv['rev_redeem'])}**")
                st.write(f"Total Spending: **{format_rp(row_idv['rev_spending'])}**")

        with sub2:
            with st.container(border=True):
                st.markdown("<p style='color: #FD7E14; font-weight: bold;'>PERFORMA NDI</p>", unsafe_allow_html=True)
                st.write(f"Revenue: **{format_rp(row_ndi['rev_purchase'])}**")
                st.write(f"Saldo: **{format_rp(row_ndi['rev_redeem'])}**")
                st.write(f"Total Spending: **{format_rp(row_ndi['rev_spending'])}**")

# Blok except ini harus sejajar dengan blok try di paling atas
except Exception as e:
    st.error(f"Gagal memuat data dari BigQuery: {e}")
