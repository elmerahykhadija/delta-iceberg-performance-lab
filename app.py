import streamlit as st
import pandas as pd
import plotly.express as px
import os

# Configuration de la page
st.set_page_config(page_title="Lakehouse Benchmark", layout="wide")
st.title("📊 Benchmark Lakehouse : Delta Lake vs Apache Iceberg")

RESULTS_FILE = "data/benchmark_results.csv"

if not os.path.exists(RESULTS_FILE):
    st.warning(f"Le fichier {RESULTS_FILE} est introuvable. Exécute d'abord le benchmark Spark.")
    st.stop()

df = pd.read_csv(RESULTS_FILE)

# Calcul des moyennes pour lisser les variations
df_avg = df.groupby(['format', 'experiment', 'operation'])['execution_time_sec'].mean().reset_index()

st.subheader("Temps d'exécution moyen")

# Création du graphique interactif avec Plotly
fig = px.bar(
    df_avg,
    x="operation",
    y="execution_time_sec",
    color="format",
    barmode="group",
    text="execution_time_sec",
    color_discrete_map={"Delta": "#1f77b4", "Iceberg": "#ff7f0e"},
    labels={"operation": "Opération", "execution_time_sec": "Temps (s)", "format": "Format"}
)

fig.update_traces(texttemplate='%{text:.2f}s', textposition='outside')
fig.update_layout(yaxis_title="Temps d'exécution (Secondes)", uniformtext_minsize=8)

st.plotly_chart(fig, use_container_width=True)

st.subheader("Comparaison détaillée de toutes les exécutions (Runs)")

# Création d'un tableau croisé (pivot) pour comparer Delta et Iceberg sur CHAQUE exécution
pivot_df = df.pivot_table(index=['experiment', 'operation', 'run_id'], columns='format', values='execution_time_sec').reset_index()

# Calcul de la différence si les deux formats sont présents
if 'Delta' in pivot_df.columns and 'Iceberg' in pivot_df.columns:
    pivot_df['Plus Rapide'] = pivot_df.apply(lambda row: 'Iceberg 🏆' if pd.notnull(row['Delta']) and pd.notnull(row['Iceberg']) and row['Delta'] > row['Iceberg'] else ('Delta 🏆' if pd.notnull(row['Delta']) and pd.notnull(row['Iceberg']) else ''), axis=1)
    pivot_df['Différence (s)'] = abs(pivot_df['Delta'] - pivot_df['Iceberg']).round(4)

# Formatage des colonnes numériques pour un meilleur affichage
st.dataframe(pivot_df.style.format(precision=4), use_container_width=True)

with st.expander("Voir tout l'historique (Résultats bruts)"):
    st.dataframe(df, use_container_width=True)