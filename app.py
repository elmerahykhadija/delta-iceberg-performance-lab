import os

import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Configuration de la page
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Lakehouse Benchmark : Delta vs Iceberg", layout="wide", page_icon="📊")

RESULTS_FILE = "data/benchmark_results.csv"
STORAGE_FILE = "data/storage_metrics.csv"

COLORS = {"Delta": "#1f77b4", "Iceberg": "#ff7f0e"}

# Métadonnées d'affichage des expériences
OP_STYLE = {
    "READ BENCHMARK":        ("🔎", "Lecture complète (Full Scan)"),
    "FILTER BENCHMARK":      ("🔍", "Filtrage (Data Skipping)"),
    "AGGREGATION BENCHMARK": ("📈", "Agrégation (GROUP BY + SUM)"),
    "UPDATE BENCHMARK":      ("✏️", "Mise à jour (UPDATE ACID)"),
    "DELETE BENCHMARK":      ("🗑️", "Suppression (DELETE ACID)"),
}

VERB = {
    "READ BENCHMARK":        "Nous avons lu intégralement la table",
    "FILTER BENCHMARK":      "Nous avons compté les lignes filtrées (VendorID = 1)",
    "AGGREGATION BENCHMARK": "Nous avons agrégé les lignes par passenger_count (SUM total_amount)",
    "UPDATE BENCHMARK":      "Nous avons modifié la valeur",
    "DELETE BENCHMARK":      "Nous avons supprimé les lignes dont",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def short_op(exp, op):
    """Version courte & lisible d'une opération pour les graphiques."""
    if exp == "UPDATE BENCHMARK":
        return op.replace("Update payment_type ", "").replace(" (VendorID=2)", "")
    if exp == "DELETE BENCHMARK":
        return op.replace("Delete payment_type ", "")
    return op


def fmt_int(v):
    try:
        return f"{int(v):,}".replace(",", " ")
    except (TypeError, ValueError):
        return "—"


def sentence(exp, op, d, i, rows):
    """Phrase de synthèse façon 'nous avons modifié X en Y → résultats q et b'."""
    d, i = float(d), float(i)
    if exp == "UPDATE BENCHMARK":
        val = op.split("→")[1].strip().split(" ")[0] if "→" in op else "?"
        detail = f"payment_type → {val} sur {fmt_int(rows)} lignes où VendorID = 2" if pd.notna(rows) else f"payment_type → {val} sur les lignes où VendorID = 2"
    elif exp == "DELETE BENCHMARK":
        val = op.split("==")[1].strip() if "==" in op else "?"
        detail = f"payment_type = {val} ({fmt_int(rows)} lignes)" if pd.notna(rows) else f"payment_type = {val}"
    elif exp == "READ BENCHMARK":
        detail = f"({fmt_int(rows)} lignes)" if pd.notna(rows) else ""
    elif exp == "FILTER BENCHMARK":
        detail = f"partout dans la table ({fmt_int(rows)} lignes lues)" if pd.notna(rows) else ""
    else:
        detail = ""

    if abs(d - i) < 1e-9:
        return f"➡️ {VERB[exp]}{' ' + detail if detail else ''} : **Delta = {d:.2f} s** et **Iceberg = {i:.2f} s** → résultats **identiques**."

    winner, loser = ("Delta", "Iceberg") if d < i else ("Iceberg", "Delta")
    diff = abs(d - i)
    ratio = max(d, i) / min(d, i)
    return (
        f"➡️ {VERB[exp]}{' ' + detail if detail else ''} : **Delta = {d:.2f} s**, **Iceberg = {i:.2f} s** → "
        f"**{winner} 🏆 est {diff:.2f} s plus rapide** (échelle {ratio:.1f}×)."
    )


def winner_col(d, i):
    if pd.isna(d) or pd.isna(i):
        return "Donnée manquante"
    if abs(float(d) - float(i)) < 1e-9:
        return "Égalité"
    return "Iceberg 🏆" if d > i else "Delta 🏆"


@st.cache_data
def load_data():
    df = pd.read_csv(RESULTS_FILE)
    df["rows_processed"] = df["rows_processed"].replace("", pd.NA)
    return df


@st.cache_data
def load_storage():
    if not os.path.exists(STORAGE_FILE):
        return None
    return pd.read_csv(STORAGE_FILE)


# ---------------------------------------------------------------------------
# Chargement des données
# ---------------------------------------------------------------------------
if not os.path.exists(RESULTS_FILE):
    st.warning(f"Le fichier {RESULTS_FILE} est introuvable. Exécute d'abord le benchmark Spark.")
    st.stop()

df = load_data()

# Moyenne par opération et par format
mean_df = df.groupby(["experiment", "operation", "format"])["execution_time_sec"].mean().unstack("format").reset_index()
rows_map = df.groupby(["experiment", "operation"])["rows_processed"].first()

# Données par run (pour les tableaux détaillés)
runs_df = df.pivot_table(index=["experiment", "operation", "run_id"], columns="format", values="execution_time_sec").reset_index()

# ---------------------------------------------------------------------------
# EN-TÊTE
# ---------------------------------------------------------------------------
st.title("📊 Benchmark Lakehouse : Delta Lake vs Apache Iceberg")
st.caption("Même moteur (Apache Spark 3.5), mêmes données (NYC Taxi, 26 173 246 lignes), même stockage (MinIO/S3), cache vidé avant chaque mesure, 3 exécutions par opération.")

m1, m2, m3, m4 = st.columns(4)
m1.metric("🛢️ Lignes", fmt_int(df[df["experiment"] == "READ BENCHMARK"]["rows_processed"].dropna().max()))
m2.metric("⚙️ Opérations", df["operation"].nunique())
m3.metric("🔁 Runs / opération", int(df["run_id"].max()))
m4.metric("📁 Formats", "Delta & Iceberg")

st.markdown("---")

# ---------------------------------------------------------------------------
# 1. VUE D'ENSEMBLE
# ---------------------------------------------------------------------------
st.subheader("📊 Vue d'ensemble — temps moyen par opération")

over = df.groupby(["experiment", "operation", "format"])["execution_time_sec"].mean().reset_index()
over["operation"] = over.apply(lambda r: short_op(r["experiment"], r["operation"]), axis=1)
over["label"] = over["experiment"].map(lambda e: " ".join(OP_STYLE[e][1].split(" (")[0])) + " — " + over["operation"]

fig = px.bar(
    over,
    x="operation",
    y="execution_time_sec",
    color="format",
    barmode="group",
    text="execution_time_sec",
    color_discrete_map=COLORS,
    hover_data={"label": True, "experiment": True, "operation": True},
    labels={"operation": "Opération", "execution_time_sec": "Temps moyen (s)", "format": "Format"},
)
fig.update_traces(texttemplate="%{text:.2f}s", textposition="outside")
fig.update_layout(yaxis_title="Temps d'exécution (Secondes)", uniformtext_minsize=8, height=420,
                  xaxis=dict(tickangle=-25))
st.plotly_chart(fig, width="stretch")

# Tableau récapitulatif global
summary = []
for _, r in mean_df.iterrows():
    d, i = r.get("Delta", pd.NA), r.get("Iceberg", pd.NA)
    diff = abs(float(d) - float(i)) if pd.notna(d) and pd.notna(i) else None
    ratio = (max(float(d), float(i)) / min(float(d), float(i))) if pd.notna(d) and pd.notna(i) and min(float(d), float(i)) > 0 else None
    summary.append({
        "Expérience": r["experiment"],
        "Opération": short_op(r["experiment"], r["operation"]),
        "Delta (s)": round(float(d), 2) if pd.notna(d) else None,
        "Iceberg (s)": round(float(i), 2) if pd.notna(i) else None,
        "🏆 Vainqueur": winner_col(d, i),
        "Écart (s)": round(diff, 2) if diff is not None else None,
        "Gain (×)": round(ratio, 2) if ratio is not None else None,
        "Lignes": fmt_int(rows_map.get((r["experiment"], r["operation"]))),
    })

st.dataframe(
    pd.DataFrame(summary),
    width="stretch",
    column_config={
        "Delta (s)": st.column_config.NumberColumn(format="%.2f"),
        "Iceberg (s)": st.column_config.NumberColumn(format="%.2f"),
        "Écart (s)": st.column_config.NumberColumn(format="%.2f"),
        "Gain (×)": st.column_config.NumberColumn(format="%.2f"),
    },
    hide_index=True,
)

# Bilan global (nombre de victoires)
wins = pd.DataFrame(summary)
n_delta = int((wins["🏆 Vainqueur"] == "Delta 🏆").sum())
n_iceberg = int((wins["🏆 Vainqueur"] == "Iceberg 🏆").sum())
st.info(
    f"🧮 **Bilan des lectures & écritures mesurées** : Delta décroche **{n_delta} opération(s)**, "
    f"Iceberg décroche **{n_iceberg} opération(s)**. "
    f"Chaque ligne du tableau ci-dessus est indépendante et peut être analysée en détail ci-dessous."
)

st.markdown("---")

# ---------------------------------------------------------------------------
# 2. ANALYSE DÉTAILLÉE PAR OPÉRATION (expliquée, comparee, structurée)
# ---------------------------------------------------------------------------
st.subheader("⚙️ Analyse détaillée : chaque opération expliquée et comparée")

experiment_order = ["READ BENCHMARK", "FILTER BENCHMARK", "AGGREGATION BENCHMARK", "UPDATE BENCHMARK", "DELETE BENCHMARK"]

for exp in experiment_order:
    edf = mean_df[mean_df["experiment"] == exp]
    if edf.empty:
        continue

    # Ordre logique : on trie les opérations selon leur run d'apparition (99→100→101 …)
    op_run = runs_df.drop_duplicates(subset=["experiment", "operation"]).set_index(["experiment", "operation"])["run_id"]
    edf["_run"] = edf.apply(lambda r: int(op_run.get((r["experiment"], r["operation"]), 99)), axis=1)
    edf = edf.sort_values("_run")

    emoji, friendly = OP_STYLE[exp]
    st.markdown(f"### {emoji} {exp} — {friendly}")

    # --- Barre d'opérations de l'expérience ---
    for _, r in edf.iterrows():
        op = r["operation"]
        d, i = r.get("Delta", pd.NA), r.get("Iceberg", pd.NA)
        rows = rows_map.get((exp, op))

        with st.container(border=True):
            st.markdown(f"**Opération : {short_op(exp, op)}**")

            # Explication détaillée de l'opération
            desc = df[(df["experiment"] == exp) & (df["operation"] == op)]["description"].dropna().iloc[0]
            with st.expander("ℹ️ Que fait cette opération ?", expanded=False):
                st.markdown(desc)

            cols = st.columns([2.2, 1])
            with cols[0]:
                # Tableau des runs Delta vs Iceberg (robuste quel que soit le nb de runs)
                sub = runs_df[(runs_df["experiment"] == exp) & (runs_df["operation"] == op)]
                run_rows = []
                for fmt_name in ["Delta", "Iceberg"]:
                    fsub = sub[["run_id", fmt_name]]
                    rec = {"Format": fmt_name}
                    for run in sorted(fsub["run_id"].dropna().unique()):
                        rec[f"Run {int(run)} (s)"] = fsub[fsub["run_id"] == run][fmt_name].iloc[0]
                    rec["Moyenne (s)"] = fsub[fmt_name].mean()
                    run_rows.append(rec)
                runs_tab = pd.DataFrame(run_rows)

                col_cfg = {"Format": st.column_config.TextColumn("Format")}
                for c in runs_tab.columns:
                    if c != "Format":
                        col_cfg[c] = st.column_config.NumberColumn(format="%.2f s")

                st.dataframe(
                    runs_tab.style.format(precision=2),
                    width="stretch",
                    hide_index=True,
                    column_config=col_cfg,
                )

                # Phrase de synthèse (résultats q et b)
                if pd.notna(d) and pd.notna(i):
                    st.success(sentence(exp, op, d, i, rows))

            with cols[1]:
                # Mini barres comparatives
                bd = over[over["experiment"] == exp]
                op_short = short_op(exp, op)
                bsub = bd[bd["operation"] == op_short]
                if not bsub.empty:
                    fig_mini = px.bar(
                        bsub,
                        x="format",
                        y="execution_time_sec",
                        color="format",
                        text="execution_time_sec",
                        color_discrete_map=COLORS,
                        labels={"format": "", "execution_time_sec": "Temps (s)"},
                    )
                    fig_mini.update_traces(texttemplate="%{text:.2f}s", textposition="outside")
                    fig_mini.update_layout(height=220, showlegend=False, margin=dict(l=10, r=10, t=20, b=10),
                                           yaxis_title=None)
                    st.plotly_chart(fig_mini, width="stretch")

    st.markdown("---")

# ---------------------------------------------------------------------------
# 3. EMPREINTE DE STOCKAGE
# ---------------------------------------------------------------------------
st.subheader("📦 Empreinte de stockage : données vs métadonnées")
df_storage = load_storage()

if df_storage is not None:
    fig_store = px.bar(
        df_storage,
        x="Format",
        y="Size",
        color="Type",
        barmode="group",
        title="Overhead des métadonnées (après les cycles UPDATE / DELETE du benchmark)",
        labels={"Size": "Taille (MB)", "Format": "Format de table"},
        color_discrete_map={"Data (MB)": "#2ca02c", "Metadata (MB)": "#d62728"},
    )
    fig_store.update_traces(texttemplate="%{text:.1f} Mo", textposition="outside")
    st.plotly_chart(fig_store, width="stretch")

    pivot_store = df_storage.pivot(index="Format", columns="Type", values="Size").reset_index()
    pivot_store["Data (Mo)"] = pivot_store["Data (MB)"].round(1)
    pivot_store["Metadata (Ko)"] = (pivot_store["Metadata (MB)"] * 1024).round(1)
    pivot_store["Overhead métadonnées (%)"] = (
        pivot_store["Metadata (MB)"] / pivot_store["Data (MB)"] * 100
    ).round(4)
    st.dataframe(
        pivot_store[["Format", "Data (Mo)", "Metadata (Ko)", "Overhead métadonnées (%)"]],
        width="stretch",
        hide_index=True,
    )

    st.info(
        "💡 **Analyse contextuelle** : Delta garde dans son dossier l'historique des fichiers "
        "« écrasés » par les UPDATE/DELETE jusqu'à un `VACUUM` (spéculation : les anciens datatiles "
        "restent physiquement présents), d'où une empreinte data plus lourde que celle d'Iceberg "
        "après les cycles d'écriture. À l'inverse, les métadonnées d'Iceberg (manifests + snapshots) "
        "sont plus volumineuses que le simple `_delta_log` de Delta. Physiologiquement, un Lakehouse "
        "efficace doit contrôler ces deux postes."
    )
else:
    st.info("Exécutez `storage_analysis.py` pour générer les métriques de stockage.")

st.markdown("---")

# ---------------------------------------------------------------------------
# 4. DONNÉES BRUTES
# ---------------------------------------------------------------------------
with st.expander("🗃️ Voir l'historique complet (résultats bruts)"):
    st.dataframe(df, width="stretch", hide_index=True)