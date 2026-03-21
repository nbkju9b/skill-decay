import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path

GOLD_PATH = Path(__file__).parent.parent / "include/data/gold/skill_scores/part-00001.parquet"

st.set_page_config(
    page_title="SkillDecay — Skills Shelf Life in the AI Era",
    page_icon="⏳",
    layout="wide",
)

@st.cache_data
def load_data():
    df = pd.read_parquet(GOLD_PATH)
    df.columns = [c.lower() for c in df.columns]
    return df

df = load_data()

RISK_COLOR = {
    "low":      "#22c55e",
    "medium":   "#f59e0b",
    "high":     "#f97316",
    "critical": "#ef4444",
}

def doomsday_gauge(value: float, title: str, subtitle: str = "") -> go.Figure:
    if value < 25:
        bar_color = RISK_COLOR["low"]
    elif value < 50:
        bar_color = RISK_COLOR["medium"]
    elif value < 75:
        bar_color = RISK_COLOR["high"]
    else:
        bar_color = RISK_COLOR["critical"]

    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=round(value, 1),
        number={"suffix": "%", "font": {"size": 36}},
        title={"text": f"<b>{title}</b><br><span style='font-size:13px;color:#888'>{subtitle}</span>",
               "font": {"size": 16}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#555",
                     "tickvals": [0, 25, 50, 75, 100],
                     "ticktext": ["0", "25", "50", "75", "100"]},
            "bar": {"color": bar_color, "thickness": 0.25},
            "bgcolor": "#1e1e2e",
            "borderwidth": 0,
            "steps": [
                {"range": [0, 25],   "color": "#14532d"},
                {"range": [25, 50],  "color": "#713f12"},
                {"range": [50, 75],  "color": "#7c2d12"},
                {"range": [75, 100], "color": "#450a0a"},
            ],
            "threshold": {
                "line": {"color": "white", "width": 3},
                "thickness": 0.8,
                "value": value,
            },
        },
    ))
    fig.update_layout(
        height=280,
        margin=dict(t=60, b=10, l=20, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        font_color="#e2e8f0",
    )
    return fig

def risk_badge(tier: str) -> str:
    colors = {
        "low":      ("🟢", "#166534", "#dcfce7"),
        "medium":   ("🟡", "#854d0e", "#fef9c3"),
        "high":     ("🟠", "#7c2d12", "#ffedd5"),
        "critical": ("🔴", "#7f1d1d", "#fee2e2"),
    }
    icon, fg, bg = colors.get(tier.lower(), ("⚪", "#333", "#eee"))
    return f'<span style="background:{bg};color:{fg};padding:2px 10px;border-radius:12px;font-weight:600;font-size:13px">{icon} {tier.upper()}</span>'

def confidence_badge(level: str) -> str:
    colors = {
        "high":   ("#1e3a5f", "#dbeafe"),
        "medium": ("#3b2f00", "#fef3c7"),
        "low":    ("#3f0000", "#fee2e2"),
    }
    fg, bg = colors.get(level.lower(), ("#333", "#eee"))
    return f'<span style="background:{bg};color:{fg};padding:2px 10px;border-radius:12px;font-size:13px">{level.capitalize()} confidence</span>'


with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/hourglass.png", width=60)
    st.markdown("## ⏳ SkillDecay")
    st.caption("Skills Shelf Life in the AI Era")
    st.markdown("---")

    st.markdown(f"**{len(df):,}** skills scored · **{df['skill'].nunique():,}** unique")
    st.markdown("")

    risk_counts = df["risk_tier"].value_counts()
    for tier, color in RISK_COLOR.items():
        count = risk_counts.get(tier, 0)
        st.markdown(
            f'<div style="display:flex;justify-content:space-between;padding:3px 0">'
            f'<span style="color:{color};font-weight:600">{tier.upper()}</span>'
            f'<span>{count:,}</span></div>',
            unsafe_allow_html=True,
        )

    st.markdown("---")
    st.markdown("### How to read this")

    with st.expander("Doomsday Clock %"):
        st.markdown("""
0–100% score measuring how close a skill is to obsolescence, derived from employer job posting frequency.

- **0%** — highest demand in the dataset, not going anywhere
- **50%** — moderate demand, worth monitoring
- **100%** — near-zero demand, employer market has moved on
        """)

    with st.expander("Shelf Life (months)"):
        st.markdown("""
Estimated months a skill is likely to remain in employer demand.

- **120 months** — maximum (10 years), top-demand skill
- **60 months** — moderate, worth maintaining
- **< 24 months** — low demand, consider upskilling

Derived as `demand_score × 120`.
        """)

    with st.expander("Risk Tier"):
        st.markdown("""
Four-level classification based on doomsday clock %:

| Tier | Range | What it means |
|------|-------|---------------|
| 🟢 LOW | 0–25% | High demand — safe to invest in |
| 🟡 MEDIUM | 25–50% | Moderate demand — monitor closely |
| 🟠 HIGH | 50–75% | Declining demand — build alternatives |
| 🔴 CRITICAL | 75–100% | Very low demand — upskill urgently |
        """)

    with st.expander("Confidence"):
        st.markdown("""
How reliable the score is, based on number of job postings mentioning this skill.

| Level | Postings | Interpretation |
|-------|----------|----------------|
| High | ≥ 30 | Reliable signal |
| Medium | 10–29 | Indicative |
| Low | < 10 | Limited data — treat with caution |

Low-confidence skills are scored and flagged, not hidden.
        """)

    with st.expander("Demand Score"):
        st.markdown("""
Normalised 0.0–1.0 score representing relative skill demand.

Uses log compression to prevent dominant skills (e.g. Python) from collapsing all others to near-zero, then MinMax scaling to [0, 1].
        """)

    st.markdown("---")
    st.caption("Data: LinkedIn job postings 2023–24 · 10K sample · [GitHub](https://github.com/nbkju9b/skill-decay)")


tab1, tab2, tab3, tab4 = st.tabs(
    ["🕰️ Doomsday Clock", "🔍 Skill Search", "🏆 Leaderboard", "🛡️ Data Quality"]
)

with tab1:
    st.caption("Average doomsday score and risk distribution across all scored skills.")

    avg_doom  = df["doomsday_clock_pct"].mean()
    avg_shelf = df["shelf_life_months"].mean()

    col_gauge, col_metrics = st.columns([1, 1])

    with col_gauge:
        st.plotly_chart(
            doomsday_gauge(avg_doom, "Portfolio Average",
                           f"Avg shelf life: {avg_shelf:.0f} months"),
            use_container_width=True,
        )

    with col_metrics:
        st.markdown("### Portfolio Snapshot")
        m1, m2 = st.columns(2)
        m1.metric("Avg Doomsday %", f"{avg_doom:.1f}%")
        m2.metric("Avg Shelf Life", f"{avg_shelf:.0f} mo")

        m3, m4 = st.columns(2)
        m3.metric("Critical Skills", f"{risk_counts.get('critical', 0):,}")
        m4.metric("Low-Risk Skills", f"{risk_counts.get('low', 0):,}")

        st.markdown("---")
        fig_donut = go.Figure(go.Pie(
            labels=[t.upper() for t in RISK_COLOR.keys()],
            values=[risk_counts.get(t, 0) for t in RISK_COLOR.keys()],
            hole=0.55,
            marker_colors=list(RISK_COLOR.values()),
            textinfo="label+percent",
            textfont_size=12,
        ))
        fig_donut.update_layout(
            height=220,
            margin=dict(t=10, b=10, l=0, r=0),
            showlegend=False,
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#e2e8f0",
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    st.markdown("---")
    st.markdown("#### Shelf Life Distribution by Risk Tier")
    fig_hist = px.histogram(
        df,
        x="shelf_life_months",
        color="risk_tier",
        nbins=40,
        color_discrete_map=RISK_COLOR,
        labels={"shelf_life_months": "Shelf Life (months)", "count": "Skills"},
        category_orders={"risk_tier": ["low", "medium", "high", "critical"]},
    )
    fig_hist.update_layout(
        height=300,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#e2e8f0",
        bargap=0.05,
        legend_title_text="Risk Tier",
        xaxis=dict(gridcolor="#334155"),
        yaxis=dict(gridcolor="#334155"),
    )
    st.plotly_chart(fig_hist, use_container_width=True)


with tab2:
    st.caption("Search any skill to see its shelf life, doomsday score, and risk tier.")

    search_col, _ = st.columns([2, 1])
    with search_col:
        query = st.text_input("Skill name (e.g. python, kubernetes, cobol)", "")

    if query.strip():
        q = query.strip().lower()
        results = df[df["skill"].str.lower().str.contains(q, na=False)]

        if results.empty:
            st.markdown(f"No results for **{query}**.")
        else:
            top = results.sort_values("job_count", ascending=False).iloc[0]

            left, right = st.columns([1, 1])

            with left:
                st.plotly_chart(
                    doomsday_gauge(
                        top["doomsday_clock_pct"],
                        top["skill"].upper(),
                        f"{int(top['job_count'])} jobs · {top['shelf_life_months']:.0f} mo shelf life",
                    ),
                    use_container_width=True,
                )

            with right:
                st.markdown(f"### {top['skill'].upper()}")
                st.markdown(
                    risk_badge(top["risk_tier"]) + "  " + confidence_badge(top["confidence"]),
                    unsafe_allow_html=True,
                )
                st.markdown("")

                r1, r2, r3 = st.columns(3)
                r1.metric("Job Count",    f"{int(top['job_count'])}")
                r2.metric("Shelf Life",   f"{top['shelf_life_months']:.0f} mo")
                r3.metric("Doomsday",     f"{top['doomsday_clock_pct']:.1f}%")

                r4, r5 = st.columns(2)
                r4.metric("Demand Score", f"{top['demand_score']:.3f}")
                r5.metric("Scaled Score", f"{top['scaled_score']:.3f}" if "scaled_score" in top else "—")

            if len(results) > 1:
                st.markdown("---")
                st.markdown(f"#### All matches for **{query}** ({len(results)} skills)")
                display_cols = [c for c in ["skill", "job_count", "shelf_life_months",
                                            "doomsday_clock_pct", "risk_tier", "confidence"]
                                if c in results.columns]
                st.dataframe(
                    results[display_cols]
                        .sort_values("job_count", ascending=False)
                        .reset_index(drop=True),
                    use_container_width=True,
                    hide_index=True,
                )
    else:
        st.markdown("Enter a skill name above to see its decay profile.")


with tab3:
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        leaderboard_mode = st.selectbox(
            "Rank by",
            ["Top demand (safest)", "Highest doomsday risk", "Longest shelf life", "Shortest shelf life"]
        )
    with filter_col2:
        min_confidence = st.selectbox("Min confidence", ["All", "medium+", "high only"])
    with filter_col3:
        top_n = st.slider("Show top N", 5, 50, 20)

    filtered = df.copy()
    if min_confidence == "medium+":
        filtered = filtered[filtered["confidence"].isin(["medium", "high"])]
    elif min_confidence == "high only":
        filtered = filtered[filtered["confidence"] == "high"]

    sort_map = {
        "Top demand (safest)":   ("job_count",          False),
        "Highest doomsday risk": ("doomsday_clock_pct", False),
        "Longest shelf life":    ("shelf_life_months",  False),
        "Shortest shelf life":   ("shelf_life_months",  True),
    }
    sort_col, ascending = sort_map[leaderboard_mode]
    top_skills = (filtered
                  .sort_values(sort_col, ascending=ascending)
                  .head(top_n)
                  .reset_index(drop=True))
    top_skills.index += 1

    bar_color_col = top_skills["risk_tier"].map(RISK_COLOR)
    fig_bar = go.Figure(go.Bar(
        x=top_skills[sort_col],
        y=top_skills["skill"],
        orientation="h",
        marker_color=bar_color_col,
        text=top_skills[sort_col].round(1),
        textposition="outside",
    ))
    fig_bar.update_layout(
        height=max(300, top_n * 20),
        xaxis_title=sort_col.replace("_", " ").title(),
        yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#e2e8f0",
        margin=dict(l=10, r=60, t=20, b=20),
        xaxis=dict(gridcolor="#334155"),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    display_cols = [c for c in ["skill", "job_count", "shelf_life_months",
                                "doomsday_clock_pct", "risk_tier", "confidence"]
                    if c in top_skills.columns]
    st.dataframe(top_skills[display_cols], use_container_width=True)


with tab4:
    st.caption("Automated checks on the Gold layer. Source: LinkedIn job postings 2023–24, 10K sample.")

    total       = len(df)
    null_counts = df.isnull().sum()
    has_nulls   = null_counts[null_counts > 0]

    q_col1, q_col2, q_col3, q_col4 = st.columns(4)
    q_col1.metric("Total rows",         f"{total:,}")
    q_col2.metric("Unique skills",      f"{df['skill'].nunique():,}")
    q_col3.metric("Columns with nulls", f"{len(has_nulls)}")
    q_col4.metric("Null cells total",   f"{null_counts.sum():,}")

    st.markdown("---")

    left_q, right_q = st.columns(2)

    with left_q:
        st.markdown("#### Confidence Distribution")
        conf_counts = df["confidence"].value_counts().reset_index()
        conf_counts.columns = ["confidence", "count"]
        conf_color_map = {"high": "#3b82f6", "medium": "#f59e0b", "low": "#ef4444"}
        fig_conf = px.bar(
            conf_counts,
            x="confidence", y="count",
            color="confidence",
            color_discrete_map=conf_color_map,
            text="count",
        )
        fig_conf.update_traces(textposition="outside")
        fig_conf.update_layout(
            height=260,
            showlegend=False,
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e2e8f0",
            xaxis=dict(gridcolor="#334155"),
            yaxis=dict(gridcolor="#334155"),
            margin=dict(t=10, b=10),
        )
        st.plotly_chart(fig_conf, use_container_width=True)

        conf_pct = (df["confidence"].value_counts(normalize=True) * 100).round(1)
        st.dataframe(
            conf_pct.reset_index().rename(columns={"confidence": "Level", "proportion": "% of skills"}),
            use_container_width=True,
            hide_index=True,
        )

    with right_q:
        st.markdown("#### Score Range Checks")

        checks = {
            "demand_score in [0, 1]":        df["demand_score"].between(0, 1).all(),
            "doomsday_clock_pct in [0, 100]": df["doomsday_clock_pct"].between(0, 100).all(),
            "shelf_life_months > 0":          (df["shelf_life_months"] > 0).all(),
            "job_count >= 1":                 (df["job_count"] >= 1).all(),
            "No duplicate skills":            df["skill"].duplicated().sum() == 0,
            "risk_tier values valid":         df["risk_tier"].isin(["low", "medium", "high", "critical"]).all(),
            "confidence values valid":        df["confidence"].isin(["low", "medium", "high"]).all(),
        }

        for check_name, passed in checks.items():
            color  = "#22c55e" if passed else "#ef4444"
            result = "PASS" if passed else "FAIL"
            st.markdown(
                f'<div style="display:flex;justify-content:space-between;padding:6px 0;'
                f'border-bottom:1px solid #1e293b">'
                f'<span style="font-size:13px">{check_name}</span>'
                f'<span style="color:{color};font-weight:700">{result}</span>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown("---")
    st.markdown("#### Column Summary")
    summary = df.describe(include="all").T
    summary.index.name = "column"
    st.dataframe(summary, use_container_width=True)

    st.markdown("---")
    st.markdown("#### Null Audit")
    if len(has_nulls) == 0:
        st.markdown("No null values found across any column.")
    else:
        st.markdown(f"{len(has_nulls)} column(s) contain nulls:")
        st.dataframe(
            has_nulls.reset_index().rename(columns={"index": "column", 0: "null_count"}),
            use_container_width=True,
            hide_index=True,
        )