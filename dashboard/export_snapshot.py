"""Render every dashboard view as a self-contained HTML snapshot.

The Streamlit dashboard needs a live server. This script produces a single
HTML file embedding the SAME Plotly figures the dashboard uses, so you can
open it in any browser, send it as an email attachment, or check it into
a PR for review.

Run from the repo root:

    .venv/bin/python post_processing_dashboard/export_snapshot.py

Outputs:
    post_processing_dashboard/snapshots/dashboard_snapshot.html
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

# Make data_loader importable when called from repo root
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


# ─────────────────────────── parquet loaders ───────────────────────────
def _read(path: Path) -> pd.DataFrame:
    files = list(path.glob("**/*.parquet"))
    if not files:
        return pd.DataFrame()
    frames = []
    for f in files:
        df = pd.read_parquet(f)
        # Re-attach partition columns from the directory path
        for p in f.parts:
            if "=" in p:
                k, v = p.split("=", 1)
                if k not in df.columns:
                    df[k] = v
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _ensure_cell_label(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "cell_no" not in df.columns:
        return df
    df = df.copy()
    df["cell_label"] = (df.get("make", "?").astype(str) + "_"
                        + df.get("batch", "?").astype(str) + "_"
                        + df["cell_no"].astype(str))
    return df


# ─────────────────────────── figure builders ───────────────────────────
HEIGHT = 420


def fig_hppc(df: pd.DataFrame) -> list[tuple[str, go.Figure]]:
    if df.empty:
        return []
    df = _ensure_cell_label(df)
    figs = []
    # 1) 2×3 panel: R0/R1/R2/C1/C2/R30s vs SoC — first cell only for clarity
    one = df.sort_values(["cell_label", "pulse_idx"]).groupby("cell_label").head(99)
    panel = make_subplots(
        rows=2, cols=3,
        subplot_titles=("R₀ (mΩ)", "R₁ (mΩ)", "R₂ (mΩ)",
                        "C₁ (F)", "C₂ (F)", "R₃₀ₛ (mΩ)"),
        shared_xaxes=True,
    )
    cols = [("R0_mOhm", 1, 1), ("R1_mOhm", 1, 2), ("R2_mOhm", 1, 3),
            ("C1_F",    2, 1), ("C2_F",    2, 2), ("R_30s_mOhm", 2, 3)]
    for label, sub in one.groupby("cell_label"):
        for ckey, r, c in cols:
            panel.add_trace(
                go.Scatter(x=sub["soc_start"], y=sub[ckey],
                           mode="lines+markers", name=label,
                           legendgroup=label,
                           showlegend=(ckey == "R0_mOhm")),
                row=r, col=c)
    for r in (1, 2):
        for c in (1, 2, 3):
            panel.update_xaxes(title_text="SoC", row=r, col=c, range=[0, 1])
    panel.update_layout(height=560, margin=dict(t=40, b=10, l=10, r=10),
                        legend=dict(orientation="h", y=-0.12))
    figs.append(("All ECM params vs SoC — every cell, every cohort", panel))

    # 2) Cohort compare on R0
    df_sorted = df.sort_values(["cell_label", "pulse_idx"])
    f2 = px.line(df_sorted, x="soc_start", y="R0_mOhm",
                 color="cell_label", markers=True,
                 labels={"soc_start": "SoC", "R0_mOhm": "R₀ (mΩ)",
                         "cell_label": "Cell"})
    f2.update_layout(height=HEIGHT, margin=dict(t=10, b=10, l=10, r=10))
    figs.append(("R₀ vs SoC — cohort compare", f2))
    return figs


def fig_ocv(df: pd.DataFrame) -> list[tuple[str, go.Figure]]:
    if df.empty:
        return []
    df = _ensure_cell_label(df).sort_values(["cell_label", "direction", "soc"])
    f = px.line(df, x="soc", y="v_oc", color="cell_label",
                line_dash="direction", markers=True,
                labels={"soc": "SoC", "v_oc": "V_OC (V)",
                        "cell_label": "Cell", "direction": "Direction"})
    f.update_layout(height=HEIGHT, margin=dict(t=10, b=10, l=10, r=10))
    return [("OCV(SoC) — charge / discharge overlay", f)]


def fig_dcir(df: pd.DataFrame) -> list[tuple[str, go.Figure]]:
    if df.empty:
        return []
    df = _ensure_cell_label(df).sort_values(["cell_label", "soc"])
    f = px.line(df, x="soc", y="r0_mohm", color="cell_label", markers=True,
                labels={"soc": "SoC", "r0_mohm": "R₀ (mΩ)", "cell_label": "Cell"})
    f.update_layout(height=HEIGHT, margin=dict(t=10, b=10, l=10, r=10))
    return [("DCIR — R₀ vs SoC", f)]


def fig_gitt(df: pd.DataFrame) -> list[tuple[str, go.Figure]]:
    if df.empty:
        return []
    df = _ensure_cell_label(df).sort_values(["cell_label", "pulse_idx"])
    panel = make_subplots(rows=1, cols=3,
                           subplot_titles=("R_pulse (mΩ)", "τ_diff (s)", "V_inf (V)"))
    for label, sub in df.groupby("cell_label"):
        for ckey, c in (("r_pulse_mohm", 1), ("tau_diff_s", 2), ("v_inf_v", 3)):
            panel.add_trace(
                go.Scatter(x=sub["soc"], y=sub[ckey],
                           mode="lines+markers", name=label,
                           legendgroup=label, showlegend=(c == 1)),
                row=1, col=c)
        for c in (1, 2, 3):
            panel.update_xaxes(title_text="SoC", row=1, col=c)
    panel.update_layout(height=HEIGHT, margin=dict(t=40, b=10, l=10, r=10))
    return [("GITT — R_pulse / τ_diff / V_inf vs SoC", panel)]


def fig_rate_cap(df: pd.DataFrame) -> list[tuple[str, go.Figure]]:
    if df.empty:
        return []
    df = _ensure_cell_label(df).sort_values(["cell_label", "direction", "c_rate"])
    f = px.line(df, x="c_rate", y="q_ah", color="cell_label",
                line_dash="direction", markers=True,
                labels={"c_rate": "C-rate", "q_ah": "Q (Ah)",
                        "cell_label": "Cell"})
    f.update_layout(height=HEIGHT, margin=dict(t=10, b=10, l=10, r=10))
    return [("Rate capability — Q vs C-rate", f)]


def fig_self_discharge(df: pd.DataFrame) -> list[tuple[str, go.Figure]]:
    if df.empty:
        return []
    df = _ensure_cell_label(df).sort_values("cell_label")
    fa = px.bar(df, x="cell_label", y="dv_dt_mV_per_h",
                labels={"cell_label": "Cell", "dv_dt_mV_per_h": "ΔV/Δt (mV/h)"})
    fa.update_layout(height=320, margin=dict(t=10, b=10, l=10, r=10))
    fb = px.bar(df, x="cell_label", y="retention_pct",
                labels={"cell_label": "Cell", "retention_pct": "Retention (%)"})
    fb.update_layout(height=320, margin=dict(t=10, b=10, l=10, r=10),
                     yaxis_range=[0, 110])
    return [("Self-discharge drift rate", fa),
            ("Capacity retention after rest", fb)]


def fig_peak_power(df: pd.DataFrame) -> list[tuple[str, go.Figure]]:
    if df.empty:
        return []
    df = _ensure_cell_label(df).sort_values(["cell_label", "direction", "soc"])
    f = px.line(df, x="soc", y="p_peak_w", color="cell_label",
                line_dash="direction", markers=True,
                labels={"soc": "SoC", "p_peak_w": "P_peak (W)",
                        "cell_label": "Cell"})
    f.update_layout(height=HEIGHT, margin=dict(t=10, b=10, l=10, r=10))
    return [("Peak power vs SoC", f)]


def fig_constant_power(df: pd.DataFrame) -> list[tuple[str, go.Figure]]:
    if df.empty:
        return []
    df = _ensure_cell_label(df).sort_values(["cell_label", "direction", "power_w"])
    f = px.line(df, x="power_w", y="energy_wh", color="cell_label",
                line_dash="direction", markers=True,
                labels={"power_w": "P (W)", "energy_wh": "Energy (Wh)",
                        "cell_label": "Cell"})
    f.update_layout(height=HEIGHT, margin=dict(t=10, b=10, l=10, r=10))
    return [("Constant power — energy vs power set-point", f)]


def fig_cycles_long(df: pd.DataFrame) -> list[tuple[str, go.Figure]]:
    if df.empty:
        return []
    df = _ensure_cell_label(df).copy()
    df["soh"] = df.groupby("cell_label")["dchg_cap_ah"].transform(
        lambda s: s / s.max() if s.max() and s.max() > 0 else s)
    f = px.line(df.sort_values(["cell_label", "cycle_no"]),
                x="cycle_no", y="soh", color="cell_label",
                labels={"cycle_no": "Cycle", "soh": "SoH",
                        "cell_label": "Cell"})
    f.add_hline(y=0.80, line_dash="dash", annotation_text="EOL = 0.80")
    f.update_layout(height=HEIGHT, margin=dict(t=10, b=10, l=10, r=10),
                    yaxis_tickformat=".0%")
    return [("Longterm — SoH trajectory (EoL = 0.80)", f)]


def fig_cycles_rpt(df: pd.DataFrame) -> list[tuple[str, go.Figure]]:
    if df.empty:
        return []
    df = _ensure_cell_label(df).sort_values(["cell_label", "cycle_no"])
    f = px.line(df, x="cycle_no", y="dchg_cap_ah", color="cell_label",
                markers=True,
                labels={"cycle_no": "Cycle", "dchg_cap_ah": "Q_dchg (Ah)",
                        "cell_label": "Cell"})
    f.update_layout(height=HEIGHT, margin=dict(t=10, b=10, l=10, r=10))
    return [("RPT — discharge capacity per cycle", f)]


# ─────────────────────────── HTML assembly ───────────────────────────
SECTIONS = [
    ("HPPC",            "HPPC",            fig_hppc),
    ("OCV",             "OCV(SoC)",        fig_ocv),
    ("DCIR",            "DCIR R₀",         fig_dcir),
    ("GITT",            "GITT",            fig_gitt),
    ("RATE_CAP",        "Rate capability", fig_rate_cap),
    ("SELF_DISCHARGE",  "Self-discharge",  fig_self_discharge),
    ("PEAK_POWER",      "Peak power",      fig_peak_power),
    ("CONSTANT_POWER",  "Constant power",  fig_constant_power),
    ("CYCLES_LONG",     "Longterm cycles", fig_cycles_long),
    ("CYCLES_RPT",      "RPT cycles",      fig_cycles_rpt),
]


def build(output_root: Path, target: Path) -> Path:
    parts = [
        "<!doctype html><html><head>",
        "<meta charset='utf-8'>",
        "<title>Battery post-processing snapshot</title>",
        "<style>",
        "  body { font: 14px/1.5 -apple-system, system-ui, sans-serif;",
        "         margin: 0; padding: 24px 40px; background: #fafafa; color: #222; }",
        "  h1 { margin: 0 0 4px 0; font-size: 24px; }",
        "  .lede { color: #666; margin-bottom: 24px; }",
        "  details { background: white; border: 1px solid #e3e3e3;",
        "            border-radius: 6px; margin-bottom: 16px; padding: 12px 18px; }",
        "  details summary { cursor: pointer; font-size: 18px; font-weight: 600; }",
        "  details[open] summary { margin-bottom: 12px; }",
        "  .caption { color: #888; margin: 8px 0 14px 0; }",
        "  .empty { color: #c00; font-style: italic; }",
        "</style>",
        "</head><body>",
        "<h1>Battery post-processing — dashboard snapshot</h1>",
        "<div class='lede'>Read-only HTML mirror of the Streamlit dashboard. ",
        "  Open in any browser; no Python or Streamlit needed.</div>",
    ]

    # First include_plotlyjs writes the full plotly JS inline; subsequent
    # calls omit it to keep the file small.
    first_fig = True
    for folder, title, builder in SECTIONS:
        df = _read(output_root / folder)
        figs = builder(df) if not df.empty else []
        parts.append(f"<details open><summary>{title}</summary>")
        parts.append(f"<div class='caption'>{folder}/ — "
                     f"{len(df):,} rows" + (f", "
                     f"{df['cell_no'].nunique()} cells"
                     if not df.empty and "cell_no" in df.columns else "") + "</div>")
        if not figs:
            parts.append("<div class='empty'>(no data)</div>")
        for caption, f in figs:
            parts.append(f"<h3 style='margin:18px 0 6px 0;'>{caption}</h3>")
            parts.append(pio.to_html(f,
                                     include_plotlyjs=("inline" if first_fig else False),
                                     full_html=False))
            first_fig = False
        parts.append("</details>")

    parts.append("</body></html>")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(parts))
    return target


def main() -> int:
    out = Path("post_processing_script/output")
    target = Path("post_processing_dashboard/snapshots/dashboard_snapshot.html")
    p = build(out, target)
    print(f"Wrote snapshot → {p} ({p.stat().st_size/1024:.1f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
