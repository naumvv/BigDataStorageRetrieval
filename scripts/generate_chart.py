"""Generate benchmark bar chart similar to Fig. 7 of the reference paper."""

from __future__ import annotations

import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

# ── Data ────────────────────────────────────────────────────────────────────
# mean / std in seconds; None = timed-out (not measured)
data = {
    "Q1": {
        "PostgreSQL": (12.564, 0.448),
        "MongoDB":    None,               # killed after >10 min
        "Neo4j":      (37.05,  3.57),
    },
    "Q2": {
        "PostgreSQL": (1.030,  0.007),
        "MongoDB":    (7.718,  0.366),
        "Neo4j":      (8,2,  0.4),
    },
    "Q3": {
        "PostgreSQL": (0.978,  0.023),
        "MongoDB":    (7.440,  0.043),
        "Neo4j":      (7.8,  0.08),
    },
}

queries   = ["Q1", "Q2", "Q3"]
databases = ["PostgreSQL", "MongoDB", "Neo4j"]

colors = {
    "PostgreSQL": "#6272a4",   # muted blue-violet
    "MongoDB":    "#e06c75",   # muted red
    "Neo4j":      "#c8a97e",   # muted tan/gold
}
hatch_timeout = "////"   # diagonal hatching for timeout bar

# ── Layout ──────────────────────────────────────────────────────────────────
n_queries   = len(queries)
n_databases = len(databases)
bar_w   = 0.22
group_w = bar_w * n_databases + 0.12
x_centers = np.arange(n_queries) * group_w

fig, ax = plt.subplots(figsize=(10, 5.5))
ax.set_facecolor("#f8f8f8")
fig.patch.set_facecolor("white")

for di, db in enumerate(databases):
    offsets = x_centers + (di - 1) * bar_w   # centre the three bars per group

    means  = []
    errors = []
    hatches = []
    for q in queries:
        entry = data[q][db]
        if entry is None:
            means.append(0)
            errors.append(0)
            hatches.append(hatch_timeout)
        else:
            means.append(entry[0])
            errors.append(entry[1])
            hatches.append("")

    for i, (x, m, e, h) in enumerate(zip(offsets, means, errors, hatches)):
        is_timeout = (h == hatch_timeout)
        bar = ax.bar(
            x, m if not is_timeout else 1.0,
            width=bar_w,
            color=colors[db],
            alpha=0.85 if not is_timeout else 0.30,
            hatch=h,
            edgecolor="white" if not is_timeout else colors[db],
            linewidth=0.6,
            capsize=4,
        )
        if e > 0 and not is_timeout:
            ax.errorbar(x, m, yerr=e, fmt="none", color="black",
                        linewidth=0.8, capsize=3, capthick=0.8)

        # ── Value label ──────────────────────────────────────────────────
        if is_timeout:
            ax.text(x, 1.25, "timeout", ha="center", va="bottom",
                    fontsize=7.5, color=colors[db], style="italic", rotation=0)
        else:
            label = f"{m:.2f}s"
            ax.text(x, m + e + 0.25, label, ha="center", va="bottom",
                    fontsize=7.5, fontweight="bold", color="#333333")

# ── Axes styling ─────────────────────────────────────────────────────────────
ax.set_xticks(x_centers)
ax.set_xticklabels(["Query 1\n(Campaign effectiveness\n& social targeting)",
                     "Query 2\n(Product\nrecommendations)",
                     "Query 3\n(Full-text\nsearch)"],
                   fontsize=9)
ax.set_ylabel("Mean execution time (seconds)", fontsize=10)
ax.set_xlabel("Query", fontsize=10)
ax.set_title("Query Execution Times — PostgreSQL vs MongoDB vs Neo4j\n"
             "(5 runs each, error bars = ±1 std dev)", fontsize=11, pad=12)

ax.set_xlim(x_centers[0] - group_w * 0.5, x_centers[-1] + group_w * 0.5)
ymax = max(
    v[0] + v[1] for q in queries for db in databases
    if (v := data[q][db]) is not None
)
ax.set_ylim(0, ymax * 1.25)
ax.yaxis.grid(True, linestyle="--", linewidth=0.5, alpha=0.7, color="#cccccc")
ax.set_axisbelow(True)
for spine in ["top", "right"]:
    ax.spines[spine].set_visible(False)

# ── Legend ───────────────────────────────────────────────────────────────────
patches = [mpatches.Patch(facecolor=colors[db], label=db, alpha=0.85)
           for db in databases]
timeout_patch = mpatches.Patch(facecolor=colors["MongoDB"], alpha=0.30,
                                hatch=hatch_timeout,
                                edgecolor=colors["MongoDB"],
                                label="MongoDB Q1 (timeout >10 min)")
ax.legend(handles=patches + [timeout_patch], loc="upper right",
          fontsize=9, framealpha=0.9)

fig.tight_layout()

out_path = os.path.join(os.path.dirname(__file__), "..", "figures", "benchmark_chart.png")
os.makedirs(os.path.dirname(out_path), exist_ok=True)
fig.savefig(out_path, dpi=180, bbox_inches="tight")
print(f"Chart saved to {os.path.abspath(out_path)}")
