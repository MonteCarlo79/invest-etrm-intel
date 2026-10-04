import struct
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

ACCENT = "#1f6fb2"
SECOND = "#c0504d"
CJK_FONTS = ["PingFang SC", "Hiragino Sans GB", "Hiragino Mincho ProN", "Arial Unicode MS"]


def set_style() -> None:
    matplotlib.rcParams.update({
        "font.sans-serif": CJK_FONTS + ["sans-serif"],
        "axes.unicode_minus": False,
        "figure.figsize": (9, 5), "figure.dpi": 100,
        "axes.grid": True, "grid.alpha": 0.25, "axes.spines.top": False,
        "axes.spines.right": False, "font.size": 11})


def png_size(path) -> tuple:
    data = Path(path).read_bytes()
    w, h = struct.unpack(">II", data[16:24])
    return w, h


def _save(fig, out) -> Path:
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)
    return Path(out)


def price_duration_curve(csv, out, price_col="rt_price", title="") -> Path:
    s = pd.read_csv(csv)[price_col].dropna().sort_values(ascending=False).reset_index(drop=True)
    fig, ax = plt.subplots()
    ax.plot(s.index / max(len(s) - 1, 1) * 100, s.values, color=ACCENT, lw=1.4)
    ax.axhline(0, color="#888", lw=0.8)
    ax.set(xlabel="小时占比 (%)", ylabel="价格 (元/kWh)", title=title)
    return _save(fig, out)


def da_rt_spread(csv, out, title="") -> Path:
    df = pd.read_csv(csv, parse_dates=["datetime"])
    daily = df.set_index("datetime")[["da_price", "rt_price"]].resample("D").mean().dropna()
    fig, ax = plt.subplots()
    ax.plot(daily.index, daily["rt_price"], color=ACCENT, lw=1.4, label="实时 RT")
    ax.plot(daily.index, daily["da_price"], color="#888", lw=1.2, ls="--", label="日前 DA")
    ax.fill_between(daily.index, daily["da_price"], daily["rt_price"],
                    where=daily["rt_price"] >= daily["da_price"], color=ACCENT, alpha=0.15)
    ax.fill_between(daily.index, daily["da_price"], daily["rt_price"],
                    where=daily["rt_price"] < daily["da_price"], color=SECOND, alpha=0.15)
    ax.legend(frameon=False)
    ax.set(ylabel="价格 (元/kWh)", title=title)
    fig.autofmt_xdate()
    return _save(fig, out)


def province_compare(csv, out, value_col, label_col="province", title="") -> Path:
    df = pd.read_csv(csv).sort_values(value_col, ascending=False)
    fig, ax = plt.subplots()
    ax.bar(df[label_col], df[value_col], color=ACCENT, width=0.6)
    ax.set(title=title)
    fig.autofmt_xdate()
    return _save(fig, out)
