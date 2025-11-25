import numpy as np
import pandas as pd


def rolling_features(df: pd.DataFrame, cols, window: int = 30) -> pd.DataFrame:
    """
    Create rolling window features for time-series data.

    For each sensor column c in `cols` we compute:
      - c__last   : last value in the window
      - c__mean   : rolling mean
      - c__std    : rolling std (ddof=0)
      - c__min    : rolling min
      - c__max    : rolling max
      - c__slope  : difference to previous sample (x_t - x_{t-1})
      - c__zlast  : z-score of last value: (last - mean) / std
    """

    # keep only the requested columns
    df = df[list(cols)].copy()

    # force numeric dtype for all columns (strings → float / NaN)
    for c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # rolling object
    roll = df.rolling(window=window, min_periods=window)

    # base rolling stats
    mean_base = roll.mean()
    std_base = roll.std(ddof=0)
    min_base = roll.min()
    max_base = roll.max()

    # basic features with suffixes
    last = df.add_suffix("__last")
    f_mean = mean_base.add_suffix("__mean")
    f_std = std_base.add_suffix("__std")
    f_min = min_base.add_suffix("__min")
    f_max = max_base.add_suffix("__max")

    # simple slope: difference to previous sample
    slope = (df - df.shift(1)).add_suffix("__slope")

    # z-score of the last value in the window
    safe_std = std_base.replace(0.0, np.nan)
    z_last = ((df - mean_base) / (safe_std + 1e-8)).add_suffix("__zlast")

    # concatenate all features
    feats = pd.concat(
        [last, f_mean, f_std, f_min, f_max, slope, z_last],
        axis=1,
    )

    # drop first window-1 rows (no full window)
    return feats.iloc[window - 1 :]


def proxy_anomaly_score(feats: pd.DataFrame) -> pd.Series:
    """Proxy anomaly score = sum of absolute z-scores of last values."""
    zcols = [c for c in feats.columns if c.endswith("__zlast")]
    score = feats[zcols].abs().sum(axis=1)
    score.name = "anomaly_score_proxy"
    return score
