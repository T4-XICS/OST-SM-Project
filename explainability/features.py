import pandas as pd

def rolling_features(df: pd.DataFrame, cols, window: int = 30) -> pd.DataFrame:
    """
    Create rolling window features for time-series data.
    Features: last, mean, std, min, max, slope, z-score of last value.
    """
    df = df[cols].copy()
    roll = df.rolling(window=window, min_periods=window)

    f_mean = roll.mean().add_suffix("__mean")
    f_std = roll.std(ddof=0).add_suffix("__std")
    f_min = roll.min().add_suffix("__min")
    f_max = roll.max().add_suffix("__max")
    last = df.add_suffix("__last")
    slope = (df - df.shift(1)).add_suffix("__slope")

    # z-score of the last value relative to rolling stats
    z_last = (
        (df - f_mean.filter(like="__mean").rename(columns=lambda c: c.replace("__mean", ""))) /
        (f_std.filter(like="__std").rename(columns=lambda c: c.replace("__std", "")) + 1e-8)
    ).add_suffix("__zlast")

    feats = pd.concat([last, f_mean, f_std, f_min, f_max, slope, z_last], axis=1)
    return feats.iloc[window - 1:]

def proxy_anomaly_score(feats: pd.DataFrame) -> pd.Series:
    """
    Proxy anomaly score = sum of absolute z-scores of last values.
    """
    zcols = [c for c in feats.columns if c.endswith("__zlast")]
    score = feats[zcols].abs().sum(axis=1)
    score.name = "anomaly_score_proxy"
    return score
