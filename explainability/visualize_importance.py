# -*- coding: utf-8 -*-
# Visualization module for feature importance
import matplotlib.pyplot as plt

def plot_feature_importance(features, importances, title="Feature Importance"):
    plt.figure(figsize=(8, 4))
    plt.barh(features, importances, color="skyblue")
    plt.xlabel("Importance")
    plt.ylabel("Features")
    plt.title(title)
    plt.tight_layout()
    plt.show()
