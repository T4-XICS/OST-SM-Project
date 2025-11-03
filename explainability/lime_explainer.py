"""LIME-based local explanations."""
def explain_lime(model, x, feature_names):
    """Return top-k influential features for a single sample.
    TODO: wire up lime.lime_tabular with model.predict / score.
    """
    return {"top_features": [], "weights": []}
