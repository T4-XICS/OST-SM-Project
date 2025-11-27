"""
Integration Helper: Use Selected Features in Model Training

This module provides helper functions to integrate feature selection
results into your existing preprocessing and training pipeline.
"""

import json
import pandas as pd
from typing import List, Dict, Optional


class FeatureSelectionIntegration:
    """
    Helper class to integrate feature selection results into existing pipeline
    """
    
    def __init__(self, report_path: str = 'feature_selection_results/feature_selection_report.json'):
        """
        Initialize with feature selection report
        
        Args:
            report_path: Path to feature_selection_report.json
        """
        self.report_path = report_path
        self.selected_features = []
        self.removed_features = {}
        self.feature_importance = {}
        
        self.load_report()
    
    def load_report(self):
        """Load feature selection report"""
        try:
            with open(self.report_path, 'r') as f:
                report = json.load(f)
            
            self.selected_features = report.get('selected_features', [])
            self.removed_features = report.get('removed_features', {})
            self.feature_importance = report.get('feature_importance', {})
            
            print(f"Loaded feature selection report: {len(self.selected_features)} features selected")
            
        except FileNotFoundError:
            print(f"Warning: Feature selection report not found at {self.report_path}")
            print("Run feature selection first: python run_feature_selection.py")
            self.selected_features = []
    
    def get_selected_features(self) -> List[str]:
        """Get list of selected feature names"""
        return self.selected_features.copy()
    
    def get_feature_importance(self, top_n: int = None) -> Dict[str, float]:
        """
        Get feature importance scores
        
        Args:
            top_n: Return only top N features (None = all)
        """
        if top_n:
            items = list(self.feature_importance.items())[:top_n]
            return dict(items)
        return self.feature_importance.copy()
    
    def filter_dataframe(self, df: pd.DataFrame, keep_labels: bool = True) -> pd.DataFrame:
        """
        Filter DataFrame to keep only selected features
        
        Args:
            df: Input DataFrame
            keep_labels: Whether to keep label columns (Timestamp, Normal_Attack)
        
        Returns:
            Filtered DataFrame
        """
        if not self.selected_features:
            print("Warning: No selected features loaded. Returning original DataFrame.")
            return df
        
        # Columns to keep
        keep_cols = self.selected_features.copy()
        
        # Optionally keep label/timestamp columns
        if keep_labels:
            for col in ['Timestamp', 'Normal_Attack', 'Normal/Attack']:
                if col in df.columns and col not in keep_cols:
                    keep_cols.append(col)
        
        # Filter
        missing_cols = [col for col in keep_cols if col not in df.columns]
        if missing_cols:
            print(f"Warning: {len(missing_cols)} selected features not found in DataFrame:")
            print(f"  {missing_cols[:5]}{'...' if len(missing_cols) > 5 else ''}")
            keep_cols = [col for col in keep_cols if col in df.columns]
        
        df_filtered = df[keep_cols]
        print(f"Filtered DataFrame: {df.shape[1]} -> {df_filtered.shape[1]} columns")
        
        return df_filtered
    
    def get_removal_summary(self) -> Dict:
        """Get summary of removed features by category"""
        summary = {
            'static_features': len(self.removed_features.get('static_features', [])),
            'high_null_features': len(self.removed_features.get('high_null_features', [])),
            'correlated_features': len(self.removed_features.get('correlated_features', [])),
            'total_removed': sum([
                len(self.removed_features.get('static_features', [])),
                len(self.removed_features.get('high_null_features', [])),
                len(self.removed_features.get('correlated_features', []))
            ])
        }
        return summary
    
    def print_summary(self):
        """Print feature selection summary"""
        print("\n" + "="*60)
        print("FEATURE SELECTION SUMMARY")
        print("="*60)
        
        removal_summary = self.get_removal_summary()
        print(f"Selected features: {len(self.selected_features)}")
        print(f"\nRemoved features:")
        print(f"  - Static (zero variance): {removal_summary['static_features']}")
        print(f"  - High null values: {removal_summary['high_null_features']}")
        print(f"  - Highly correlated: {removal_summary['correlated_features']}")
        print(f"  - Total removed: {removal_summary['total_removed']}")
        
        if self.feature_importance:
            print(f"\nTop 10 most important features:")
            for i, (feat, score) in enumerate(list(self.feature_importance.items())[:10], 1):
                print(f"  {i:2d}. {feat:20s}: {score:.6f}")


def update_preprocess_data_with_features(selected_features_path: str = 'feature_selection_results/selected_features.txt'):
    """
    Generate updated preprocessing code that uses selected features
    
    Args:
        selected_features_path: Path to selected_features.txt file
    
    Returns:
        String containing updated preprocessing function
    """
    
    # Load selected features
    try:
        with open(selected_features_path, 'r') as f:
            selected_features = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: Selected features file not found: {selected_features_path}")
        return None
        
# Generate updated preprocessing function
def preprocess_spark_with_feature_selection(df, selected_features: List[str]):
    """
    Updated preprocessing function with feature selection
    
    This function applies the selected features from feature selection analysis.
    Selected features: {len(selected_features)}
    """
    from pyspark.sql import functions as F
    
    # Define selected features (from feature selection)
    SELECTED_FEATURES = {selected_features}
    
    # Drop timestamp column if present
    first_col = df.columns[0]
    if first_col == 'Timestamp' or dict(df.dtypes)[first_col] not in ['double', 'float', 'int', 'bigint']:
        df = df.drop(first_col)
    
    # Drop label column if present (will be handled separately)
    last_col = df.columns[-1]
    if last_col in ['Normal_Attack', 'Normal/Attack'] or dict(df.dtypes)[last_col] not in ['double', 'float', 'int', 'bigint']:
        df = df.drop(last_col)
    
    # Keep only selected features
    available_features = [f for f in SELECTED_FEATURES if f in df.columns]
    if len(available_features) < len(SELECTED_FEATURES):
        missing = set(SELECTED_FEATURES) - set(available_features)
        print(f"Warning: {{len(missing)}} selected features not found in data: {{list(missing)[:5]}}")
    
    df = df.select(available_features)
    
    # Convert all columns to numeric
    for c in df.columns:
        df = df.withColumn(c, F.col(c).cast("double"))
    
    # Fill missing values
    df = df.fillna(0)
    
    print(f"Preprocessed data: {{len(df.columns)}} features selected")
    
    return df


# Example usage functions
def example_basic_usage():
    """Example: Basic feature filtering"""
    print("Example 1: Basic Feature Filtering")
    print("-" * 60)
    
    # Load integration helper
    integrator = FeatureSelectionIntegration()
    
    # Get selected features
    selected_features = integrator.get_selected_features()
    print(f"Selected {len(selected_features)} features")
    
    # Load your data
    df = pd.read_csv('../datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv', nrows=1000)
    
    # Filter to selected features
    df_filtered = integrator.filter_dataframe(df, keep_labels=True)
    
    print(f"Original: {df.shape}, Filtered: {df_filtered.shape}")


def example_with_importance():
    """Example: Use feature importance for further selection"""
    print("\nExample 2: Using Feature Importance")
    print("-" * 60)
    
    integrator = FeatureSelectionIntegration()
    
    # Get top 20 features by importance
    top_features = integrator.get_feature_importance(top_n=20)
    
    print(f"Top 20 features:")
    for feat, score in top_features.items():
        print(f"  {feat}: {score:.6f}")


def example_generate_preprocessing_code():
    """Example: Generate updated preprocessing code"""
    print("\nExample 3: Generate Updated Preprocessing Code")
    print("-" * 60)
    
    code = update_preprocess_data_with_features()
    
    if code:
        print("Generated preprocessing function:")
        print(code[:500] + "...")
        
        # Save to file
        with open('preprocess_data_updated.py', 'w') as f:
            f.write(code)
        print("\nSaved to: preprocess_data_updated.py")