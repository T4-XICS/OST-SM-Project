"""
Feature Selection Script for Anomaly Detection Model

This script performs feature selection on the SWaT dataset following best practices:
1. Remove features with static values (zero variance)
2. Remove features with more than 50% null values
3. Remove highly correlated features (>98% correlation)
4. Optional: Apply advanced feature selection techniques

Note: In production cybersecurity systems, feature selection should be used cautiously
as attackers may target different features over time. This script is for training purposes.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import mutual_info_classif
import matplotlib.pyplot as plt
import seaborn as sns
import json
import os
from typing import List, Dict, Tuple


class FeatureSelector:
    """
    Comprehensive feature selection for time-series anomaly detection
    """
    
    def __init__(self, variance_threshold: float = 1e-6, 
                 null_threshold: float = 0.5,
                 correlation_threshold: float = 0.98):
        """
        Args:
            variance_threshold: Threshold below which features are considered static
            null_threshold: Max proportion of null values allowed (0.5 = 50%)
            correlation_threshold: Correlation above which features are considered redundant
        """
        self.variance_threshold = variance_threshold
        self.null_threshold = null_threshold
        self.correlation_threshold = correlation_threshold
        
        # Track removed features at each step
        self.static_features = []
        self.high_null_features = []
        self.correlated_features = []
        self.selected_features = []
        self.feature_importance_scores = {}
        
    def load_data(self, filepath: str, sample_size: int = None) -> pd.DataFrame:
        """
        Load dataset with optional sampling for large files
        
        Args:
            filepath: Path to CSV file
            sample_size: Number of rows to sample (None = all rows)
        """
        print(f"Loading data from {filepath}...")
        
        if sample_size:
            # For large files, sample efficiently
            n_lines = sum(1 for line in open(filepath)) - 1  # Subtract header
            skip_idx = sorted(np.random.choice(range(1, n_lines + 1), 
                                              n_lines - sample_size, 
                                              replace=False))
            df = pd.read_csv(filepath, skiprows=skip_idx)
        else:
            df = pd.read_csv(filepath)
        
        print(f"Loaded {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    
    def remove_static_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 1: Remove features with zero or near-zero variance (static values)
        """
        print("\n" + "="*60)
        print("STEP 1: Removing Static Features")
        print("="*60)
        
        # Exclude non-numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        variances = df[numeric_cols].var()
        static_cols = variances[variances < self.variance_threshold].index.tolist()
        
        self.static_features = static_cols
        
        print(f"Found {len(static_cols)} static features:")
        for col in static_cols:
            unique_vals = df[col].nunique()
            print(f"  - {col}: variance={variances[col]:.2e}, unique_values={unique_vals}")
        
        # Remove static features
        df_cleaned = df.drop(columns=static_cols)
        print(f"\nRemaining features: {df_cleaned.shape[1]}")
        
        return df_cleaned
    
    def remove_high_null_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Step 2: Remove features with more than threshold% null values
        """
        print("\n" + "="*60)
        print("STEP 2: Removing High-Null Features")
        print("="*60)
        
        null_proportions = df.isnull().sum() / len(df)
        high_null_cols = null_proportions[null_proportions > self.null_threshold].index.tolist()
        
        self.high_null_features = high_null_cols
        
        print(f"Found {len(high_null_cols)} features with >{self.null_threshold*100}% null values:")
        for col in high_null_cols:
            print(f"  - {col}: {null_proportions[col]*100:.2f}% null")
        
        # Remove high-null features
        df_cleaned = df.drop(columns=high_null_cols)
        print(f"\nRemaining features: {df_cleaned.shape[1]}")
        
        return df_cleaned
    
    def remove_correlated_features(self, df: pd.DataFrame, 
                                   save_plot: bool = True,
                                   plot_path: str = 'correlation_matrix.png') -> pd.DataFrame:
        """
        Step 3: Remove highly correlated features (>threshold correlation)
        Keeps the first feature of each correlated pair
        """
        print("\n" + "="*60)
        print("STEP 3: Removing Highly Correlated Features")
        print("="*60)
        
        # Get numeric columns only (exclude Timestamp and Normal/Attack labels)
        exclude_cols = ['Timestamp', 'Normal_Attack', 'Normal/Attack']
        numeric_cols = [col for col in df.select_dtypes(include=[np.number]).columns 
                       if col not in exclude_cols]
        
        # Calculate correlation matrix
        print("Calculating correlation matrix...")
        corr_matrix = df[numeric_cols].corr().abs()
        
        # Find highly correlated pairs
        upper_triangle = corr_matrix.where(
            np.triu(np.ones(corr_matrix.shape), k=1).astype(bool)
        )
        
        # Find features with correlation > threshold
        to_drop = []
        correlated_pairs = []
        
        for column in upper_triangle.columns:
            correlated_features = upper_triangle.index[upper_triangle[column] > self.correlation_threshold].tolist()
            if correlated_features:
                for corr_feature in correlated_features:
                    corr_value = upper_triangle.loc[corr_feature, column]
                    correlated_pairs.append((corr_feature, column, corr_value))
                    if column not in to_drop:
                        to_drop.append(column)
        
        self.correlated_features = to_drop
        
        print(f"Found {len(correlated_pairs)} highly correlated pairs (>{self.correlation_threshold*100}%):")
        for feat1, feat2, corr in sorted(correlated_pairs, key=lambda x: x[2], reverse=True)[:10]:
            print(f"  - {feat1} <-> {feat2}: {corr:.4f}")
        
        if len(correlated_pairs) > 10:
            print(f"  ... and {len(correlated_pairs) - 10} more pairs")
        
        print(f"\nRemoving {len(to_drop)} features to eliminate redundancy")
        
        # Visualize correlation matrix (top correlated features)
        if save_plot and len(numeric_cols) > 0:
            self._plot_correlation_matrix(corr_matrix, plot_path, top_n=30)
        
        # Remove correlated features
        df_cleaned = df.drop(columns=to_drop)
        print(f"Remaining features: {df_cleaned.shape[1]}")
        
        return df_cleaned
    
    def _plot_correlation_matrix(self, corr_matrix: pd.DataFrame, 
                                 plot_path: str, top_n: int = 30):
        """
        Visualize correlation matrix for top N features
        """
        print(f"Saving correlation matrix plot to {plot_path}...")
        
        # Select top N features by average correlation
        avg_corr = corr_matrix.mean().sort_values(ascending=False)
        top_features = avg_corr.head(top_n).index.tolist()
        
        plt.figure(figsize=(14, 12))
        sns.heatmap(corr_matrix.loc[top_features, top_features], 
                   annot=False, cmap='coolwarm', center=0,
                   vmin=-1, vmax=1, square=True)
        plt.title(f'Correlation Matrix (Top {top_n} Features by Avg Correlation)')
        plt.tight_layout()
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        plt.close()
        print(f"Correlation matrix saved to {plot_path}")
    
    def calculate_feature_importance(self, df: pd.DataFrame, 
                                    label_col: str = 'Normal_Attack',
                                    method: str = 'mutual_info') -> Dict[str, float]:
        """
        Step 4 (Optional): Calculate feature importance scores
        
        Args:
            df: DataFrame with features and labels
            label_col: Name of the label column
            method: 'mutual_info' or 'random_forest'
        
        Returns:
            Dictionary of feature: importance score
        """
        print("\n" + "="*60)
        print("STEP 4: Calculating Feature Importance")
        print("="*60)
        
        if label_col not in df.columns:
            print(f"Warning: Label column '{label_col}' not found. Skipping feature importance.")
            return {}
        
        # Prepare data
        exclude_cols = ['Timestamp', label_col]
        feature_cols = [col for col in df.columns if col not in exclude_cols]
        
        X = df[feature_cols].fillna(0)
        
        # Convert labels to binary if string
        if df[label_col].dtype == 'object':
            y = (df[label_col] == 'Attack').astype(int)
        else:
            y = df[label_col]
        
        print(f"Calculating importance for {len(feature_cols)} features using {method}...")
        
        if method == 'mutual_info':
            # Mutual Information - works well for non-linear relationships
            importance_scores = mutual_info_classif(X, y, random_state=42)
            importance_dict = dict(zip(feature_cols, importance_scores))
            
        elif method == 'random_forest':
            # Random Forest - captures complex interactions
            rf = RandomForestClassifier(n_estimators=100, random_state=42, 
                                       max_depth=10, n_jobs=-1)
            rf.fit(X, y)
            importance_dict = dict(zip(feature_cols, rf.feature_importances_))
        
        else:
            raise ValueError(f"Unknown method: {method}")
        
        # Sort by importance
        self.feature_importance_scores = dict(
            sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
        )
        
        # Display top features
        print(f"\nTop 20 most important features ({method}):")
        for i, (feat, score) in enumerate(list(self.feature_importance_scores.items())[:20], 1):
            print(f"  {i:2d}. {feat:15s}: {score:.6f}")
        
        return self.feature_importance_scores
    
    def select_top_features(self, df: pd.DataFrame, 
                           n_features: int = None,
                           importance_threshold: float = None) -> pd.DataFrame:
        """
        Select top N features based on importance scores
        
        Args:
            df: DataFrame to filter
            n_features: Number of top features to keep (None = keep all)
            importance_threshold: Minimum importance score (None = no threshold)
        """
        if not self.feature_importance_scores:
            print("No feature importance scores available. Skipping selection.")
            return df
        
        print("\n" + "="*60)
        print("STEP 5: Selecting Top Features")
        print("="*60)
        
        selected_features = []
        
        if importance_threshold:
            selected_features = [
                feat for feat, score in self.feature_importance_scores.items()
                if score >= importance_threshold
            ]
            print(f"Selected {len(selected_features)} features with importance >= {importance_threshold}")
        
        if n_features:
            selected_features = list(self.feature_importance_scores.keys())[:n_features]
            print(f"Selected top {n_features} features by importance")
        
        if not selected_features:
            selected_features = list(self.feature_importance_scores.keys())
            print("No filtering applied - keeping all features")
        
        self.selected_features = selected_features
        
        # Keep selected features + label columns
        keep_cols = selected_features.copy()
        for col in ['Timestamp', 'Normal_Attack', 'Normal/Attack']:
            if col in df.columns and col not in keep_cols:
                keep_cols.append(col)
        
        df_selected = df[keep_cols]
        print(f"Final feature count: {len(selected_features)}")
        
        return df_selected
    
    def save_feature_selection_report(self, output_path: str = 'feature_selection_report.json'):
        """
        Save detailed report of feature selection process
        """
        report = {
            'parameters': {
                'variance_threshold': self.variance_threshold,
                'null_threshold': self.null_threshold,
                'correlation_threshold': self.correlation_threshold
            },
            'removed_features': {
                'static_features': self.static_features,
                'high_null_features': self.high_null_features,
                'correlated_features': self.correlated_features
            },
            'selected_features': self.selected_features,
            'feature_importance': self.feature_importance_scores,
            'summary': {
                'total_static_removed': len(self.static_features),
                'total_high_null_removed': len(self.high_null_features),
                'total_correlated_removed': len(self.correlated_features),
                'final_feature_count': len(self.selected_features) if self.selected_features else 'N/A'
            }
        }
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\nFeature selection report saved to {output_path}")
        return report
    
    def plot_feature_importance(self, output_path: str = 'feature_importance.png', 
                               top_n: int = 25):
        """
        Visualize top N features by importance
        """
        if not self.feature_importance_scores:
            print("No feature importance scores to plot")
            return
        
        print(f"Saving feature importance plot to {output_path}...")
        
        top_features = dict(list(self.feature_importance_scores.items())[:top_n])
        
        plt.figure(figsize=(10, 8))
        features = list(top_features.keys())
        scores = list(top_features.values())
        
        plt.barh(range(len(features)), scores)
        plt.yticks(range(len(features)), features)
        plt.xlabel('Importance Score')
        plt.title(f'Top {top_n} Features by Importance')
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        print(f"Feature importance plot saved to {output_path}")


def run_feature_selection(normal_data_path: str,
                         attack_data_path: str = None,
                         output_dir: str = 'feature_selection_results',
                         sample_size: int = 50000,
                         variance_threshold: float = 1e-6,
                         null_threshold: float = 0.5,
                         correlation_threshold: float = 0.98,
                         calculate_importance: bool = True,
                         top_n_features: int = None):
    """
    Main pipeline for feature selection
    
    Args:
        normal_data_path: Path to normal data CSV
        attack_data_path: Path to attack data CSV (optional, for combined analysis)
        output_dir: Directory to save results
        sample_size: Number of rows to sample for analysis
        variance_threshold: Threshold for static feature removal
        null_threshold: Threshold for null feature removal
        correlation_threshold: Threshold for correlation-based removal
        calculate_importance: Whether to calculate feature importance
        top_n_features: Number of top features to select (None = keep all after filtering)
    """
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize selector
    selector = FeatureSelector(
        variance_threshold=variance_threshold,
        null_threshold=null_threshold,
        correlation_threshold=correlation_threshold
    )
    
    # Load data
    print("="*60)
    print("LOADING DATA")
    print("="*60)
    df_normal = selector.load_data(normal_data_path, sample_size=sample_size)
    
    # Optionally combine with attack data for more comprehensive analysis
    if attack_data_path:
        df_attack = selector.load_data(attack_data_path, sample_size=sample_size // 2)
        df = pd.concat([df_normal, df_attack], ignore_index=True)
        print(f"Combined dataset: {df.shape[0]} rows, {df.shape[1]} columns")
    else:
        df = df_normal
    
    # Step 1: Remove static features
    df = selector.remove_static_features(df)
    
    # Step 2: Remove high-null features
    df = selector.remove_high_null_features(df)
    
    # Step 3: Remove correlated features
    df = selector.remove_correlated_features(
        df, 
        save_plot=True,
        plot_path=os.path.join(output_dir, 'correlation_matrix.png')
    )
    
    # Step 4 (Optional): Calculate feature importance
    if calculate_importance:
        selector.calculate_feature_importance(df, method='mutual_info')
        selector.plot_feature_importance(
            output_path=os.path.join(output_dir, 'feature_importance.png'),
            top_n=25
        )
    
    # Step 5 (Optional): Select top N features
    if top_n_features:
        df = selector.select_top_features(df, n_features=top_n_features)
    else:
        # Keep all features that passed the filters
        exclude_cols = ['Timestamp', 'Normal_Attack', 'Normal/Attack']
        selector.selected_features = [col for col in df.columns if col not in exclude_cols]
    
    # Save results
    print("\n" + "="*60)
    print("SAVING RESULTS")
    print("="*60)
    
    # Save cleaned dataset
    output_csv = os.path.join(output_dir, 'selected_features_dataset.csv')
    df.to_csv(output_csv, index=False)
    print(f"Cleaned dataset saved to {output_csv}")
    
    # Save feature list
    feature_list_path = os.path.join(output_dir, 'selected_features.txt')
    with open(feature_list_path, 'w') as f:
        f.write('\n'.join(selector.selected_features))
    print(f"Selected features list saved to {feature_list_path}")
    
    # Save detailed report
    report_path = os.path.join(output_dir, 'feature_selection_report.json')
    selector.save_feature_selection_report(report_path)
    
    # Print summary
    print("\n" + "="*60)
    print("FEATURE SELECTION SUMMARY")
    print("="*60)
    print(f"Original features: {len(df_normal.columns)}")
    print(f"Static features removed: {len(selector.static_features)}")
    print(f"High-null features removed: {len(selector.high_null_features)}")
    print(f"Correlated features removed: {len(selector.correlated_features)}")
    print(f"Final features selected: {len(selector.selected_features)}")
    print(f"Reduction: {(1 - len(selector.selected_features)/len(df_normal.columns))*100:.1f}%")
    
    return selector, df


if __name__ == "__main__":
    # Example usage
    NORMAL_DATA = '../datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv'
    ATTACK_DATA = '../datasets/swat/attack/SWaT_Dataset_Attack_v0_1.csv'
    
    selector, df_selected = run_feature_selection(
        normal_data_path=NORMAL_DATA,
        attack_data_path=ATTACK_DATA,
        output_dir='feature_selection_results',
        sample_size=50000,  # Sample 50k rows for faster processing
        variance_threshold=1e-6,  # Very low variance threshold
        null_threshold=0.5,  # 50% null values
        correlation_threshold=0.98,  # 98% correlation
        calculate_importance=True,  # Calculate feature importance
        top_n_features=None  # Keep all features after filtering (set to int to limit)
    )
    
    print("\n✓ Feature selection complete!")
    print(f"Selected features: {len(selector.selected_features)}")
    print(f"Results saved to: feature_selection_results/")
