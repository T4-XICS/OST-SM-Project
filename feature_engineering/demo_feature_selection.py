#!/usr/bin/env python3
"""
Demo: Feature Selection on SWaT Dataset

This demo runs feature selection on a small sample of the SWaT dataset
to demonstrate the complete workflow quickly.

This is perfect for:
- Understanding how feature selection works
- Testing before running on full dataset
- Quick experimentation with parameters
"""

import os
import sys

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from feature_selection import run_feature_selection


def run_demo():
    """
    Run a quick demo of feature selection
    """
    print("=" * 70)
    print(" " * 15 + "FEATURE SELECTION DEMO")
    print(" " * 10 + "Quick Analysis on SWaT Dataset Sample")
    print("=" * 70)
    print()
    
    print("This demo will:")
    print("  1. Load a small sample (10,000 rows) from SWaT dataset")
    print("  2. Remove static features (zero variance)")
    print("  3. Remove features with >50% null values")
    print("  4. Remove highly correlated features (>98% correlation)")
    print("  5. Calculate feature importance using Mutual Information")
    print("  6. Generate visualizations and reports")
    print()
    print("Estimated time: 1-2 minutes")
    print()
    
    # Define paths
    NORMAL_DATA = '../datasets/swat/normal/SWaT_Dataset_Normal_v0_1.csv'
    ATTACK_DATA = '../datasets/swat/attack/SWaT_Dataset_Attack_v0_1.csv'
    OUTPUT_DIR = 'demo_feature_selection_results'
    
    # Check if files exist
    if not os.path.exists(NORMAL_DATA):
        print(f"❌ Error: Normal data file not found at {NORMAL_DATA}")
        print(f"Current directory: {os.getcwd()}")
        print("\nPlease ensure you're running from the 'model' directory")
        return False
    
    input("Press ENTER to start the demo...")
    print()
    
    try:
        # Run feature selection with demo parameters
        selector, df_selected = run_feature_selection(
            normal_data_path=NORMAL_DATA,
            attack_data_path=ATTACK_DATA if os.path.exists(ATTACK_DATA) else None,
            output_dir=OUTPUT_DIR,
            sample_size=10000,  # Small sample for quick demo
            variance_threshold=1e-6,
            null_threshold=0.5,
            correlation_threshold=0.98,
            calculate_importance=True,
            top_n_features=None  # Keep all features after filtering
        )
        
        # Show results
        print("\n" + "=" * 70)
        print(" " * 20 + "DEMO RESULTS")
        print("=" * 70)
        print()
        
        print("📊 Feature Selection Statistics:")
        print(f"   • Static features removed: {len(selector.static_features)}")
        print(f"   • High-null features removed: {len(selector.high_null_features)}")
        print(f"   • Correlated features removed: {len(selector.correlated_features)}")
        print(f"   • Final features selected: {len(selector.selected_features)}")
        print()
        
        if selector.feature_importance_scores:
            print("🏆 Top 10 Most Important Features:")
            for i, (feat, score) in enumerate(list(selector.feature_importance_scores.items())[:10], 1):
                print(f"   {i:2d}. {feat:25s} (score: {score:.6f})")
            print()
        
        print("📁 Generated Files:")
        print(f"   Location: {os.path.abspath(OUTPUT_DIR)}/")
        print()
        for filename in os.listdir(OUTPUT_DIR):
            filepath = os.path.join(OUTPUT_DIR, filename)
            size = os.path.getsize(filepath)
            size_str = f"{size/1024:.1f} KB" if size > 1024 else f"{size} bytes"
            print(f"   ✓ {filename:40s} ({size_str})")
        print()
        
        print("=" * 70)
        print(" " * 22 + "DEMO COMPLETE! ✓")
        print("=" * 70)
        print()
        
        print("Next Steps:")
        print("  1. Review the visualizations:")
        print(f"     - Open {OUTPUT_DIR}/correlation_matrix.png")
        print(f"     - Open {OUTPUT_DIR}/feature_importance.png")
        print()
        print("  2. Examine the detailed report:")
        print(f"     - Open {OUTPUT_DIR}/feature_selection_report.json")
        print()
        print("  3. See the selected features:")
        print(f"     - Open {OUTPUT_DIR}/selected_features.txt")
        print()
        print("  4. Run on larger sample for production:")
        print("     python run_feature_selection.py --sample-size 100000")
        print()
        
        return True
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: File not found - {e}")
        return False
    except Exception as e:
        print(f"\n❌ Error during demo: {e}")
        import traceback
        traceback.print_exc()
        return False


def show_example_usage():
    """
    Show example code for using the results
    """
    print("=" * 70)
    print(" " * 18 + "EXAMPLE INTEGRATION CODE")
    print("=" * 70)
    print()
    
    example_code = '''
# Example 1: Load selected features in your preprocessing pipeline
# -----------------------------------------------------------------
import json

# Load the feature selection report
with open('demo_feature_selection_results/feature_selection_report.json') as f:
    report = json.load(f)

selected_features = report['selected_features']
print(f"Using {len(selected_features)} selected features")

# Apply to your DataFrame
df_clean = df[selected_features + ['Timestamp', 'Normal_Attack']]


# Example 2: Use the integration helper
# --------------------------------------
from feature_integration import FeatureSelectionIntegration

integrator = FeatureSelectionIntegration(
    report_path='demo_feature_selection_results/feature_selection_report.json'
)

# Filter any DataFrame
df_filtered = integrator.filter_dataframe(df)

# Get feature importance
top_features = integrator.get_feature_importance(top_n=20)


# Example 3: Update your Spark preprocessing
# -------------------------------------------
from pyspark.sql import DataFrame

def preprocess_with_selected_features(df: DataFrame):
    # Load selected features
    with open('demo_feature_selection_results/selected_features.txt') as f:
        selected_features = [line.strip() for line in f]
    
    # Select only these features
    df = df.select(selected_features)
    
    # Continue with your normal preprocessing...
    return df
'''
    
    print(example_code)
    print()


if __name__ == "__main__":
    print()
    success = run_demo()
    
    if success:
        print()
        show_input = input("Would you like to see example integration code? (y/n): ")
        if show_input.lower() in ['y', 'yes']:
            show_example_usage()
    
    print()
    print("Thank you for using the Feature Selection Demo!")
    print()
    
    sys.exit(0 if success else 1)
