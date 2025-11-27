# Feature Engineering Module

It addresses the curse of dimensionality in high-dimensional sensor data by systematically removing static features (near-zero variance), 
incomplete measurements (high null values), and redundant features (high correlation >0.98), then ranks remaining features using mutual information or random forest importance scores. 
The pipeline is optimized for large-scale datasets with parallel processing (`n_jobs=-1`) and can handle datasets with hundreds of features, typically achieving 40-60% dimensionality 
reduction while preserving the most informative features for anomaly detection. The `FeatureSelectionIntegration` class provides seamless integration with existing training pipelines 
by filtering DataFrames to selected features and exposing importance scores for further analysis, with all results (filtered datasets, feature lists, JSON reports, and visualizations) saved for reproducibility and pipeline integration.

## Components

- **`feature_selection.py`** - Multi-stage feature selection pipeline with variance, null, and correlation-based filtering
- **`feature_integration.py`** - Integration utilities for applying selected features to training pipelines
- **`requirements.txt`** - Python dependencies (pandas, numpy, scikit-learn, matplotlib, seaborn)
- **`Dockerfile`** - Containerized execution environment

## Installation

### Local Setup

```bash
# Activate environment
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Docker Setup

```bash
docker build -t feature-engineering .
docker run -v $(pwd):/app/datasets -v $(pwd)/output:/app/feature_selection_results feature-engineering
```

## Feature Selection Pipeline

### Stage 1: Remove Static Features
Removes features with variance below threshold (default: 1e-6)
- Identifies features with constant or near-constant values
- Tracks unique value counts

### Stage 2: Remove High-Null Features  
Removes features exceeding null value threshold (default: 50%)
- Calculates null proportions per feature
- Filters out incomplete measurements

### Stage 3: Remove Correlated Features
Removes highly correlated feature pairs (default: >0.98 correlation)
- Computes full correlation matrix
- Keeps first feature from each correlated pair
- Generates correlation heatmap visualization

### Stage 4: Calculate Feature Importance
Ranks remaining features using mutual information or random forest
- `mutual_info` - Captures non-linear relationships
- `random_forest` - Identifies complex feature interactions
- Utilizes multiprocessing (`n_jobs=-1`) for parallel computation

### Stage 5: Select Top Features
Optional filtering to top N features by importance score

## Usage

### Basic Feature Selection

```python
from feature_selection import run_feature_selection

selector, df_selected = run_feature_selection(
    normal_data_path='datasets/normal_data.csv',
    attack_data_path='datasets/attack_data.csv',  # Optional
    output_dir='tmp/feature_results',
    sample_size=50000,
    variance_threshold=1e-6,
    null_threshold=0.5,
    correlation_threshold=0.98,
    calculate_importance=True,
    top_n_features=None  # Keep all filtered features
)
```

### Integration with Training Pipeline

```python
from feature_integration import FeatureSelectionIntegration

# Load selection results
integrator = FeatureSelectionIntegration('tmp/feature_results/feature_selection_report.json')

# Filter DataFrame to selected features
df_filtered = integrator.filter_dataframe(df, keep_labels=True)

# Get top features by importance
top_features = integrator.get_feature_importance(top_n=20)

# Print summary
integrator.print_summary()
```

### Command Line Execution

```bash
# Run with logging
source .venv/bin/activate
python feature_selection.py > logs/feature_selection_$(date +%Y%m%d_%H%M%S).log 2>&1
```

## Output Files

Generated in specified output directory:

- **`selected_features_dataset.csv`** - Filtered dataset with selected features
- **`selected_features.txt`** - List of selected feature names (one per line)
- **`feature_selection_report.json`** - Detailed selection report including:
  - Configuration parameters
  - Removed features by category
  - Selected features list
  - Feature importance scores
  - Summary statistics
- **`correlation_matrix.png`** - Heatmap of top 30 correlated features
- **`feature_importance.png`** - Bar chart of top 25 features by importance


## Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `variance_threshold` | 1e-6 | Minimum variance for non-static features |
| `null_threshold` | 0.5 | Maximum proportion of null values (0-1) |
| `correlation_threshold` | 0.98 | Correlation above which features are redundant |
| `sample_size` | None | Number of rows to sample (None = all rows) |
| `calculate_importance` | True | Whether to compute feature importance |
| `top_n_features` | None | Limit to top N features (None = keep all filtered) |



## Dependencies

- pandas 1.5.3 - Data manipulation
- numpy 1.24.3 - Numerical operations
- scikit-learn 1.2.2 - Feature selection algorithms
- matplotlib 3.7.1 - Visualization
- seaborn 0.12.2 - Statistical visualization
