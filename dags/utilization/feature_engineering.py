import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, MinMaxScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.impute import KNNImputer
from sklearn.pipeline import make_pipeline
import pickle
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.ensemble import RandomForestRegressor

# ===================== Helper Functions =====================

def read_and_filter_data(filepath):
    """Read CSV and select relevant columns."""
    df = pd.read_csv(filepath)
    selected_columns = [
        'land_size_m2', 'building_size_m2', 'road_width', 'city', 'property_type',
        'certificate', 'furniture', 'house_facing', 'water_source', 'property_condition',
        'bedroom', 'bathroom', 'garage', 'carport', 'voltage_watt', 'maid_bedroom',
        'maid_bathroom', 'kitchen', 'floor_level', 'price_mio'
    ]
    return df[selected_columns]

def split_features_and_target(df, target_column='price_mio',random_state=999):
    """Split data into features (X) and target (y)."""
    X = df.drop(target_column, axis=1)
    y = df[target_column]

    # Split dataset into training+validation and test subsets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.10, random_state=random_state)

    return X_train, X_test, y_train, y_test

def impute_with_knn(X_train, X_test, num_cols, cat_cols):
    # Create deep copies to avoid modifying original dataframes
    X_train_imputed = X_train.copy(deep=True)
    X_test_imputed = X_test.copy(deep=True)
    
    # Prepare data with numeric columns and encoded categorical columns
    def prepare_data_for_imputation(df, label_encoders):
        # Create a copy of the dataframe
        prepared_df = df.copy(deep=True)
        
        # Handle numeric columns
        for col in num_cols:
            # Replace NaNs with median for numeric columns
            prepared_df[col] = prepared_df[col].fillna(prepared_df[col].median())
        
        # Handle categorical columns
        for col in cat_cols:
            # If encoder exists, use it, otherwise create a new one
            if col not in label_encoders:
                # Combine unique values from both train and test
                combined_categories = pd.concat([X_train[col], X_test[col]]).dropna().unique()
                
                # Create LabelEncoder with combined categories
                le = LabelEncoder()
                le.fit(combined_categories.astype(str))
                label_encoders[col] = le
            
            # Get the encoder for this column
            le = label_encoders[col]
            
            # Create a copy of the column for transformation
            col_to_encode = prepared_df[col].copy()
            
            # Replace NaNs with a special category that is in the original categories
            col_to_encode = col_to_encode.fillna(le.classes_[0])
            
            # Transform categorical columns
            prepared_df[col] = le.transform(col_to_encode.astype(str))
        
        return prepared_df, label_encoders
    
    # Dictionary to store label encoders
    label_encoders = {}
    
    # Prepare train and test data
    X_train_prep, label_encoders = prepare_data_for_imputation(X_train, label_encoders)
    X_test_prep, label_encoders = prepare_data_for_imputation(X_test, label_encoders)
    
    # Combine all columns to impute
    cols_to_impute = num_cols + cat_cols
    
    # Prepare data for KNN Imputer
    X_train_to_impute = X_train_prep[cols_to_impute]
    X_test_to_impute = X_test_prep[cols_to_impute]
    
    # Initialize KNN Imputer
    knn_imputer = KNNImputer(n_neighbors=5)
    
    # Fit and transform training data
    X_train_imputed_values = knn_imputer.fit_transform(X_train_to_impute)

    # Transform test data
    X_test_imputed_values = knn_imputer.transform(X_test_to_impute)
    
    # Update the original dataframes with imputed values
    X_train_imputed[cols_to_impute] = X_train_imputed_values
    X_test_imputed[cols_to_impute] = X_test_imputed_values
    
    # Decode categorical columns
    for col in cat_cols:
        # Inverse transform categorical columns
        X_train_imputed[col] = label_encoders[col].inverse_transform(
            X_train_imputed[col].astype(int)
        )
        X_test_imputed[col] = label_encoders[col].inverse_transform(
            X_test_imputed[col].astype(int)
        )
    
    return X_train_imputed, X_test_imputed

# ===================== Main Feature Engineering =====================

def FeatureEngineering():
    """Main function to perform feature engineering."""
    # Filepath to cleaned data
    filepath = '/opt/airflow/data/data_cleaned.csv'

    # Define numerical and categorical columns
    num_cols = ['land_size_m2', 'building_size_m2', 'road_width', 'maid_bedroom',
                'maid_bathroom', 'kitchen', 'floor_level', 'bedroom', 'bathroom',
                'garage', 'carport', 'voltage_watt']

    cat_cols = ['city', 'property_type', 'certificate', 'furniture', 'house_facing',
                'water_source', 'property_condition']

    # Step 1: Read and filter data
    df = read_and_filter_data(filepath)

    # Step 2: Split features and target
    X_train, X_test, y_train, y_test = split_features_and_target(df)

    # Step 3: Handle missing values
    X_train_imputed, X_test_imputed = impute_with_knn(X_train, X_test, num_cols, cat_cols)

    # Step 4: Making column transformer for preprocessing
    transformer = ColumnTransformer([
    ('scaler', MinMaxScaler(), num_cols),
    ('encoder_ohe', OneHotEncoder(), cat_cols)
    ])
    transformer

    # Step 5: Making pipeline for Random Forest
    pipe_rf = Pipeline([
    ('transformer', transformer),
    ('model', RandomForestRegressor(random_state=999))
    ])

    # Step 5: Save preprocessed data
    data_to_save = {
        "X_train_imputed": X_train_imputed,
        "X_test_imputed": X_test_imputed,
        "y_train": y_train,
        "y_test": y_test,
        "pipe_rf": pipe_rf
    }
    with open("/opt/airflow/data/data_after_fe.pkl", "wb") as f:
        pickle.dump(data_to_save, f)

    print('Feature Engineering completed successfully!')

# ===================== Run as Script =====================

if __name__ == "__main__":
    # Call FeatureEngineering
    FeatureEngineering()
