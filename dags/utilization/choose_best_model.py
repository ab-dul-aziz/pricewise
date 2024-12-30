from utilization.fetch_from_postgresql import FetchFromPostgresql
from utilization.cleaning_data import CleaningData
from utilization.feature_engineering import read_and_filter_data, split_features_and_target, impute_with_knn
import pandas as pd
import pickle

import numpy as np
from sklearn.model_selection import KFold
from sklearn.metrics import mean_absolute_error, r2_score


# Fungsi untuk memuat model
def load_models():
    with open('best_model.pkl', 'rb') as file1:
        best_model = pickle.load(file1)
    with open('data/best_model_ever.pkl', 'rb') as file2:
        best_model_ever = pickle.load(file2)
    return best_model, best_model_ever


# Fungsi untuk evaluasi model
def evaluate_pretrained_model(model, X, y, cv=3):
    kf = KFold(n_splits=cv, shuffle=True, random_state=10)
    mae_scores = []
    r2_scores = []

    for fold, (_, test_index) in enumerate(kf.split(X), 1):
        X_test = X.iloc[test_index]
        y_test = y.iloc[test_index]
        
        # Prediksi menggunakan model yang sudah dilatih
        y_pred = model.predict(X_test)
        
        # Hitung metrik
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)
        mae_scores.append(mae)
        r2_scores.append(r2)
        
        # Debug: Tampilkan metrik untuk setiap fold
        print(f"Fold {fold} - MAE: {mae:.2f}, R²: {r2:.2f}")
    
    # Rata-rata dan standar deviasi MAE dan R²
    mean_mae = np.mean(mae_scores)
    std_mae = np.std(mae_scores)
    mean_r2 = np.mean(r2_scores)
    std_r2 = np.std(r2_scores)

    print(f"\nMAE - Mean: {mean_mae:.2f}, Std: {std_mae:.2f}")
    print(f"R² - Mean: {mean_r2:.2f}, Std: {std_r2:.2f}")

    return mean_mae, std_mae, mean_r2, std_r2


# Fungsi utama untuk memilih model terbaik
def ChooseBestModel():
    # Define numerical and categorical columns
    num_cols = ['land_size_m2', 'building_size_m2', 'road_width', 'maid_bedroom',
                'maid_bathroom', 'kitchen', 'floor_level', 'bedroom', 'bathroom',
                'garage', 'carport', 'voltage_watt']

    cat_cols = ['city', 'property_type', 'certificate', 'furniture', 'house_facing',
                'water_source', 'property_condition']
    
    FetchFromPostgresql()
    CleaningData()
    df = read_and_filter_data('/opt/airflow/data/data_cleaned.csv')
    X_train, X_test, y_train, y_test = split_features_and_target(df)
    X_train_imputed, X_test_imputed = impute_with_knn(X_train, X_test, num_cols, cat_cols)

    # Load models
    best_model, best_model_ever = load_models()

    # Evaluasi untuk model pertama
    print("Evaluating Best Model:")
    mae_best_model, std_mae_best_model, r2_best_model, std_r2_best_model = evaluate_pretrained_model(best_model, X_test_imputed, y_test, cv=3)

    # Evaluasi untuk model kedua
    print("\nEvaluating Best Model Ever:")
    mae_best_model_ever, std_mae_best_model_ever, r2_best_model_ever, std_r2_best_model_ever = evaluate_pretrained_model(best_model_ever, X_test_imputed, y_test, cv=3)

    # Print hasil evaluasi rata-rata dan standar deviasi
    print("\nFinal Results:")
    print(f"Best Model - MAE: {mae_best_model:.2f} (±{std_mae_best_model:.2f}), R²: {r2_best_model:.2f} (±{std_r2_best_model:.2f})")
    print(f"Best Model Ever - MAE: {mae_best_model_ever:.2f} (±{std_mae_best_model_ever:.2f}), R²: {r2_best_model_ever:.2f} (±{std_r2_best_model_ever:.2f})")

    # Logika untuk menyimpan model terbaik
    if (mae_best_model < mae_best_model_ever) and (std_mae_best_model <= std_mae_best_model_ever):
        print("\nBest Model has better performance. Saving it as 'best_model_ever.pkl'.")
        best_model_ever = best_model
        with open("/opt/airflow/data/best_model_ever.pkl", "wb") as file:
            pickle.dump(best_model_ever, file)
    elif (mae_best_model_ever < mae_best_model) and (std_mae_best_model_ever <= std_mae_best_model):
        print("\nBest Model Ever retains its position as the best model.")
    else:
        # Jika terjadi konflik, pilih berdasarkan MAE saja
        if mae_best_model < mae_best_model_ever:
            print("\nConflict resolved: Best Model has better MAE. Saving it as 'best_model_ever.pkl'.")
            best_model_ever = best_model
            with open("/opt/airflow/data/best_model_ever.pkl", "wb") as file:
                pickle.dump(best_model_ever, file)
        else:
            print("\nConflict resolved: Best Model Ever remains as the best model.")


if __name__ == "__main__":
    ChooseBestModel()
