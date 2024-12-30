from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.model_selection import RandomizedSearchCV
import pickle

def Modeling():
    """Main function to train and evaluate the model."""
    # Load Data
    with open("/opt/airflow/data/data_after_fe.pkl", "rb") as f:
        loaded_data = pickle.load(f)

    X_train_imputed = loaded_data["X_train_imputed"]
    X_test_imputed = loaded_data["X_test_imputed"]
    y_train = loaded_data["y_train"]
    y_test = loaded_data["y_test"]
    pipe_rf = loaded_data['pipe_rf']

    # Parameter Grid
    param_distributions = {
        'model__n_estimators': [5, 10, 50, 100, 300, 500],
        'model__max_depth': [10, 20, 30, None],
        'model__min_samples_split': [2, 5, 10],
        'model__min_samples_leaf': [1, 2, 4]
    }

    # RandomizedSearchCV
    random_search = RandomizedSearchCV(
        estimator=pipe_rf,
        param_distributions=param_distributions,
        n_iter=50,  # Reduce iterations for faster experimentation; adjust for final runs
        cv=5,
        scoring='neg_mean_absolute_error',
        n_jobs=-1,
        random_state=0  # Ensures reproducibility
    )

    # Fit RandomizedSearchCV
    random_search.fit(X_train_imputed, y_train)

    # Display Best Results
    print("Best Parameters:", random_search.best_params_)
    print("Best Score (Negative MAE):", random_search.best_score_)

    # Use the Best Model
    best_model = random_search.best_estimator_

    # Predict train-set & test-set
    y_train_predict = best_model.predict(X_train_imputed)
    y_test_predict = best_model.predict(X_test_imputed)

    # RMSE (Root Mean Squared Error) Train and Test:
    mae_train = mean_absolute_error(y_train, y_train_predict)
    mae_test = mean_absolute_error(y_test, y_test_predict)
    r2_train = r2_score(y_train, y_train_predict)
    r2_test = r2_score(y_test, y_test_predict)

    print(f"Mean Absolute Error (MAE) - TRAIN: {mae_train:.2f}")
    print(f"Mean Absolute Error (MAE) - TEST: {mae_test:.2f}")
    print(f"R-squared (R2 Score): {r2_train:.2f}")
    print(f"R-squared (R2 Score): {r2_test:.2f}")

    # Menyimpan model ke file
    with open('/opt/airflow/data/best_model.pkl', 'wb') as file:
        pickle.dump(best_model, file)

    print("Model berhasil disimpan!")

if __name__ == "__main__":
    Modeling()
