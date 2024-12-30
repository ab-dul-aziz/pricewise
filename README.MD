<h1 align="center">PRiceWise: Property Price Predictor</h1>

PRiceWise is a robust model designed to predict property prices in Jabodetabek. It utilizes real-world data obtained through web scraping, meticulously processed to ensure optimal performance. The model is fully automated, performing monthly updates that encompass automated data scraping and retraining with new data. This ensures PRiceWise remains relevant and consistently aligned with evolving market trends.

## **Table of Contents**
1. [Introduction](#introduction)
2. [Data Overview](#data-overview)
3. [Methodology](#methodology)
5. [Machine Learning Models Employed](#machine-learning-models-employed)
6. [Model Analysis](#model-analysis)
7. [Further Improvement](#further-improvement)
8. [How to Use](#how-to-use)
9. [Team Members](#team-members)
10. [About Me](#about-me)


## **Introduction**
Determining the right selling price for a property is often a challenge for sellers, involving significant time and costs spent on market research to find an appropriate price. With the availability of a property price prediction model, sellers can set prices more quickly, accurately, and strategically, based on comprehensive data analysis, ultimately maximizing their profits and reduce human error. Moreover, this model also helps sellers understand current market trends and the factors influencing property prices, enabling them to make more informed decisions in the selling and marketing process.


## **Data Overview**
This dataset obtained from scraping website [rumah123](https://www.rumah123.com/). Here's a summary of the columns after scraping.

| **Column Name**           | **Data Type**     | **Description**                                                                  |
|---------------------------|-------------------|----------------------------------------------------------------------------------|
| url                       | String            | URL of the property listing.                                                     |
| title                     | String            | Title of the property listing.                                                   |
| description               | String            | Description of the property features and details.                                |
| price                     | Float             | The price of the property in IDR.                                                |
| address                   | String            | The address of the property.                                                     |
| city                      | String            | The city where the property is located.                                          |
| land_size_m2              | Float             | The size of the land in square meters.                                           |
| building_size_m2          | Float             | The size of the building in square meters.                                       |
| bedroom                   | Integer           | The number of bedrooms in the property.                                          |
| bathroom                  | Integer           | The number of bathrooms in the property.                                         |
| garage                    | Integer           | The number of garage spaces available.                                           |
| carport                   | Integer           | The number of carport spaces available.                                          |
| property_type             | String            | The type of property (e.g., House, Land, Apartment).                             |
| certificate               | String            | The certificate type (e.g., SHM - Sertifikat Hak Milik).                         |
| voltage_watt              | Integer           | The electric power supply in watts (e.g., 2200 watts).                           |
| maid_bedroom              | Integer           | The number of maid's bedrooms in the property.                                   |
| maid_bathroom             | Integer           | The number of maid's bathrooms in the property.                                  |
| kitchen                   | Integer           | The number of kitchen in the property.                                           |
| dining_room               | Integer           | The number of dining room in the property.                                       |
| living_room               | Integer           | The number of living room in the property.                                       |
| furniture                 | String            | The furnishing status of the property (e.g., Furnished, Semi Furnished, Unfurnished). |
| building_material         | String            | The materials used for the building construction.                               |
| floor_material            | String            | The materials used for the flooring of the property.                            |
| floor_level               | Integer           | The floor level (for apartments or multi-story buildings).                       |
| house_facing              | String            | The direction the house is facing (e.g., North, South, East, West).             |
| concept_and_style         | String            | The architectural concept and style of the property (e.g., Modern, Minimalist). |
| view                      | String            | The type of view available from the property (e.g., Garden View, Sea View).     |
| internet_access           | String            | Indicates whether internet access is available (e.g., Yes, No).                |
| road_width                | Float             | The width of the road in meters.                                                 |
| year_built                | Integer           | The year the property was built.                                                |
| year_renovated            | Integer           | The year the property was renovated (if applicable).                            |
| water_source              | String            | The source of water for the property (e.g., PAM, Well).                         |
| corner_property           | Boolean           | Indicates whether the property is a corner lot (True/False).                    |
| property_condition        | String            | The condition of the property (e.g., New, Renovated, Needs Renovation).         |
| ad_type                   | String            | The type of advertisement (e.g., Sale, Rent).                                   |
| ad_id                     | String            | A unique identifier for the advertisement.                                      |


## **Methodology**
![flow](flow.png)

The methodology consists of the following steps:

1. **Scraping Link All Property in Jabodetabek from Website**  
   The first step involves scraping a list of all available property links in the Jabodetabek area from website. This will provide the necessary URLs to access individual property details.

2. **Scraping Property Data from Website**  
   Using the list of property URLs, data is scraped from each individual property listing page. The data gathered includes essential information such as price, location, features, and other relevant property details.

3. **Extract Needed Data from Scraping Result**  
   After scraping the raw data, the next step is to extract the relevant information. Only the data necessary for the analysis, such as price, property type, and other attributes, will be retained while removing any unnecessary or irrelevant information.

4. **Update Data to Database in Postgres**  
   The extracted data will be uploaded to a PostgreSQL database. This allows for efficient storage, retrieval, and querying of the data for further analysis.

5. **Fetch Data from PostgreSQL**  
   Once the data is stored in the PostgreSQL database, it will be fetched for further processing and analysis.

6. **Cleaning Data**  
   This involves handling cardinality, removing duplicates, remove trailing and leading whitespace, and ensuring consistency across different data entries.

7. **Feature Engineering**  
   In this step, the dataset will be prepared for analysis and modeling. This includes handling missing value, feature selection, encoding, and scaling.

8. **Modeling**  
    Various machine learning models will be tested to predict property prices based on certain attributes. However, in the pipeline, the focus will be on testing the Random Forest model due to its robustness, as well as demonstrated the best performance during initial testing in `modeling.ipynb`. Then we will find and save the best parameter.

9. **Choose Best Model**  
   The process involves comparing the performance (MAE and R²) of the current and previous best models using a test dataset. The evaluation considers the mean and standard deviation of these metrics across folds. The model with superior performance is saved as the new "best_model_ever."

10. **Deployment**
    This model will be deployed locally using Streamlit.


## **Machine Learning Models Employed**
- K-Nearest Neighbours (KNN)
- Support Vector Machine (SVM)
- Decision Tree
- Random Forest
- AdaBoost
- Deep Learning (Neural Network)


## **Model Analysis**
The best-performing model based on cross-validation results is Random Forest. However, despite hyperparameter tuning, this model still shows signs of overfitting, as seen from the significant difference between the results on the training and testing datasets, with the following MAE and R² Score values:

Dataset      | MAE          | R2 Score  
------------ | ------------ | ---------  
Train        | 234.36 mio   | 0.93  
Test         | 574.64 mio   | 0.72  

This significant difference indicates that the model performs better on the training data compared to the test data. This can be addressed with several approaches, such as:
- **Increasing Data**: Expanding the training data to help the model learn better patterns and reduce overfitting on limited data.
- **Feature Engineering**: Revisiting the features used in the model and considering adding new features or removing less relevant features to enhance the model's predictive ability.


## **Further Improvement**
- **Scraping data from various trusted sources** to ensure the completeness and accuracy of the property information used.
- **Optimizing the model for properties priced above 5.8 billion** to improve price predictions for the premium market segment.
- **Adding new features**, such as sub-districts, developer names, and proximity to key facilities, to enrich the analysis and enhance prediction accuracy.
- **Integrating algorithms other than Random Forest into the automation** to improve model performance and ensure better adaptation to different types of data.
- **Expanding the data coverage to include areas outside Jabodetabek**, to increase data diversity and reach a broader potential market across Indonesia.


## **How to Use**  
To get started, please ensure the following requirements are met:  

- **Docker Installation**  
   Ensure Docker is installed and running on your system. Docker is required to deploy and run both Airflow and PostgreSQL seamlessly.  

- **Zyte and OpenAI Accounts**  
   - Create an account on [Zyte](https://app.zyte.com/). Zyte provides a $5 free credit upon registration.  
   - Sign up for an account on [OpenAI](https://platform.openai.com/). Note that OpenAI requires a minimum credit top-up of $5 to access their services.
   - Copy the API Keys from both Zyte and OpenAI and save them in the `.env` file.

- **Create a Container**  
   Set up and configure a Docker container to run the necessary services for Airflow and PostgreSQL. Open the terminal, ensure you're in the correct directory, and run the following commands:  
   ```bash
   docker compose -f airflow_python3_9.yaml build  
   docker compose -f airflow_python3_9.yaml up -d  
   ```

- **Copy Files**  
   Copy all files from the `dags` folder into your local `dags` folder.

- **Verify Container**  
   Ensure that the Docker container is running properly.

- **Run SQL Command**  
   Execute the SQL command from `create_db.txt` to create the required database and tables.

- **Create Airflow Connection to PostgreSQL**  
   To establish a connection between Airflow and PostgreSQL:
   1. Open [http://localhost:8080/](http://localhost:8080/).
   2. Navigate to **Admin** -> **Connections** -> **Add a New Record** (the "+" symbol).
      - **Connection Id**: `postgres_airflow`.
      - **Connection Type**: `Postgres`.
      - **Host**: `postgres` (this is the name of the PostgreSQL container).
      - **Schema**: `house_prediction_db` (the name of the database in PostgreSQL).
      - **Login**: `airflow` (as specified in the `.env` file).
      - **Password**: `airflow` (as specified in the `.env` file).
   3. Click **Test** to ensure the connection is successful, then click **Save**.

Once these steps are completed, the system will be ready for use.

## **Team Members**  

| **Name**                  | **Role**       | **GitHub**                                |  **LinkedIn**                                                        |
|---------------------------|----------------|-------------------------------------------| ---------------------------------------------------------------------|
| **Abdul Aziz**            | Data Engineer  | [GitHub](https://github.com/ab-dul-aziz)  | [LinkedIn](https://www.linkedin.com/in/abdul-aziz-data-enthusiast/)  |
| **Muhammad Noerhadi**     | Data Engineer  | [GitHub](https://github.com/noerHadi17)   | [LinkedIn](https://www.linkedin.com/in/noerhadi/)                    |
| **Rafina Dhiya Pradani**  | Data Analyst   | [GitHub](https://github.com/rafinadhiya)  | [LinkedIn](https://www.linkedin.com/in/rafinadhiya/)                 |
| **Yasmin Naila**          | Data Scientist | [GitHub](https://github.com/yasmin-naila) | [LinkedIn](https://www.linkedin.com/in/yasmin-naila/)                |

## **About Me**
Abdul Aziz <br>
LinkedIn: [Abdul Aziz](https://www.linkedin.com/in/abdul-aziz-data-enthusiast/)


