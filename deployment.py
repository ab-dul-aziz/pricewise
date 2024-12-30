import streamlit as st
import pandas as pd
import pickle

# Application Configuration
st.set_page_config(page_title="Real Estate Price Prediction App", page_icon="🏡", layout="centered")

# Application Header
st.title("🏡 Real Estate Price Prediction App")
st.write("Enter property details below to predict the price.")

# Input Section
st.subheader("📋 Property Details")
col1, col2 = st.columns(2)
with col1:
    land_size = st.number_input("Land Size (m²)", min_value=0.0, help="Enter the land size in square meters.")
    building_size = st.number_input("Building Size (m²)", min_value=0.0, help="Enter the building size in square meters.")
    road_width = st.number_input("Road Width (m)", min_value=0.0, help="Enter the road width in meters.")
    bedroom = st.number_input("Number of Bedrooms", min_value=0, step=1)
    bathroom = st.number_input("Number of Bathrooms", min_value=0, step=1)
    carport = st.number_input("Number of Carports", min_value=0, step=1)

with col2:
    kitchen = st.number_input("Number of Kitchens", min_value=0, step=1)
    city = st.selectbox("City", ['Jakarta', 'Bogor', 'Depok', 'Tangerang', 'Bekasi'])
    property_type = st.selectbox(
        "Property Type",
        ['Rumah Tipe 21', 'Rumah Tipe 36', 'Rumah Tipe 45', 'Rumah Tipe 54', 
         'Rumah Tipe 60', 'Rumah Tipe 70', 'Rumah Tipe 120', 'Rumah Tipe >120']
    )
    certificate = st.selectbox("Certificate", ['SHM', 'HGB', 'Other'])
    water_source = st.selectbox("Water Source", ['PAM/PDAM', 'Sumber Air'])

# Additional Features Section
st.subheader("⚙️ Additional Features")
col3, col4 = st.columns(2)
with col3:
    furniture = st.selectbox("Furniture", ['Unfurnished', 'Semi Furnished', 'Fully Furnished'], index=0)
    house_facing = st.selectbox("House Facing Direction", ['North', 'South', 'East', 'West'], index=0)
    maid_bedroom = st.number_input("Number of Maid Bedrooms", min_value=0, step=1)
    maid_bathroom = st.number_input("Number of Maid Bathrooms", min_value=0, step=1)

with col4:
    property_condition = st.selectbox(
        "Property Condition", 
        ['Well Maintained', 'Renovated', 'New', 'Needs Renovation'], index=0
    )
    floor_level = st.number_input("Floor Level", min_value=1, step=1)
    garage = st.number_input("Number of Garages", min_value=0, step=1)
    voltage_watt = st.number_input("Voltage Watt", min_value=0, step=100, value=2200)

# Prediction Button and Output
st.markdown("---")
if st.button("Predict"):
    # Collect input data
    input_data = {
        "land_size_m2": land_size,
        "building_size_m2": building_size,
        "road_width": road_width,
        "bedroom": bedroom,
        "bathroom": bathroom,
        "carport": carport,
        "kitchen": kitchen,
        "city": city,
        "property_type": property_type,
        "certificate": certificate,
        "water_source": water_source,
        "furniture": furniture,
        "house_facing": house_facing,
        "maid_bedroom": maid_bedroom,
        "property_condition": property_condition,
        "floor_level": floor_level,
        "garage": garage,
        "maid_bathroom": maid_bathroom,
        "voltage_watt": voltage_watt
    }

    # Load the trained model
    try:
        model_path = "data/best_model_ever.pkl"  # Update the path to your model
        with open(model_path, 'rb') as file:
            model = pickle.load(file)

        # Prepare data for prediction
        input_df = pd.DataFrame([input_data])

        # Make prediction
        prediction = model.predict(input_df)
        st.subheader("Prediction Result:")
        st.success(f"Predicted Price: **Rp {prediction[0]:,.2f} Million**")
    except FileNotFoundError:
        st.error("Model file not found. Please ensure the model file exists.")
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
