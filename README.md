**YouTube Data Harvesting and Warehousing Using SQL, MongoDB, and Streamlit**

**Problem Statement**  

The goal is to develop a Streamlit application that allows users to analyze data from various YouTube channels. By entering a YouTube channel ID, users can access information such as channel details, video statistics, and user engagement metrics. The app should store this data in a MongoDB database and support collecting data from up to 10 different channels. Additionally, the app should enable users to migrate selected data from MongoDB to a SQL database for more in-depth analysis. Users should be able to search and retrieve data from the SQL database, including performing advanced queries like table joins to obtain comprehensive channel information.

**Technology Stack**  

- Python  
- MySQL  
- MongoDB  
- Google Client Library  

**Approach**  

1. **Streamlit Setup**: Begin by creating a user-friendly interface using the Streamlit library in Python. This interface will allow users to input a YouTube channel ID, view detailed channel information, and choose which channels to migrate.

2. **YouTube API Integration**: Establish a connection with the YouTube API V3 using the Google API client library for Python to retrieve channel and video data.

3. **Data Storage in MongoDB**: Store the retrieved data in a MongoDB database, which is well-suited for managing unstructured and semi-structured data. Implement a method to capture the API response and save the data in three distinct collections within the database.

4. **Data Migration to SQL**: Transfer the data from multiple channels—specifically channels, videos, and comments—from MongoDB to a SQL data warehouse, using a SQL database such as MySQL or PostgreSQL.

5. **SQL Querying and Data Retrieval**: Use SQL queries to join tables within the SQL data warehouse and retrieve specific channel information based on user input. Ensure that the SQL tables are appropriately designed with primary and foreign keys.

6. **Data Visualization in Streamlit**: Display the retrieved data in the Streamlit app, using its visualization features to generate charts and graphs, allowing users to analyze the data effectively.
