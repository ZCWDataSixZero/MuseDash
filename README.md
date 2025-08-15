

# 🎵 MuseDash — Music Streaming Analytics Dashboard  

## 📌 Overview  
MuseDash is an interactive music analytics dashboard built to analyze **historical streaming data** from Zip-pot-ify, a fictional but nationwide music platform. The project showcases our ability to design and deploy a **full data engineering and analytics pipeline** — from raw data ingestion to dynamic visualizations — highlighting **regional listening trends, artist popularity, genre breakdowns, and time-based metrics**.  

This project was developed collaboratively as a portfolio piece to demonstrate **modern data engineering and data visualization practices**.  

---

## 👩‍💻 The Team  
- **Angelika Brown** — [LinkedIn](https://www.linkedin.com/in/angelikabrown/) 
- **Isiah Armstrong**  — [LinkedIn](https://www.linkedin.com/in/isiaharmstrong00/) 
- **James Heller**  — [LinkedIn](https://www.linkedin.com/in/james-heller-xiii/) 
- **Kunle Adeyanju**  — [LinkedIn](https://www.linkedin.com/in/kunleadeyanju/) 

---

## Our Solution

![Screenshot](/App_Screenshot.png)

---

## 🚀 Tech Stack  

| Layer | Tools & Technologies |
|-------|----------------------|
| **Data Ingestion & Storage** | **AWS S3** (data storage)|
| **Data Processing** | **PySpark** (distributed processing), **Pandas** (data wrangling) |
| **Visualization** | **Altair**, **Plotly** |
| **Application Layer** | **Streamlit** (interactive dashboard) |
| **Version Control & Collaboration** | **GitHub** |

---

## 🔄 Data Pipeline Architecture  

Our pipeline processes millions of rows of listening data efficiently, using a combination of **cloud storage, distributed computing, and interactive front-end visualization**.  

**Workflow:**  
1. **Data Storage:** Raw Data files stored in AWS S3.  
2. **Data Processing & Enrichment:**   Data is loaded into **PySpark** where it is cleaned, filtered, and transformed at Scale
   - For visulaizion, the processed data is converted into **Pandas** DataFrames
   - We call AI APIs to supplement and enrich the data, such as generating music genre information for artis, which was not avaliable in the source dataset.
3. **Analytics:** Generated metrics such as:  
   - Most streamed artists/songs by region  
   - Genre popularity trends over time  
   - Listening activity heatmaps  
4. **Visualization:** Interactive charts and maps using **Altair** & **Plotly**.  
5. **Dashboard Deployment:** **Streamlit** app providing filtering, search, and drill-down capabilities.  

📌 **Pipeline Diagram:**  
![Pipeline](/MuseDash_Pipeline.png)  

---

## 📊 Dashboard Features  
- **Choropleth Maps** — visualize listening habits across U.S. states.  
- **Artist & Genre Filters** — deep dive into specific music categories.  
- **Time-based Trends** — track popularity shifts over time.  
- **Responsive Design** — fast filtering with **Streamlit caching** for smooth UI.  

---



# MuseDash

### The Team
## Connect with us
Angelika Brown: https://www.linkedin.com/in/angelikabrown/

Isiah Armstrong: https://www.linkedin.com/in/isiaharmstrong00/

James Heller: https://www.linkedin.com/in/james-heller-xiii/

Kunle Adeyanju: https://www.linkedin.com/in/kunleadeyanju/


### The Task:

Zip-pot-ify is an amazing new music streaming company with listeners all over the country.

We, the mgmt team, want you, the data engineering team, to create a cool new dashboard that takes in historical listening data and show us cool things. What cool things?

Regional differences, popularities, and other metrics; show it to us by? artist? song? genre? time? what else?




### Our Pipeline

![image](https://github.com/user-attachments/assets/16e80648-9c17-4af4-8bd2-51e5f6a9bae3)



* AWS S3
* PySpark
* Pandas
* Altair
* Plotly
* Streamlit

